# -*- coding: utf-8 -*-
import findspark
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, count, sum, datediff, from_unixtime, current_timestamp, when, udf
from pyspark.sql.types import DecimalType, IntegerType
from MLModelTools import MLModelTools


class DoRfmTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoRfmTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "200"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'

        # 计算RFM值
        #     R：消费周期 finishTime
        #       日期时间函数：current_timestamp from_unixtimestamp datediff
        #     F: 消费次数 orderSn
        #       count
        #     M：消费金额 orderAmount
        #       sum
        df = spark.read.jdbc(url=url, table='tbl_orders_new', properties=prop)
        rfmDf = df.groupby("memberId") \
            .agg(max("finishTime").alias("max_finishTime"),
                 count("orderSn").alias("frequency"),
                 sum(col("orderAmount").cast(DecimalType(10, 2))).alias("monetary")) \
            .select(col("memberId").alias("user_id"),
                    datediff(current_timestamp(), from_unixtime("max_finishTime")).alias("recency"),
                    "frequency",
                    "monetary")
        # rfmDf.printSchema()
        # rfmDf.show(10, truncate=False)

        # 按照RFM值进行打分（RFM_SCORE)
        #     R: 1 - 3天 = 5分，4 - 6天 = 4分，7 - 9天 = 3分，10 - 15天 = 2分，大于16天 = 1分
        #     F: ≥200 = 5分，150 - 199 = 4分，100 - 149 = 3分，50 - 99 = 2分，1 - 49 = 1分
        #     M: ≥20w = 5分，10 - 19w = 4分，5 - 9w = 3分，1 - 4w = 2分， < 1w = 1分
        #     使用CASE WHEN..WHEN...ELSE....END
        rWhen = when(col("recency").between(1, 3), 5.0) \
            .when(col("recency").between(4, 6), 4.0) \
            .when(col("recency").between(7, 9), 3.0) \
            .when(col("recency").between(10, 15), 2.0) \
            .when(col("recency") >= 16, 1.0)
        fWhen = when(col("frequency").between(1, 49), 1.0) \
            .when(col("frequency").between(50, 99), 2.0) \
            .when(col("frequency").between(100, 149), 3.0) \
            .when(col("frequency").between(150, 199), 4.0) \
            .when(col("frequency") >= 200, 5.0)
        mWhen = when(col("monetary") < 10000, 1.0) \
            .when(col("monetary").between(10000, 49999), 2.0) \
            .when(col("monetary").between(50000, 99999), 3.0) \
            .when(col("monetary").between(100000, 199999), 4.0) \
            .when(col("monetary") >= 200000, 5.0)
        rfmScoreDf = rfmDf.select("user_id",
                                  rWhen.alias("r_score"),
                                  fWhen.alias("f_score"),
                                  mWhen.alias("m_score"))
        # rfmScoreDf.printSchema()
        # rfmScoreDf.show(10, truncate=False)

        # 使用RFM_SCORE进行Kmeans聚类（K=5）
        # 组合R\F\M列为特征值features
        assembler = VectorAssembler() \
            .setInputCols(["r_score", "f_score", "m_score"]) \
            .setOutputCol("raw_features")
        rawFeaturesDf = assembler.transform(rfmScoreDf)
        # 将训练数据缓存
        # rawFeaturesDf.persist(StorageLevel.MEMORY_AND_DISK)
        # 对特征数据进行处理：最大最小归一化
        scalerModel = MinMaxScaler() \
            .setInputCol("raw_features") \
            .setOutputCol("features") \
            .fit(rawFeaturesDf)
        featuresDf = scalerModel.transform(rawFeaturesDf)

        # 加载模型
        # kMeansModel = DoRfmTag.loadModel(featuresDf)
        kMeansModel = MLModelTools.loadModel(featuresDf, "rfm")

        # 使用模型预测
        predictionDf = kMeansModel.transform(featuresDf)
        # predictionDf.show()

        # 通过计算轮廓系数评估聚类
        # evaluator = ClusteringEvaluator()
        # silhouette = evaluator.evaluate(predictionDf)
        # print("欧氏距离平方的轮廓系数 = " + str(silhouette))

        # 获取聚类中心，并根据rfm大小修改索引
        centers = kMeansModel.clusterCenters()
        clusterDf = MLModelTools.convertKMeansIndexMap(centers, predictionDf, "rfm")

        # 读取基础标签表tbl_basic_tags
        df2 = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df2.filter("level==5") \
            .where(col("pid") == 301) \
            .select("name", "rule")
        # attr.show()

        # 打标签
        rst = clusterDf.join(attr, col("prediction") == col("rule")) \
            .drop("prediction", "rule") \
            .withColumnRenamed("name", "rfm") \
            .orderBy("user_id")
        # rst.show()

        # 存储打好标签的数据
        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_rfm_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("用户价值标签计算完成！")


if __name__ == '__main__':
    DoRfmTag.start()
