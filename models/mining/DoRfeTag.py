# -*- coding: utf-8 -*-
import findspark
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, count, sum, datediff, from_unixtime, current_timestamp, when, udf, \
    countDistinct, date_sub
from pyspark.sql.types import DecimalType, IntegerType, LongType
from models.mining.MLModelTools import MLModelTools


class DoRfeTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoRfeTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "200"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'

        # 计算RFE值
        # R：最近一次访问时间，距离今天的天数 max->datediff
        # F：所有访问浏览量 count
        # E：所有访问页面量（不包含重复） count distinct
        df = spark.read.jdbc(url=url, table='tbl_logs', properties=prop)
        rfeDf = df.groupby("global_user_id") \
            .agg(max("log_time").alias("last_time"),
                 count("loc_url").alias("frequency"),
                 countDistinct("loc_url").alias("engagements")) \
            .select(col("global_user_id").cast(LongType()).alias("userId"),
                    datediff(date_sub(current_timestamp(), 1640), col("last_time")).alias("recency"),
                    "frequency",
                    "engagements")
        # rfeDf.printSchema()
        # rfeDf.show(truncate=False)

        # 按照RFE值进行打分（RFM_SCORE)
        #   R:0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
        #   F:≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
        #   E:≥250=5分，200-249=4分，150-199=3分，50-149=2分，≤49=1分
        #   使用CASE WHEN..WHEN...ELSE....END
        rWhen = when(col("recency").between(1, 15), 5.0) \
            .when(col("recency").between(16, 30), 4.0) \
            .when(col("recency").between(31, 45), 3.0) \
            .when(col("recency").between(46, 60), 2.0) \
            .when(col("recency") >= 61, 1.0)
        fWhen = when(col("frequency") <= 99, 1.0) \
            .when(col("frequency").between(100, 199), 2.0) \
            .when(col("frequency").between(200, 299), 3.0) \
            .when(col("frequency").between(300, 399), 4.0) \
            .when(col("frequency") >= 400, 5.0)
        eWhen = when(col("engagements") <= 49, 1.0) \
            .when(col("engagements").between(50, 149), 2.0) \
            .when(col("engagements").between(150, 199), 3.0) \
            .when(col("engagements").between(200, 249), 4.0) \
            .when(col("engagements") >= 250, 5.0)
        rfeScoreDf = rfeDf.select("userId",
                                  rWhen.alias("r_score"),
                                  fWhen.alias("f_score"),
                                  eWhen.alias("e_score"))
        # rfeScoreDf.printSchema()
        # rfeScoreDf.show(truncate=False)

        # 使用RFE_SCORE进行Kmeans聚类（K=4）
        # 组合R\F\E列为特征值features
        assembler = VectorAssembler() \
            .setInputCols(["r_score", "f_score", "e_score"]) \
            .setOutputCol("raw_features")
        rawFeaturesDf = assembler.transform(rfeScoreDf)
        # 将训练数据缓存
        # rawFeaturesDf.persist(StorageLevel.MEMORY_AND_DISK)
        # 对特征数据进行处理：最大最小归一化
        scalerModel = MinMaxScaler() \
            .setInputCol("raw_features") \
            .setOutputCol("features") \
            .fit(rawFeaturesDf)
        featuresDf = scalerModel.transform(rawFeaturesDf)

        # 加载模型
        kMeansModel = MLModelTools.loadModel(featuresDf, "rfe")

        # 使用模型预测
        predictionDf = kMeansModel.transform(featuresDf)
        # predictionDf.select("userId", "r_score", "f_score", "e_score", "prediction").show()

        # 通过计算轮廓系数评估聚类
        # evaluator = ClusteringEvaluator()
        # silhouette = evaluator.evaluate(predictionDf)
        # print("欧氏距离平方的轮廓系数 = " + str(silhouette))

        # 获取聚类中心，并根据rfe大小修改索引
        centers = kMeansModel.clusterCenters()
        clusterDf = MLModelTools.convertKMeansIndexMap(centers, predictionDf, "rfe")

        # 读取基础标签表tbl_basic_tags
        df2 = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df2.filter("level==5") \
            .where(col("pid") == 307) \
            .select("name", "rule")
        # attr.show()

        # 打标签
        rst = clusterDf.join(attr, col("prediction") == col("rule")) \
            .drop("prediction", "rule") \
            .withColumnRenamed("name", "rfe") \
            .orderBy("userId")
        rst.show()

        # 存储打好标签的数据
        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_rfe_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("用户活跃度标签计算完成！")


# if __name__ == '__main__':
#     DoRfeTag.start()
