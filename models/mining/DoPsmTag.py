# -*- coding: utf-8 -*-
import findspark
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, count, sum, datediff, from_unixtime, current_timestamp, when, udf
from pyspark.sql.types import DecimalType, IntegerType
from MLModelTools import MLModelTools


class DoPsmTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoPsmTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "200"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'

        # 计算PSM值
        #   psm = 优惠订单占比tdonr + 平均优惠金额占比adar + 优惠总金额占比tdar
        # 	tdonr 优惠订单占比(优惠订单数 / 订单总数)
        # 	adar  平均优惠金额占比(平均优惠金额 / 平均每单应收金额)
        # 	平均优惠金额 = 优惠总金额 / 优惠订单数
        # 	如果某个人不差钱，购物从来不使用优惠，此时优惠订单数为0，分母为0，计算出平均优惠金额为null
        # 	tdar  优惠总金额占比(优惠总金额 / 订单总金额)
        df = spark.read.jdbc(url=url, table='tbl_orders_new', properties=prop)
        # 计算指标
        # ra: receivableAmount 应收金额
        raColumn = (col("orderAmount") + col("couponCodeValue")).alias("ra")
        # da: discountAmount 优惠金额
        daColumn = col("couponCodeValue").cast(DecimalType(10, 2)).alias("da")
        # pa: practicalAmount 实收金额
        paColumn = col("orderAmount").cast(DecimalType(10, 2)).alias("pa")
        # state: 订单状态，此订单是否是优惠订单，0表示非优惠订单，1表示优惠订单
        stateColumn = when(col("couponCodeValue") == 0.0, 0) \
            .otherwise(1).alias("state")
        # tdon 优惠订单数
        tdonColumn = sum("state").alias("tdon")
        # ton 总订单总数
        tonColumn = count("state").alias("ton")
        # tda 优惠总金额
        tdaColumn = sum("da").alias("tda")
        # tra 应收总金额
        traColumn = sum("ra").alias("tra")
        # tdonr 优惠订单占比(优惠订单数 / 订单总数)
        # tdar 优惠总金额占比(优惠总金额 / 订单总金额)
        # adar 平均优惠金额占比(平均优惠金额 / 平均每单应收金额)
        tdonrColumn = (col("tdon") / col("ton")).alias("tdonr")
        tdarColumn = (col("tda") / col("tra")).alias("tdar")
        adarColumn = ((col("tda") / col("tdon")) / (col("tra") / col("ton"))).alias("adar")
        psmColumn = (col("tdonr") + col("tdar") + col("adar")).alias("psm")
        psmDf = df.select(col("memberId").alias("userId"),
                          raColumn, daColumn, paColumn, stateColumn) \
            .groupBy("userId") \
            .agg(tonColumn, tdonColumn, traColumn, tdaColumn) \
            .select("userId", tdonrColumn, tdarColumn, adarColumn) \
            .select("*", psmColumn) \
            .select("*", when(col("psm").isNull(), 0.00000001).otherwise(col("psm")).alias("psm_score"))
        # psmDf.printSchema()
        # psmDf.show()

        # 使用RFM_SCORE进行Kmeans单列聚类（K=5）
        assembler = VectorAssembler() \
            .setInputCols(["psm_score"]) \
            .setOutputCol("raw_features")
        rawFeaturesDf = assembler.transform(psmDf)
        # 对特征数据进行处理：最大最小归一化
        scalerModel = MinMaxScaler() \
            .setInputCol("raw_features") \
            .setOutputCol("features") \
            .fit(rawFeaturesDf)
        featuresDf = scalerModel.transform(rawFeaturesDf)

        # 加载模型
        kMeansModel = MLModelTools.loadModel(featuresDf, "psm")

        # 使用模型预测
        predictionDf = kMeansModel.transform(featuresDf)
        # predictionDf.select("userId", "psm_score", "prediction").show()

        # 获取聚类中心，并根据psm大小修改索引
        centers = kMeansModel.clusterCenters()
        clusterDf = MLModelTools.convertKMeansIndexMap(centers, predictionDf, "psm")

        # 读取基础标签表tbl_basic_tags
        df2 = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df2.filter("level==5") \
            .where(col("pid") == 312) \
            .select("name", "rule")
        # attr.show()

        # 打标签
        rst = clusterDf.join(attr, col("prediction") == col("rule")) \
            .drop("prediction", "rule") \
            .withColumnRenamed("name", "psm") \
            .orderBy("userId")
        # rst.show()

        # 存储打好标签的数据
        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_psm_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("价格敏感度标签计算完成！")


if __name__ == '__main__':
    DoPsmTag.start()
