# -*- coding: utf-8 -*-
import findspark
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, count, sum, datediff, from_unixtime, current_timestamp, when, udf
from pyspark.sql.types import DecimalType, IntegerType, DoubleType, StringType
from models.mining.MLModelTools import MLModelTools


class DoUsgTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoUsgTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "200"). \
            config("spark.sql.broadcastTimeout", "36000"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'

        df = spark.read.jdbc(url=url, table='tbl_goods', properties=prop)

        # 获取订单表数据tbl_orders，与订单商品表数据关联获取会员ID
        ordersDf = spark.read.jdbc(url=url, table='tbl_orders_new', properties=prop) \
            .select(col("memberId"), col("orderSn").alias("cOrderSn"))

        # 构建颜色WHEN语句
        colorColumn = when(col("ogColor") == "香槟色", 1) \
            .when(col("ogColor") == "樱花粉", 2) \
            .when(col("ogColor") == "月光银", 3) \
            .when(col("ogColor") == "灰色", 4) \
            .when(col("ogColor") == "金属灰", 5) \
            .when(col("ogColor") == "金色", 6) \
            .when(col("ogColor") == "布朗灰", 7) \
            .when(col("ogColor") == "蓝色", 8) \
            .when(col("ogColor") == "金属银", 9) \
            .when(col("ogColor") == "香槟金", 10) \
            .when(col("ogColor") == "乐享金", 11) \
            .when(col("ogColor") == "粉色", 12) \
            .when(col("ogColor") == "玫瑰金", 13) \
            .when(col("ogColor") == "浅金棕", 14) \
            .when(col("ogColor") == "银色", 15) \
            .when(col("ogColor") == "布鲁钢", 16) \
            .when(col("ogColor") == "卡其金", 17) \
            .when(col("ogColor") == "白色", 18) \
            .when(col("ogColor") == "黑色", 19) \
            .otherwise(0).alias("color")

        # 构建商品类别WHEN语句
        productColumn = when(col("productType") == "Haier/海尔冰箱", 1) \
            .when(col("productType") == "波轮洗衣机", 2) \
            .when(col("productType") == "燃气灶", 3) \
            .when(col("productType") == "净水机", 4) \
            .when(col("productType") == "取暖电器", 5) \
            .when(col("productType") == "智能电视", 6) \
            .when(col("productType") == "烤箱", 7) \
            .when(col("productType") == "挂烫机", 8) \
            .when(col("productType") == "嵌入式厨电", 9) \
            .when(col("productType") == "吸尘器/除螨仪", 10) \
            .when(col("productType") == "烟灶套系", 11) \
            .when(col("productType") == "微波炉", 12) \
            .when(col("productType") == "LED电视", 13) \
            .when(col("productType") == "电水壶/热水瓶", 14) \
            .when(col("productType") == "电饭煲", 15) \
            .when(col("productType") == "冷柜", 16) \
            .when(col("productType") == "Leader/统帅冰箱", 17) \
            .when(col("productType") == "前置过滤器", 18) \
            .when(col("productType") == "冰吧", 19) \
            .when(col("productType") == "电风扇", 20) \
            .when(col("productType") == "4K电视", 21) \
            .when(col("productType") == "电热水器", 22) \
            .when(col("productType") == "破壁机", 23) \
            .when(col("productType") == "燃气热水器", 24) \
            .when(col("productType") == "料理机", 25) \
            .when(col("productType") == "滤芯", 26) \
            .when(col("productType") == "电磁炉", 27) \
            .when(col("productType") == "空气净化器", 28) \
            .when(col("productType") == "其他", 29) \
            .otherwise(0).alias("product")

        # 根据运营规则标注的部分数据
        labelColumn = when((col("ogColor") == "樱花粉") |
                           (col("ogColor") == "粉色") |
                           (col("ogColor") == "白色") |
                           (col("ogColor") == "玫瑰金") |
                           (col("ogColor") == "香槟色") |
                           (col("ogColor") == "香槟金") |
                           (col("productType") == "料理机") |
                           (col("productType") == "烤箱") |
                           (col("productType") == "电饭煲") |
                           (col("productType") == "挂烫机") |
                           (col("productType") == "破壁机") |
                           (col("productType") == "微波炉") |
                           (col("productType") == "波轮洗衣机") |
                           (col("productType") == "取暖电器") |
                           (col("productType") == "吸尘器/除螨仪"), 1) \
            .otherwise(0) \
            .alias("label")

        # 关联订单数据，颜色维度和商品类别维度数据
        goodsDf = df.join(ordersDf, on="cOrderSn") \
            .select(col("memberId").alias("userId"),
                    colorColumn,
                    productColumn,
                    labelColumn)
        # goodsDf.printSchema()
        # goodsDf.show(100, truncate=False)
        # goodsDf.groupBy("label").count().show()

        # 当模型存在时，直接加载模型；如果不存在，训练获取最佳模型，并保存
        pipelineModel = MLModelTools.loadModel(goodsDf, "usg")
        predictionDf = pipelineModel.transform(goodsDf)
        # predictionDf.select("userId", "color", "product", "prediction").show()

        # 按照用户ID分组，统计每个用户购物男性或女性商品个数及占比
        genderDf = predictionDf.groupBy("userId") \
            .agg(count("userId").alias("total"),
                 sum(when(col("prediction") == 0, 1).otherwise(0)).alias("maleTotal"),
                 sum(when(col("prediction") == 1, 1).otherwise(0)).alias("femaleTotal"))
        # genderDf.printSchema()
        # goodsDf.show(100, truncate=False)

        # 自定义UDF函数，计算占比，确定标签值 对每个用户，分别计算男性商品和女性商品占比，当占比大于等于0.5时，确定购物性别
        def gender_tag(total, maleTotal, femaleTotal):
            maleRate = maleTotal / total
            femaleRate = femaleTotal / total
            if maleRate >= 0.5:
                return "0"
            elif femaleRate >= 0.5:
                return "1"
            else:
                return "-1"

        gender_tag_udf = udf(gender_tag, StringType())

        # 获取标签数据
        modelDf = genderDf.select("userId", gender_tag_udf("total", "maleTotal", "femaleTotal").alias("usg"))
        # modelDf.printSchema()
        # modelDf.show(100, truncate=False)
        # modelDf.groupBy("usg").count().show()

        # 读取基础标签表tbl_basic_tags
        df2 = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df2.filter("level==5") \
            .where(col("pid") == 318) \
            .select("name", "rule")
        # attr.show()

        # 打标签
        rst = modelDf.join(attr, col("usg") == col("rule")) \
            .drop("usg", "rule") \
            .withColumnRenamed("name", "usg") \
            .orderBy("userId")
        # rst.show()

        # 存储打好标签的数据
        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_usg_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("用户购物性别标签计算完成！")


# if __name__ == '__main__':
#     DoUsgTag.start()
