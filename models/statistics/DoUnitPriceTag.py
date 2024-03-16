import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from TagTools import rule_to_tuple_udf


class DoUnitPriceTag(object):

    @staticmethod
    def start():
        findspark.init()

    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoUnitPriceTag'). \
        master('local'). \
        config("spark.debug.maxToStringFields", "100"). \
        getOrCreate()
    # mysql 配置
    prop = {'user': 'root',
            'password': 'admin',
            'driver': 'com.mysql.jdbc.Driver'}
    # database 地址
    url = ('jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'
           '&rewriteBatchedStatements=true')
    # 读取表
    df = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
    # 取四级标签的rule
    rule = df.filter("level==4") \
        .where(col("id") == 217) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    # 取五级标签
    attr = df.filter("level==5") \
        .where(col("pid") == 217) \
        .select(col("name"), rule_to_tuple_udf(col("rule")).alias("rules")) \
        .select(
        col("name"),  # 选择name列
        col("rules.start").alias("start"),  # 从rules结构体中选择start字段
        col("rules.end").alias("end")  # 从rules结构体中选择end字段
    )
    # attr.show()

    # 读取order_new表
    df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
    biz = df2.select(selectField)

    # 按照用户ID分组，获取所有订单总金额
    # 这里的订单号没有去重 (orderSn),故计算的是每一笔交易的平均成交额
    orderAmountDF = biz.groupBy("memberId") \
        .agg({'orderAmount': 'sum', 'memberId': 'count'}) \
        .withColumnRenamed('sum(orderAmount)', 'total_orderAmount') \
        .withColumnRenamed('count(memberId)', 'total_order')
    # orderAmountDF.show()

    # 计算客单价
    unitPriceDF = orderAmountDF.select(
        col("memberId"),
        # 计算天数
        round(col("total_orderAmount") / col("total_order"), 2).alias("unitPrice")
    )
    # unitPriceDF.show(30)

    rst = (unitPriceDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("unitPrice").between(col("start"), col("end"))  # 筛选出unitPrice在start和end之间的行
    )
    .select(
        col("memberId").alias("userId"),  # 重命名id列为userId
        col("name").alias("unitPriceRange"),
        col("unitPrice")
    )
        # .orderBy("userId")
    )
    # rst.show()

    rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_unitPrice_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
    print("客单价标签计算完成！")



'''
+--------+-----------------+-----------+
|memberId|total_orderAmount|total_order|
+--------+-----------------+-----------+
|      29|        408084.29|        248|
|      26|        397622.20|        217|
|     474|        234717.54|        109|
|      65|        435320.18|        228|
|     418|        174938.16|        106|
|     541|        151929.87|        110|
|     558|        209222.96|        109|
|     191|        211314.91|        118|
|     938|        168691.73|        120|
|     270|        153553.00|        101|
|     293|        322274.82|        132|
|     730|        231711.56|        122|
|     222|        308160.86|         97|
|     243|        206543.73|        125|
|     720|        198607.73|        109|
|     278|        248427.26|        111|
|     367|        226642.10|        121|
|     705|        155385.93|        102|
|     442|        212651.07|        123|
|      54|        433587.66|        251|
+--------+-----------------+-----------+
'''
