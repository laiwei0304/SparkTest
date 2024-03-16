import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from TagTools import rule_to_tuple_udf


class DoMaxOrderTag(object):

    @staticmethod
    def start():
        findspark.init()

    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoMaxOrderTag'). \
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
        .where(col("id") == 230) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    # 取五级标签
    attr = df.filter("level==5") \
        .where(col("pid") == 230) \
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

    # 按照用户ID分组，获取最大订单金额
    maxOrderDF = biz.groupBy("memberId")\
        .agg({'orderAmount': 'max'}).withColumnRenamed('max(orderAmount)', 'max_orderAmount')
    # maxOrderDF.show()

    rst = (maxOrderDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("max_orderAmount").between(col("start"), col("end"))  # 筛选出unitPrice在start和end之间的行
    )
    .select(
        col("memberId").alias("userId"),  # 重命名id列为userId
        col("max_orderAmount"),
        col("name").alias("maxOrderRange")
    )
        # .orderBy("userId")
    )
    # rst.show()

    rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_maxOrder_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
    print("单笔最高标签计算完成！")


