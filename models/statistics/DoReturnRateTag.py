# import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from TagTools import rule_to_tuple_udf

'''
class DoReturnRateTag(object):

    @staticmethod
    def start():
        findspark.init()
'''
if __name__ == '__main__':
    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoReturnRateTag'). \
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
        .where(col("id") == 240) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    # 取五级标签
    attr = df.filter("level==5") \
        .where(col("pid") == 240) \
        .select(col("name"), rule_to_tuple_udf(col("rule")).alias("rules")) \
        .select(
        col("name"),  # 选择name列
        col("rules.start").alias("start"),  # 从rules结构体中选择start字段
        col("rules.end").alias("end")  # 从rules结构体中选择end字段
    )


    df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
    biz = df2.select(selectField)

    # 将退货状态的改为1，其他变成0
    updated_biz = biz.withColumn(
        "orderStatus",
        when(col("orderStatus") == 0, 1).otherwise(0)
    )

    # 可以过滤掉6个月前的数据，但本次数据均在一个月内，不过滤，直接计算退货数量/6个月
    # 按照用户ID分组，获取订单数量
    countReturnDF = updated_biz.groupBy("memberId") \
        .agg({'orderStatus': 'sum'}) \
        .withColumnRenamed('sum(orderStatus)', 'returnRate') \
        .withColumnRenamed('memberId', 'userId')

    # countReturnDF.show()

    # 计算周期是近半年,直接给总数，结果是半年退货多少
    '''
    rateDF = countRateDF.select(
        col("memberId"),
        # 计算
        round(col("countReturn") / 6, 0).alias("rate")
    )
    rateDF.show()
    '''

    rst = (countReturnDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("returnRate").between(col("start"), col("end"))  # 筛选出countReturn在start和end之间的行
    )
    .select(
        col("userId"),
        col("name").alias("value"),
        col("returnRate")
    )
        # .orderBy("userId")
    )

    rst.write.format("jdbc").mode("overwrite") \
        .option("truncate", "true") \
        .option("url", url) \
        .option("dbtable", 'tbl_returnRate_tag') \
        .option("user", 'root') \
        .option("password", 'admin') \
        .save()
