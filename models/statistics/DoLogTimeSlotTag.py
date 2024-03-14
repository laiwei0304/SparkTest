import findspark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, hour, row_number
from TagTools import rule_to_tuple_udf

'''
class DoLogTimeSlotTag(object):

    @staticmethod
    def start():
        findspark.init()
'''
if __name__ == '__main__':

    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoLogTimeSlotTag'). \
        master('local'). \
        config("spark.debug.maxToStringFields", "100"). \
        getOrCreate()
    # mysql 配置
    prop = {'user': 'root',
            'password': 'admin',
            'driver': 'com.mysql.jdbc.Driver'}
    # database 地址
    url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'
    # 读取表
    df = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
    # 取四级标签的rule
    rule = df.filter("level==4") \
        .where(col("id") == 259) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    # 提取start和end
    # 取五级标签
    attr = df.filter("level==5") \
        .where(col("pid") == 259) \
        .select(col("name"), rule_to_tuple_udf(col("rule")).alias("rules")) \
        .select(
        col("name"),  # 选择name列
        col("rules.start").alias("start"),  # 从rules结构体中选择start字段
        col("rules.end").alias("end")  # 从rules结构体中选择end字段
    )
    # attr.show()
    # 读取user表
    df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
    biz = df2.select(selectField)

    # 获取log_time的时间
    updated_biz = biz.withColumn("hour", hour("log_time"))

    # a. 按照先用户ID分组，再按支付编码paymentCode分组，使用count函数，统计次数
    timeSlotDF = (updated_biz.groupBy("global_user_id", "hour")
                 .count()  # 使用count函数列名称就是：count
                 # b. 使用窗口分析函数，获取最多次数
                 .withColumn(
        "rnk",
        row_number().over(
            Window.partitionBy("global_user_id").orderBy(col("count").desc())
        )
    )
                 .where(col("rnk") == 1)
                 # c. 选取字段
                 .select(col("global_user_id"), col("hour").alias("logHour"))
                 )
    # paymentDF.show()

    rst = (timeSlotDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("logHour").between(col("start"), col("end"))  # 筛选出bornDate在start和end之间的行
    )
    .select(
        col("global_user_id").alias("userId"),  # 重命名id列为userId
        col("name").alias("timeSlot")
    )
    )
    # rst.show(50)


    rst.write.format("jdbc").mode("overwrite") \
        .option("truncate", "true") \
        .option("url", url) \
        .option("dbtable", 'tbl_logTimeSlot_tag') \
        .option("user", 'root') \
        .option("password", 'admin') \
        .save()


