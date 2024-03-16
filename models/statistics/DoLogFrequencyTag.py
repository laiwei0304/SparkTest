import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, when
from TagTools import rule_to_tuple_udf


class DoLogFrequencyTag(object):

    @staticmethod
    def start():
        findspark.init()

    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoLogFrequencyTag'). \
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
        .where(col("id") == 254) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)
    # selectTable=tbl_logs;selectField=global_user_id|log_time

    attr = df.filter("level==5") \
        .where(col("pid") == 254) \
        .select(col("name"), rule_to_tuple_udf(col("rule")).alias("rules")) \
        .select(
        col("name"),  # 选择name列
        col("rules.start").alias("start"),  # 从rules结构体中选择start字段
        col("rules.end").alias("end")  # 从rules结构体中选择end字段
    )

    # 取五级标签


    df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
    biz = df2.select(selectField)

    # 过滤掉logtime在一个月之前的数据！！！

    # 选出 id/nowtime/logtime
    updated_biz = biz.select(
        col("global_user_id"),
        current_date().alias("now_time"),
        col("log_time")
    )

    # 选出logtime距离现在的时间
    updated_biz = updated_biz.select(
        col("global_user_id").alias("userId"),
        datediff(col("now_time"), col("log_time")).alias("logCycle")
    )

    # 过滤掉logCycle大于30的
    updated_biz = updated_biz.withColumn(
        "logCycle",
        when(col("logCycle") < 30, 1).otherwise(0)
    )
    # updated_biz.show(50)

    # 按照用户ID分组，获取一个月内的log数量(
    countLogDF = updated_biz.groupBy("userId") \
        .agg({'logCycle': 'sum'}).withColumnRenamed('sum(logCycle)', 'frequency')
    # countLogDF.show()



    rst = (countLogDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("frequency").between(col("start"), col("end"))  # 筛选出countLogTime在start和end之间的行
    )
    .select(
        col("userId"),
        col("name").alias("value"),
        col("frequency")
    )
        # .orderBy("userId")
    )

    rst.write.format("jdbc").mode("overwrite") \
    .option("truncate", "true") \
    .option("url", url) \
    .option("dbtable", 'tbl_logFrequency_tag') \
    .option("user", 'root') \
    .option("password", 'admin') \
    .save()
    print("访问频率标签计算完成！")
