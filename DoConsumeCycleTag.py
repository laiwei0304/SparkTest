from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, current_timestamp, datediff
from TagTools import rule_to_tuple_udf

if __name__ == '__main__':
    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoConsumeCycleTag'). \
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
        .where(col("id") == 208) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    # 取五级标签
    attr = df.filter("level==5") \
        .where(col("pid") == 208) \
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

    # 按照用户ID分组，获取最大订单完成时间
    consumerDaysDF = biz.groupBy("memberId") \
        .agg({'finishTime': 'max'}).withColumnRenamed('max(finishTime)', 'max_finishTime')
    # .agg(max("finishTime").alias("max_finishTime"))
    # consumerDaysDF.show()

    # 日期时间转换，和获取当前日期时间
    consumerDaysDF = consumerDaysDF.select(
        col("memberId"),
        # 将Long类型转换日期时间类型
        from_unixtime(col("max_finishTime")).alias("finish_time"),
        # from_unixtime(col("finishTime")).alias("finish_time"),
        # 获取当前日期时间
        current_timestamp().alias("now_time")
    )

    # 计算天数
    consumerDaysDF = consumerDaysDF.select(
        col("memberId").alias("id"),
        # 计算天数
        datediff(col("now_time"), col("finish_time")).alias("consumer_days")
    )

    # 显示结果
    # consumerDaysDF.show()

    rst = (consumerDaysDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("consumer_days").between(col("start"), col("end"))  # 筛选出bornDate在start和end之间的行
    )
    .select(
        col("id").alias("userId"),  # 重命名id列为userId
        col("name").alias("consumptionCycle")
    )
    )
    # rst.show()
rst.write.format("jdbc").mode("overwrite") \
    .option("truncate", "true") \
    .option("url", url) \
    .option("dbtable", 'tbl_consumeCycle_tag') \
    .option("user", 'root') \
    .option("password", 'admin') \
    .save()
