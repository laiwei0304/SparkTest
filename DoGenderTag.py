from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoGenderTag'). \
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
    # 打印data数据类型
    # print(type(data))
    # 展示数据
    # data.show()
    # 关闭spark会话
    # spark.stop()

    rule = df.filter("level==4") \
        .where(col("id") == 106) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    attr = df.filter("level==5") \
        .where(col("pid") == 106) \
        .select("name", "rule")
    # attr.show()

    df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
    biz = df2.select(selectField)
    rst = biz.join(attr, col("gender") == col("rule")) \
        .drop("gender", "rule") \
        .withColumnRenamed("name", "gender") \
        .withColumnRenamed("id", "user_id") \
        .orderBy("user_id")

    # rst.show()

    rst.write.jdbc(url=url, table='tbl_gender_tag', mode='append', properties=prop)
