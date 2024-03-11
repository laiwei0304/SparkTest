from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_replace
from pyspark.sql.types import StructType, StructField, IntegerType

if __name__ == '__main__':
    # spark 初始化
    spark = SparkSession. \
        Builder(). \
        appName('DoAgeRangeTag'). \
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
        .where(col("id") == 201) \
        .head() \
        .asDict()["rule"] \
        .split(";")
    selectTable = rule[0].split("=")[1]
    selectField = rule[1].split("=")[1].split("|")
    # print(rule)

    # 自定义udf函数,解析五级标签规则
    rule_to_tuple_udf = udf(
        lambda rule: (int(rule.split("-")[0]), int(rule.split("-")[1])),
        StructType([
            StructField("start", IntegerType(), True),
            StructField("end", IntegerType(), True)
        ])
    )
    # 注册UDF
    spark.udf.register("rule_to_tuple", rule_to_tuple_udf)
    # 提取start和end
    # 取五级标签
    attr = df.filter("level==5") \
        .where(col("pid") == 201) \
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
    rst = (biz.select(
        col("id"),  # 选择id列
        regexp_replace(col("birthday"), "-", "").cast(IntegerType()).alias("bornDate")
    ).join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
    .where(
        col("bornDate").between(col("start"), col("end"))  # 筛选出bornDate在start和end之间的行
    )
    .select(
        col("id").alias("userId"),  # 重命名id列为userId
        col("name").alias("ageRange")  # 如果需要，可以取消注释这行来重命名name列为agerange
    )
    )
    # rst.show()

    rst.write.jdbc(url=url, table='tbl_ageRange_tag', mode='append', properties=prop)
