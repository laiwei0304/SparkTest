from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType

# 初始化Spark会话（如果还没有的话）
spark = SparkSession.builder.appName("MyUDFs").getOrCreate()

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
