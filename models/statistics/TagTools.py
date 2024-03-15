from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType,StringType
import re

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


# 定义正则表达式模式
regex_pattern = r'^.+\/product\/(\d+)\.html.+$'

# 定义UDF
def url_to_product(url):
    # 尝试匹配正则表达式
    match = re.search(regex_pattern, url)

    # 如果匹配成功，返回组1（即产品ID），否则返回None
    if match:
        return match.group(1)
    else:
        return "not_a_product"

        # 注册UDF

url_to_product = udf(url_to_product, StringType())

# 自定义udf函数,提取给用户推荐的top5商品
string_to_product_udf = udf(
    lambda rule: (int(rule.split(",")[0]), int(rule.split(",")[1]),int(rule.split(",")[2]),int(rule.split(",")[3]),int(rule.split(",")[4])),
    StructType([
        StructField("top1", IntegerType(), True),
        StructField("top2", IntegerType(), True),
        StructField("top3", IntegerType(), True),
        StructField("top4", IntegerType(), True),
        StructField("top5", IntegerType(), True)
    ])
)
# 注册UDF
spark.udf.register("string_to_product", string_to_product_udf)