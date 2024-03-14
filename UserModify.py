
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, date_add, from_unixtime
'''
# 构建SparkSession实例对象
spark = SparkSession.builder \
    .appName("UserModify") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

# mysql 配置
prop = {'user': 'root',
        'password': 'admin',
        'driver': 'com.mysql.jdbc.Driver'}
# database 地址
url = ('jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'
       '&rewriteBatchedStatements=true')
# 读取表
df = spark.read.jdbc(url=url, table='tbl_users', properties=prop)

# 将会员ID值和支付方式值，使用UDF函数
newUsersDF = df \
    .withColumn("lastLoginTime", unix_timestamp(
    date_add(from_unixtime(col("lastLoginTime")), 3067),
    "yyyy-MM-dd"
)
                )
# 时间是2015年的数据，加上3060天模拟为近年数据

# 显示转换后的DataFrame
# newOrdersDF.show(185)

# newOrdersDF.write.jdbc(url=url, table='tbl_orders_new', mode='append', properties=prop)

newUsersDF.write.format("jdbc").mode("overwrite") \
    .option("truncate", "true") \
    .option("url", url) \
    .option("dbtable", 'tbl_users_new') \
    .option("user", 'root') \
    .option("password", 'admin') \
    .save()
'''