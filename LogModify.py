from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_add
'''
# 构建SparkSession实例对象
spark = SparkSession.builder \
    .appName("LogModify") \
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
df = spark.read.jdbc(url=url, table='tbl_logs', properties=prop)

newLogsDF = df \
    .withColumn("log_time", date_add(col("log_time"), 1670))

# 时间是2019年的数据，模拟为近年数据

# 显示转换后的DataFrame
newLogsDF.show(185)

# newLogsDF.write.jdbc(url=url, table='tbl_orders_new', mode='append', properties=prop)

newLogsDF.write.format("jdbc").mode("overwrite") \
    .option("truncate", "true") \
    .option("url", url) \
    .option("dbtable", 'tbl_logs_new') \
    .option("user", 'root') \
    .option("password", 'admin') \
    .save()
'''



