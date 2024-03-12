from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, unix_timestamp, date_add, from_unixtime
from pyspark.sql.types import StringType
import random

# 只运行一次！生成tbl_orders_new表格

'''
# 构建SparkSession实例对象
spark = SparkSession.builder \
    .appName("OrderModify") \
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
df = spark.read.jdbc(url=url, table='tbl_orders', properties=prop)


# 自定义UDF函数，处理UserId
def user_id_udf(userId):
    if int(userId) >= 950:
        return str(random.randint(1, 950))
    else:
        return userId


user_id_udf_py = udf(user_id_udf, StringType())

# 自定义UDF函数，处理paymentcode
paycodeList = ["alipay", "wxpay", "chinapay", "cod", "kjtpay", "giftCard"]

def pay_code_udf(paymentcode):
    # if paymentcode == "ccb" or paymentcode == "chinaecpay":
    # return paycodeList[2]
    if paymentcode not in paycodeList:
        return paycodeList[random.randint(0, 5)]
    else:
        return paymentcode


pay_code_udf_py = udf(pay_code_udf, StringType())

# 定义支付方式的映射
payMap = {
    "alipay": "支付宝",
    "wxpay": "微信支付",
    "chinapay": "银联支付",
    "cod": "货到付款",
    "kjtpay": "快捷通",
    "giftCard": "礼品卡"
}
def pay_name_udf(paymentcode):
    return payMap.get(paymentcode, "未知支付方式")

pay_name_udf_py = udf(pay_name_udf, StringType())

# 将会员ID值和支付方式值，使用UDF函数
newOrdersDF = df \
    .withColumn("memberId", user_id_udf_py(col("memberId"))) \
    .withColumn("paymentCode", pay_code_udf_py(col("paymentCode"))) \
    .withColumn("paymentName", pay_name_udf_py(col("paymentCode"))) \
    .withColumn("finishTime", unix_timestamp(
    date_add(from_unixtime(col("finishTime")), 1640),
    "yyyy-MM-dd"
)
                )
# 时间是2019年的数据，加上1640天模拟为近年数据

# 显示转换后的DataFrame
#newOrdersDF.show(185)

# newOrdersDF.write.jdbc(url=url, table='tbl_orders_new', mode='append', properties=prop)

newOrdersDF.write.format("jdbc").mode("overwrite") \
    .option("truncate", "true") \
    .option("url", url) \
    .option("dbtable", 'tbl_orders_new') \
    .option("user", 'root') \
    .option("password", 'admin') \
    .save()
    
'''
