import findspark
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number


class DoPayTypeTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoPayTypeTag'). \
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
        # 取四级标签的rule,支付方式id=223
        rule = df.filter("level==4") \
            .where(col("id") == 223) \
            .head() \
            .asDict()["rule"] \
            .split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")
        # print(rule)

        # 取五级标签
        attr = df.filter("level==5") \
            .where(col("pid") == 223) \
            .select("name", "rule")
        # attr.show()

        # 读取order_new表
        df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
        biz = df2.select(selectField)

        # a. 按照先用户ID分组，再按支付编码paymentCode分组，使用count函数，统计次数
        paymentDF = (biz.groupBy("memberId", "paymentCode")
                     .count()  # 使用count函数列名称就是：count
                     # b. 使用窗口分析函数，获取最多次数
                     .withColumn(
            "rnk",
            row_number().over(
                Window.partitionBy("memberId").orderBy(col("count").desc())
            )
        )
                     .where(col("rnk") == 1)
                     # c. 选取字段
                     .select(col("memberId").alias("id"), col("paymentCode").alias("payment"))
                     )
        # paymentDF.show()

        rst = paymentDF.join(attr, col("payment") == col("rule")) \
            .drop("payment", "rule") \
            .withColumnRenamed("name", "payment") \
            .withColumnRenamed("id", "userId") \
            .orderBy("userId")
        # rst.show()

        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_payType_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("支付方式标签计算完成！")