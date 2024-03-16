# import findspark
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from models.statistics.TagTools import rule_to_tuple_udf


class DoBuyFrequencyTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoBuyFrequencyTag'). \
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
            .where(col("id") == 236) \
            .head() \
            .asDict()["rule"] \
            .split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")
        # print(rule)

        # 取五级标签
        attr = df.filter("level==5") \
            .where(col("pid") == 236) \
            .select(col("name"), rule_to_tuple_udf(col("rule")).alias("rules")) \
            .select(
            col("name"),  # 选择name列
            col("rules.start").alias("start"),  # 从rules结构体中选择start字段
            col("rules.end").alias("end")  # 从rules结构体中选择end字段
        )

        df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
        biz = df2.select(selectField)

        # 可以过滤掉6个月前的数据，但本次数据均在一个月内，不过滤，直接计算订单总数/6个月
        # 按照用户ID分组，获取订单数量
        countOrderDF = biz.groupBy("memberId") \
            .agg({'orderSn': 'count'}).withColumnRenamed('count(orderSn)', 'countOrder')
        # countOrderDF.show()

        '''
        # 计算finishTime的次数（和计算orderSn数量的结果一样）
        +--------+----------+
        |memberId|countOrder|
        +--------+----------+
        |      29|       248|
        |      26|       217|
        |     474|       109|
        |      65|       228|
        |     418|       106|
        |     541|       110|
        |     558|       109|
        |     191|       118|
        |     938|       120|
        |     270|       101|
        |     293|       132|
        +--------+----------+
        '''
        # 计算周期是近半年,结果是一个月购买多少次
        frequencyDF = countOrderDF.select(
            col("memberId"),
            # 计算
            round(col("countOrder") / 6, 0).alias("frequency")
        )
        # frequencyDF.show()

        rst = (frequencyDF.join(attr, on=None)  # 连接attr，这里默认使用两个DataFrame中同名的列作为连接键
        .where(
            col("frequency").between(col("start"), col("end"))  # 筛选出frequency在start和end之间的行
        )
        .select(
            col("memberId").alias("userId"),  # 重命名id列为userId
            col("name").alias("value"),
            col("frequency")
        )
            # .orderBy("userId")
        )

        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_buyFrequency_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("购买频率标签计算完成！")

# if __name__ == '__main__':
#     DoBuyFrequencyTag.start()
