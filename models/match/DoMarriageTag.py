# -*- coding: utf-8 -*-
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# if __name__ == '__main__':
class DoMarriageTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoMarriageTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "100"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'
        # 读取基础标签表tbl_basic_tags
        df = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)
        # 打印data数据类型
        # print(type(df))
        # 展示数据
        # df.show()
        # 关闭spark会话
        # spark.stop()

        # 从基础标签表tbl_basic_tags中提取规则，存入字典rule中
        rule = df.filter("level==4") \
            .where(col("id") == 120) \
            .head() \
            .asDict()["rule"]
        if not rule:
            raise Exception("婚姻状况标签未提供数据源信息，无法获取业务数据")
        else:
            rule = rule.split(";")
        print(rule)

        # 提取rule字典中的selectTable和selectField
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df.filter("level==5") \
            .where(col("pid") == 120) \
            .select("name", "rule")
        attr.show()

        # 从selectTable中提取selectField字段
        df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
        biz = df2.select(selectField)
        # 打标签（不同模型不一样）
        rst = biz.join(attr, col("marriage") == col("rule")) \
            .drop("marriage", "rule") \
            .withColumnRenamed("name", "marriage") \
            .withColumnRenamed("id", "user_id") \
            .orderBy("user_id")
        rst.show()

        # 存储打好标签的数据
        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_marriage_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("婚姻状况标签计算完成!")
        # spark.stop()
