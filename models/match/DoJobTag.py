# -*- coding: utf-8 -*-
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# if __name__ == '__main__':
class DoJobTag(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoJobTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "100"). \
            config("spark.executor.memory", "6g"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'
        # 读取基础标签表tbl_basic_tags
        df = spark.read.jdbc(url=url, table='tbl_basic_tags', properties=prop)

        # 从基础标签表tbl_basic_tags中提取规则，存入字典rule中
        rule = df.filter("level==4") \
            .where(col("id") == 113) \
            .head() \
            .asDict()["rule"]
        if not rule:
            raise Exception("职业标签未提供数据源信息，无法获取业务数据")
        else:
            rule = rule.split(";")
        # print(rule)

        # 提取rule字典中的selectTable和selectField
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = df.filter("level==5") \
            .where(col("pid") == 113) \
            .select("name", "rule")
        # attr.show()

        # 从selectTable中提取selectField字段
        df2 = spark.read.jdbc(url=url, table=selectTable, properties=prop)
        biz = df2.select(selectField)
        # 打标签（不同模型不一样）
        rst = biz.join(attr, col("job") == col("rule")) \
            .drop("job", "rule") \
            .withColumnRenamed("name", "job") \
            .withColumnRenamed("id", "userId") \
            .orderBy("userId")
        # rst.show()

        # 存储打好标签的数据
        # rst.write.jdbc(url=url, table='tbl_job_tag', mode='overwrite', properties=prop)
        rst.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_job_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("职业标签计算完成！")
