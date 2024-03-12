from pyspark.sql.context import HiveContext
from pyspark.sql.functions import col
from SparkSessionBase import SparkSessionBase


class TestRandJob(SparkSessionBase):
    SPARK_URL = "local"
    SPARK_APP_NAME = 'TextRandJob'
    ENABLE_HIVE_SUPPORT = True

    def __init__(self):
        self.spark = self._create_spark_session()
        self.spark.sparkContext.setLogLevel("ERROR")

    def start(self):
        # XXX 大数据分析代码
        # sql = "SELECT * FROM profile_tags.tbl_basic_tags"
        hc = HiveContext(self.spark.sparkContext)
        # res = hc.sql(sql)
        # res.show()

        # 将DataFrame保存为JSON格式的文件
        # res.write.format("json").mode("overwrite").save("test")
        # 将DataFrame保存为hdfs上的csv格式的文件
        # res.write.csv("sample_file.csv", header=True)
        # 将DataFrame保存为本地的csv格式的文件
        # res.toPandas().to_csv("sample_file.csv", header=True)

        # b_df = hc.table('business')
        # b_df.limit(10).show()

        hc.sql("use tags_dat")
        rule = hc.table("tbl_basic_tags") \
            .filter("level==4") \
            .where(col("id") == 106) \
            .head().asDict()["rule"]
        if not rule:
            raise Exception("性别标签未提供数据源信息，无法获取业务数据")
        else:
            rule = rule.split(";")
        # print(rule)

        # 提取rule字典中的selectTable和selectField
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 从基础标签表中提取该4级标签对应5级标签的名称和规则
        attr = hc.table("tbl_basic_tags") \
            .filter("level==5") \
            .where(col("pid") == 106) \
            .select("name", "rule")
        # attr.show()

        # 从selectTable中提取selectField字段
        df2 = hc.table(selectTable)
        biz = df2.select(selectField)
        # 打标签（不同模型不一样）
        rst = biz.join(attr, col("gender") == col("rule")) \
            .drop("gender", "rule") \
            .withColumnRenamed("name", "gender") \
            .withColumnRenamed("id", "user_id") \
            .orderBy("user_id")
        # rst.show()

        # 存储打好标签的数据
        rst.write.format("hive").mode("overwrite").saveAsTable('tbl_gender_tag')


if __name__ == '__main__':
    TestRandJob().start()
