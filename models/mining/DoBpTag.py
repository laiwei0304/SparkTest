import findspark
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from models.statistics.TagTools import url_to_product
from pyspark.sql.types import StringType, IntegerType
from MLModelTools import MLModelTools
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


'''
class DoBpTag(object):

    @staticmethod
    def start():
        findspark.init()
'''
if __name__ == '__main__':
    # DoBpTag.start()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoBpTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "200"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = ('jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'
               '&rewriteBatchedStatements=true')

        df = spark.read.jdbc(url=url, table='tbl_logs_new', properties=prop)

        df = df.withColumn("productId", url_to_product(col("loc_url"))) \
            .select(col("global_user_id").alias("userId"),
                    col("productId"))
        df.show(50)

        ratings_df = (df
        .filter("productId != 'not_a_product'")
        # .filter(col("productId").isNotNull())  # 过滤不为空的数据
        # 统计每个用户点击各个商品的次数
        .groupBy("userId", "productId")
        .count()
        .select(
            col("userId").cast(IntegerType()),  #
            col("productId").cast(IntegerType()),  #
            col("count").alias("rating").cast(IntegerType()) #
        )
        )
        ratings_df.show(30)
        '''
        +------+---------+-----+
        |userId|productId|count|
        +------+---------+-----+
        |   673|    10217|    2|
        |    94|     9605|    1|
        |   237|     6038|    2|
        '''

        alsModel = MLModelTools.loadModel(ratings_df, "bp")





