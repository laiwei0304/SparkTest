import findspark
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, concat_ws
from models.statistics.TagTools import url_to_product, string_to_product_udf
from pyspark.sql.types import StringType, IntegerType
from models.mining.MLModelTools import MLModelTools
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator


class DoBpTag(object):

    @staticmethod
    def start():
        findspark.init()

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
        # df.show(50)

        ratings_df = (df
        .filter("productId != 'not_a_product'")
        # .filter(col("productId").isNotNull())  # 过滤不为空的数据
        # 统计每个用户点击各个商品的次数
        .groupBy("userId", "productId")
        .count()
        .select(
            col("userId").cast(IntegerType()),  #
            col("productId").cast(IntegerType()),  #
            col("count").alias("rating").cast(IntegerType())  #
        )
        )
        # ratings_df.show(30)
        '''
        +------+---------+-----+
        |userId|productId|count|
        +------+---------+-----+
        |   673|    10217|    2|
        |    94|     9605|    1|
        |   237|     6038|    2|
        '''

        alsModel = MLModelTools.loadModel(ratings_df, "bp")

        # 转换ratings_df，以便得到预测值
        # predictions_df = alsModel.transform(ratings_df)
        # predictions_df.show()

        # 调用recommendForAllUsers方法获取所有用户的推荐结果
        rmdItemsDF = alsModel.recommendForAllUsers(5)

        # 构造modelDF DataFrame，选择userId，以及将recommendations数组中的productId和rating分别提取出来
        modelDF = (
            rmdItemsDF
            .select(
                col("userId"),
                col("recommendations.productId").alias("productIds"),
                col("recommendations.rating").alias("ratings")
            )
            .select(
                col("userId").alias("userId").cast(StringType()),
                concat_ws(",", col("productIds")).alias("productIds"),
                concat_ws(",", col("ratings")).alias("ratings")
            )
        )

        # 打印DataFrame的schema
        # modelDF.printSchema()

        # 显示前10行数据，不进行截断
        # modelDF.show(10, truncate=False)

        '''
        +------+--------------------------+------------------------------------------------------+
        |userId|productIds                |ratings                                               |
        +------+--------------------------+------------------------------------------------------+
        |471   |6603,10935,9371,6395,11949|0.24074095,0.23915184,0.2311815,0.21431997,0.20516977 |
        |463   |6603,10935,9371,6395,11949|0.21344179,0.21203287,0.20496635,0.19001685,0.18190423|
        |833   |6603,10935,9371,6395,11949|0.20551993,0.20416333,0.19735909,0.18296441,0.17515288|
        |496   |6603,10935,9371,6395,11949|0.21262854,0.211225,0.20418541,0.18929286,0.18121114  |
        |148   |6603,10935,9371,6395,11949|0.19759148,0.19628724,0.18974546,0.17590612,0.16839594|
        |540   |6603,10935,9371,6395,11949|0.21321389,0.21180649,0.20474751,0.18981396,0.18171   |
        |243   |6603,10935,9371,6395,11949|0.23329245,0.23175251,0.22402877,0.20768893,0.19882181|
        |392   |6603,10935,9371,6395,11949|0.23288094,0.23134376,0.22363363,0.20732261,0.19847114|
        |623   |6603,10935,9371,6395,11949|0.21432339,0.21290867,0.20581295,0.19080171,0.18265559|
        |737   |6603,10935,9371,6395,11949|0.24617983,0.24455485,0.23640442,0.21916196,0.209805  |
        +------+--------------------------+------------------------------------------------------+
        '''
        recDF = modelDF \
            .select(col("userId"), string_to_product_udf(col("productIds")).alias("products")) \
            .select(
            col("userId"),  # 选择name列
            col("products.top1").alias("top1"),
            col("products.top2").alias("top2"),
            col("products.top3").alias("top3"),
            col("products.top4").alias("top4"),
            col("products.top5").alias("top5"),
        )

        recDF.show()
        # productId没有10935等商品！！tbl_goods数据不全！
        #         df2 = spark.read.jdbc(url=url, table='tbl_goods', properties=prop)
        #         nameDF=df2.groupBy(col("productId"), col("productName")).count()\
        #                 .select(col("productId"), col("productName"))
        #         # nameDF.show()
        #
        #         #top1
        #         recDF=recDF.withColumnRenamed('top1', 'productId')
        #         recDF=recDF.join(nameDF,on="productId")\
        #                     .withColumnRenamed('productName', 'top1')\
        #                     .drop("productId")
        #         recDF.show()
        #         nameDF.show()
        #         # top2
        #         recDF2 = recDF.withColumnRenamed('top2', 'productId')
        #         recDF2.show()
        #         recDF2 = recDF2.join(nameDF, on="productId") \
        #             .withColumnRenamed('productName', 'top2') \
        #             .drop("productId")
        #         recDF2.show()
        #     # top3
        #         recDF3 = recDF2.withColumnRenamed('top3', 'productId')
        #         recDF3 = recDF3.join(nameDF, on="productId") \
        #             .withColumnRenamed('productName', 'top3') \
        #             .drop("productId")
        #         recDF3.show()
        # # top4
        #         recDF4 = recDF3.withColumnRenamed('top4', 'productId')
        #         recDF4 = recDF4.join(nameDF, on="productId") \
        #             .withColumnRenamed('productName', 'top4') \
        #             .drop("productId")
        #         recDF4.show()
        # # top5
        #         recDF5 = recDF4.withColumnRenamed('top5', 'productId')
        #         recDF5 = recDF5.join(nameDF, on="productId") \
        #             .withColumnRenamed('productName', 'top5') \
        #             .drop("productId")
        #
        #         recDF5.show()

        recDF.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_bp_tag') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("用户购物偏好标签计算完成！")

# if __name__ == '__main__':
#   DoBpTag.start()
