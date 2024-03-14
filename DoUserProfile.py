# -*- coding: utf-8 -*-
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class DoUserProfile(object):

    @staticmethod
    def start():
        findspark.init()

        # spark 初始化
        spark = SparkSession. \
            Builder(). \
            appName('DoGenderTag'). \
            master('local'). \
            config("spark.debug.maxToStringFields", "100"). \
            getOrCreate()
        # mysql 配置
        prop = {'user': 'root',
                'password': 'admin',
                'driver': 'com.mysql.jdbc.Driver'}
        # database 地址
        url = 'jdbc:mysql://172.16.0.189:3306/tags_dat?useSSL=false&useUnicode=true&characterEncoding=utf8'

        # df = spark.read.jdbc(url=url, table='tbl_user_profile', properties=prop).select("user_id")
        gender_df = spark.read.jdbc(url=url, table='tbl_gender_tag', properties=prop)
        job_df = spark.read.jdbc(url=url, table='tbl_job_tag', properties=prop)
        nationality_df = spark.read.jdbc(url=url, table='tbl_nationality_tag', properties=prop)
        marriage_df = spark.read.jdbc(url=url, table='tbl_marriage_tag', properties=prop)
        politicalFace_df = spark.read.jdbc(url=url, table='tbl_politicalface_tag', properties=prop)
        isBlackList_df = spark.read.jdbc(url=url, table='tbl_isblacklist_tag', properties=prop)
        rfm_df = spark.read.jdbc(url=url, table='tbl_rfm_tag', properties=prop)
        rfe_df = spark.read.jdbc(url=url, table='tbl_rfe_tag', properties=prop)
        psm_df = spark.read.jdbc(url=url, table='tbl_psm_tag', properties=prop)
        usg_df = spark.read.jdbc(url=url, table='tbl_usg_tag', properties=prop)

        add_df = gender_df.join(job_df, on='user_id').join(nationality_df, on='user_id') \
            .join(marriage_df, on='user_id').join(politicalFace_df, on='user_id').join(isBlackList_df, on='user_id') \
            .join(rfm_df, on='user_id').join(rfe_df, on='user_id').join(psm_df, on='user_id')\
            .join(usg_df, on='user_id').orderBy("user_id")
        add_df.show()
        # add_df.write.format("jdbc").mode("overwrite") \
        #     .option("truncate", "true") \
        #     .option("url", url) \
        #     .option("dbtable", 'tbl_user_profile') \
        #     .option("user", 'root') \
        #     .option("password", 'admin') \
        #     .save()


if __name__ == '__main__':
    DoUserProfile.start()
