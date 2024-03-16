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
        ageRange_df = spark.read.jdbc(url=url, table='tbl_ageRange_tag', properties=prop)
        buyFrequency_df = spark.read.jdbc(url=url, table='tbl_buyFrequency_tag', properties=prop).select(
            col("userId"), col("value").alias("buyFrequency"))
        consumeCycle_df = spark.read.jdbc(url=url, table='tbl_consumeCycle_tag', properties=prop)
        exchangeRate_df = spark.read.jdbc(url=url, table='tbl_exchangeRate_tag', properties=prop).select(
            col("userId"), col("value").alias("exchangeRate"))
        lastLogin_df = spark.read.jdbc(url=url, table='tbl_lastLogin_tag', properties=prop)
        logFrequency_df = spark.read.jdbc(url=url, table='tbl_logFrequency_tag', properties=prop).select(
            col("userId"), col("value").alias("logFrequency"))
        logTimeSlot_df = spark.read.jdbc(url=url, table='tbl_logTimeSlot_tag', properties=prop).select(
            col("userId"), col("timeSlot").alias("logTimeSlot"))
        maxOrder_df = spark.read.jdbc(url=url, table='tbl_maxOrder_tag', properties=prop).select(
            col("userId"), col("maxOrderRange"))
        payType_df = spark.read.jdbc(url=url, table='tbl_payType_tag', properties=prop).select(
            col("userId"), col("payment").alias("payType"))
        returnRate_df = spark.read.jdbc(url=url, table='tbl_returnRate_tag', properties=prop).select(
            col("userId"), col("value").alias("returnRate"))
        unitPrice_df = spark.read.jdbc(url=url, table='tbl_unitPrice_tag', properties=prop).select(
            col("userId"), col("unitPriceRange"))
        bp_df = spark.read.jdbc(url=url, table='tbl_bp_tag', properties=prop).select(
            col("userId"), col("top1").alias("BpTop1"), col("top2").alias("BpTop2"), col("top3").alias("BpTop3"),
            col("top4").alias("BpTop4"), col("top5").alias("BpTop5"), )

        add_df = gender_df.join(job_df, on='userId').join(nationality_df, on='userId') \
            .join(marriage_df, on='userId').join(politicalFace_df, on='userId').join(isBlackList_df, on='userId') \
            .join(rfm_df, on='userId').join(rfe_df, on='userId').join(psm_df, on='userId') \
            .join(usg_df, on='userId').join(ageRange_df, on='userId').join(buyFrequency_df, on='userId') \
            .join(consumeCycle_df, on='userId').join(exchangeRate_df, on='userId').join(lastLogin_df, on='userId') \
            .join(logFrequency_df, on='userId').join(logTimeSlot_df, on='userId').join(maxOrder_df, on='userId') \
            .join(payType_df, on='userId').join(returnRate_df, on='userId').join(unitPrice_df, on='userId') \
            .join(bp_df,on='userId')\
            .orderBy("userId")
        add_df.show()
        add_df.write.format("jdbc").mode("overwrite") \
            .option("truncate", "true") \
            .option("url", url) \
            .option("dbtable", 'tbl_user_profile') \
            .option("user", 'root') \
            .option("password", 'admin') \
            .save()
        print("所有用户画像标签更新完成！")


# if __name__ == '__main__':
#     DoUserProfile.start()
