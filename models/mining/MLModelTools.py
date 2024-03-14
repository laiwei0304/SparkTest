# -*- coding: utf-8 -*-
import os
import string
from pyspark import StorageLevel
from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.clustering import KMeansModel, KMeans
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, VectorIndexer
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType


class MLModelTools(object):

    @staticmethod
    def loadModel(dataframe: DataFrame, mlType: string):
        model = None
        modelPath = "D:\\suncaper\\MLmodels\\" + mlType
        if os.path.exists(modelPath):
            print("模型存在，加载模型")
            if mlType == "rfm" or mlType == "rfe" or mlType == "psm":
                model = KMeansModel.load(modelPath)
            elif mlType == "usg":
                model = PipelineModel.load(modelPath)
            return model
        else:
            print("模型不存在，开始训练模型")
            if mlType == "rfm" or mlType == "psm":
                model = MLModelTools.trainBestKMeansModel(dataframe, 5)
            elif mlType == "rfe":
                model = MLModelTools.trainBestKMeansModel(dataframe, 4)
            elif mlType == "usg":
                model = MLModelTools.trainBestPipelineModel(dataframe)
            model.save(modelPath)
            return model

    @staticmethod
    def trainBestKMeansModel(dataframe: DataFrame, kClusters):
        # 针对KMeans聚类算法来说，超参数有哪些呢？？
        # K值，采用肘部法则确定，但是对于RFM等模型来说，K值确定（等于4或5）
        # 最大迭代次数MaxIters，迭代训练模型最大次数，可以调整
        # 模型调优：调整算法超参数 -> MaxIter
        # 最大迭代次数, 使用训练验证模式完成
        # 1.设置超参数的值
        maxIters = [5, 10, 20]
        dataframe.persist(StorageLevel.MEMORY_AND_DISK)
        # 2.不同超参数的值，训练模型
        # 返回三元组(评估指标, 模型, 超参数的值)
        models = []
        for maxIter in maxIters:
            model = MLModelTools.trainKMeansModel(dataframe, maxIter, kClusters)
            models.append(model)
        print(models)
        # 3.获取最佳模型
        models.sort(key=lambda x: x[0])
        bestModel = models[0][1]
        print(bestModel)
        # 4.返回最佳模型
        return bestModel

    @staticmethod
    def trainKMeansModel(dataframe: DataFrame, maxIter, kClusters):
        kMeans = KMeans() \
            .setFeaturesCol("features") \
            .setPredictionCol("prediction") \
            .setK(kClusters) \
            .setMaxIter(maxIter) \
            .setSeed(31)
        model = kMeans.fit(dataframe)
        ssse = model.computeCost(dataframe)
        # print(f"WSSSE = {ssse}")
        return ssse, model, maxIter

    @staticmethod
    def convertKMeansIndexMap(centers,predictionDf: DataFrame,mlType: string):
        oldIndex = []
        if mlType == "rfm" or mlType == "psm":
            oldIndex = [0, 1, 2, 3, 4]
        elif mlType == "rfe":
            oldIndex = [0, 1, 2, 3]
        centersDict = dict(zip(oldIndex, centers))
        print(f"聚类中心: {centersDict}")
        rfm = []
        for center in centers:
            rfm.append(center.sum())
        rfmDict = dict(zip(oldIndex, rfm))
        print(f"旧聚类中心索引-指标值: {rfmDict}")
        sortedDict = dict(sorted(rfmDict.items(), key=lambda item: item[1], reverse=True))
        print(f"排序后旧聚类中心索引-指标值: {sortedDict}")
        i = 0
        for key, value in sortedDict.items():
            sortedDict[key] = i
            i += 1
        print(f"旧聚类中心索引-新聚类中心索引: {sortedDict}")
        change_index_udf = udf(lambda x: sortedDict[x], IntegerType())
        clusterDf = predictionDf.withColumn("prediction", change_index_udf(col("prediction"))) \
            .select("user_id", "prediction")
        return clusterDf

    @staticmethod
    def trainBestPipelineModel(dataframe):
        dataframe.persist(StorageLevel.MEMORY_AND_DISK)
        # a.特征向量化
        assembler = VectorAssembler() \
            .setInputCols(["color", "product"]) \
            .setOutputCol("raw_features")

        # b.类别特征进行索引
        vectorIndexer = VectorIndexer() \
            .setInputCol("raw_features") \
            .setOutputCol("features") \
            .setMaxCategories(30)

        # c.构建决策树分类器
        dtc = DecisionTreeClassifier() \
            .setFeaturesCol("features") \
            .setLabelCol("label") \
            .setPredictionCol("prediction")

        # 构建Pipeline管道对象，组合模型学习器（算法）和转换器（模型）
        pipeline = Pipeline() \
            .setStages([assembler, vectorIndexer, dtc])

        # 创建网格参数对象实例，设置算法中超参数的值
        paramGrid = ParamGridBuilder() \
            .addGrid(dtc.impurity, ["gini", "entropy"]) \
            .addGrid(dtc.maxDepth, [5, 10]) \
            .addGrid(dtc.maxBins, [32, 64]) \
            .build()

        # f.多分类评估器
        evaluator = MulticlassClassificationEvaluator() \
            .setLabelCol("label") \
            .setPredictionCol("prediction") \
            .setMetricName("accuracy")  # 指标名称，支持：f1、weightedPrecision、weightedRecall、accuracy

        # 创建交叉验证实例对象，设置算法、评估器和数据集占比
        cv = CrossValidator() \
            .setEstimator(pipeline) \
            .setEvaluator(evaluator) \
            .setEstimatorParamMaps(paramGrid) \
            .setNumFolds(3)  # 将数据集划分为K份，其中1份为验证数据集，其余K - 1份为训练收集，通常K >= 3

        # 传递数据集，训练模型
        cvModel = cv.fit(dataframe)

        # 获取最佳模型
        pipelineModel = cvModel.bestModel

        # 返回最佳模型
        return pipelineModel
