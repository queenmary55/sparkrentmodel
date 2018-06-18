#!usr/bin/env python
# - * - coding:utf-8 - * -


from pyspark.ml.feature import OneHotEncoder, StringIndexer,StringIndexerModel
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel


import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# from pyspark.mllib.linalg import Vectors, VectorUDT

from pyspark.sql.functions import udf
import pandas as pd
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel


os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

sparkConf = SparkConf() \
    .setAppName('pyspark rentmodel') \
    .setMaster('local[*]')
sc = SparkContext.getOrCreate(sparkConf)

sc.setLogLevel('ERROR')
spark = SparkSession(sparkContext=sc)

parquet_df = spark.read.parquet('/root/daxing.parquet')

print(parquet_df.columns)

model_path = "/root/save_data_processed_models/"
columns_list = []
col_names = parquet_df.columns
for i in col_names:
    if i == 'facilities_vectors':
        loadedCountVectorizerModel = CountVectorizerModel.load(model_path + 'count-vectorizer-model')
        temp = loadedCountVectorizerModel.vocabulary
        columns_list.extend(temp)
    elif i == 'rent_typeVec':
        loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelrent_type')
        temp = loadedStringIndexerModel.labels
        columns_list.extend(temp)
    elif i == 'agency_nameVec':
        loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelagency_name')
        temp = loadedStringIndexerModel.labels
        columns_list.extend(temp)
    elif i == 'directionVec':
        loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modeldirection')
        temp = loadedStringIndexerModel.labels
        columns_list.extend(temp)
    elif i == 'zoneVec':
        loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelzone')
        temp = loadedStringIndexerModel.labels
        columns_list.extend(temp)
    elif i == 'pay_typeVec':
        loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelpay_type')
        temp = loadedStringIndexerModel.labels
        columns_list.extend(temp)
    else:
        columns_list.append(i)

print(len(columns_list),'========',columns_list)


# loadedIndexer = StringIndexer.load('/root/oneHotModels_61/')
# loadedStringIndexerModel = StringIndexerModel.load('/root/oneHotModels_61/stringIndexer_modelzone')
# loadedStringIndexerModel.labels
# loadedCountVectorizerModel = CountVectorizerModel.load('save_data_processed_models/count-vectorizer-model')
# loadedCountVectorizerModel.vocabulary
