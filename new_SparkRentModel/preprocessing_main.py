#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: all_preprocessing.py
@time: 2018/05/{DAY}
"""

from features_projects.one_hot_encoding import *
from features_projects.facilities_transform import tranFacilitiesFieldNewMethod
from data_preprocessing.null_fill import NullAndDuplications

from features_projects.data_uniformity import dataUniform
from features_projects.derive_features import *
from data_preprocessing.check_null_rate import checkNullRate
from data_preprocessing.outlier import outlierValue

"""
    processing_main函数中数据处理顺序如下：
    0、检查空值率，并将空置率大于70%的列去掉
    1、空值填充（不包括area)和重复数据删除
    2、facilities字段编码
    3、数据一致性处理
    4、oneHotEncoding编码拍
    5、除area外的异常值处理
    5、area特征空值填充
    6、派生变量的异常值处理   
"""


def processingMain(df):
    import sys
    print('=======',sys.path)
    print('------------before:',df.count(),df.columns)
    df = checkNullRate(df)
    print('------------after:', df.count(), df.columns)

    nad = NullAndDuplications()
    df = nad.finalFillNull(df)
    print('11111111111111111')
    print('df.dtypes11===',df.dtypes)

    df = tranFacilitiesFieldNewMethod(df)
    print('2222222222222222222')
    print('df.dtypes22===', df.dtypes)

    df = dataUniform(df)

    print('333333333333333333')

    df = oneHotAll(df)
    print('444444444444444444444')

    df = outlierValue(df)
    print('666666666666666666666')

    df = derive(df)
    print('777777777777777777777777')
    print(df.dtypes)

    return df


if __name__ == '__main__':

    import time
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os
    from algorithms.spark_algorithms import rfRegressor
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql.types import FloatType
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel

    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('WARN')
    spark = SparkSession(sparkContext=sc)

    start = time.time()

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')
    df = df.drop('bus')
    df = df.drop('_c0')
    # df = df.drop('id')
    df = df.drop('crawl_time')

    df = processingMain(df)
    df.show(400,truncate=False)

    df = df.drop('id')
    print('type(df)',type(df))

    df.write.mode('overwrite').parquet('/root/data/test2.parquet')
    print('successed!!!!!!!!!!!!!!')



    # for i in df.columns:
    #     if 'Vec'not in i:
    #         df = df.select('*',df[i].cast('float').alias('tmp_name')).drop(i)
    #         df = df.withColumnRenamed('tmp_name',i)
    #
    # columns = df.columns
    # columns.remove('price')
    #
    # vecAssembler = VectorAssembler(inputCols=columns, outputCol="features")
    #
    # df = vecAssembler.transform(df)
    # df = df.withColumnRenamed('price', 'label')
    # print('============************--------------=======================')
    # # df.show(truncate=False)
    #
    # sdf = df.select("features", "label")
    #
    # rf = RandomForestRegressor()
    #
    # model = rf.fit(sdf)
    #
    # print('model.featureImportances===========',model.featureImportances)

    # importance_map_df = rfRegressor(df)
    # print('importance_map_df===', importance_map_df)


    #

    # df.write.mode("overwrite").options(header="true").csv('/root/data/procesded_data/test.csv')
    # # temp_df = df.select('price', 'area', 'room_num', 'hall_num', 'toilet_num', 'floor', 'floor_total')
    #
    # sc.wholeTextFiles

    # df.show()
    print(df.dtypes)

    # df.write.csv('/root/processed_data_sparse', header=True)

    spark.stop()
    sc.stop()

    end = time.time()

    print('程序运行时间（秒）：', round(end - start, 2))


from pyspark.sql import DataFrameWriter
