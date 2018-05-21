#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: all_preprocessing.py
@time: 2018/05/{DAY}
"""

import pandas as pd
from features_projects.one_hot_encoding import *
from features_projects.facilities_transform import *
from data_preprocessing.null_fill import NullAndDuplications

from data_preprocessing.data_uniformity import dataUniform
from features_projects.derive_features import *
from data_preprocessing.check_null_rate import checkNullRate
from data_preprocessing.outlier import outlierValue
from pyspark.mllib.regression import LabeledPoint

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
    print('------------before:',df.count(),df.columns)
    df = checkNullRate(df)
    print('------------after:', df.count(), df.columns)

    nad = NullAndDuplications()
    df = nad.finalFillNull(df)
    print('11111111111111111')

    df = spark_tranFacilitiesField(df)
    print('2222222222222222222')

    df = dataUniform(df)
    print('333333333333333333')

    df = oneHotEncoding(df)
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

    # os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

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
    df = df.drop('id')

    df = processingMain(df)

    df.write.csv('/root/processed',header=True)

    df.show(400,truncate=False)

    spark.stop()
    sc.stop()

    end = time.time()

    print('程序运行时间（秒）：', round(end - start, 2))