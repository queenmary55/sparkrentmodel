#!usr/bin/env python
# - * - coding:utf-8 - * -

"""
@author:limeng
@file: spark_one_hot_encoding.py
@time: 2018/04/{DAY}
"""

import pandas as pd
from resource.configing import config as cf

from pyspark.ml.feature import OneHotEncoder, StringIndexer,StringIndexerModel
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.sql.functions import udf


def oneHot(df, base_col_name, col_name):
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os
    import time

    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('WARN')

    spark = SparkSession(sparkContext=sc)

    df = df.select(base_col_name, col_name)
    df = df.filter(df[base_col_name].isNotNull())
    # StringIndexer'handleInvalid of python'version no have 'keep',so it can't process null value
    null_col_name = col_name + '_null'
    df = df.na.fill(null_col_name, col_name)
    df_NULL = df.filter(df[col_name] == 'NULL')

    temp_path = '/root/oneHotModels_61/'

    if df_NULL.count() > 0:

        def udf_NULL(s):
            return null_col_name

        udf_transf = udf(udf_NULL)

        df_NULL = df_NULL.select('*', udf_transf(col_name).alias('tmp_col_name'))
        df_NULL = df_NULL.na.fill(null_col_name, 'tmp_col_name')
        df_NULL = df_NULL.drop(col_name)
        df_NULL = df_NULL.withColumnRenamed('tmp_col_name', col_name)

        df_no_NULL = df.filter(df[col_name] != 'NULL')
        df_no_NULL = df_no_NULL.withColumn('tmp_col_name', df[col_name])
        df_no_NULL = df_no_NULL.drop(col_name)
        df_no_NULL = df_no_NULL.withColumnRenamed('tmp_col_name', col_name)
        df = df_no_NULL.union(df_NULL)
        del df_no_NULL

    index_name = col_name + 'Index'
    vector_name = col_name + 'Vec'

    """
        StringIndexer可以设置handleInvalid='skip'，但是不可以设置handleInvalid='keep'.
        设置这个会删除需要跳过的这一行，这样会导致用户体验差，因为用户输入
        一条数据，就直接给删了，什么都没有。因此暂不设置，新数据输入时，如果没有，
        可以在已经有的字符串中随机选择一个来替换没有的这个新字符串.
    """
    stringIndexer = StringIndexer(inputCol=col_name, outputCol=index_name)
    model = stringIndexer.fit(df)
    indexed = model.transform(df)
    encoder = OneHotEncoder(dropLast=False, inputCol=index_name, outputCol=vector_name)
    encoded = encoder.transform(indexed)

    #save
    stringIndexer.save(temp_path + 'stringIndexer' + col_name)
    model.save(temp_path + 'stringIndexer_model' + col_name)

    # StringIndexer(inputCol=col_name, outputCol=index_name)
    # onehotEncoderPath = temp_path + col_name
    # loadedEncoder = OneHotEncoder.load(onehotEncoderPath)
    # loadedEncoder.setParams(inputCol=index_name, outputCol=vector_name)
    # encoded = loadedEncoder.transform(df)
    # encoded.show()

    onehotEncoderPath = temp_path + col_name + '_new'
    encoder.save(onehotEncoderPath)

    sub_encoded = encoded.select(base_col_name, vector_name)

    return sub_encoded


def oneHotAll(df):
    onHotFields = cf.get('null_no_processing')
    columns = df.columns
    onHotFields = list(set(onHotFields) & set(columns))
    # if len(onHotFields) TODO
    sdf = oneHot(df, "id", onHotFields[0])

    for i in onHotFields[1:]:
        tmp_sdf = oneHot(df, "id", i)
        sdf = sdf.join(tmp_sdf, on='id', how='inner')

    for j in onHotFields:
        columns.remove(j)

    numerice_sdf = df.select(columns)
    total_df = numerice_sdf.join(sdf, 'id', 'inner')
    del numerice_sdf
    del sdf
    return total_df

if __name__ == '__main__':

    import time
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os

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

    df = oneHotAll(df)
    #
    # # df.write.csv('/root/processed_data',header=True)
    # df.write.mode("overwrite").options(header="true").csv('/root/data/procesded_data/test.csv')
    # # temp_df = df.select('price', 'area', 'room_num', 'hall_num', 'toilet_num', 'floor', 'floor_total')
    #
    # sc.wholeTextFiles

    df.show(truncate=False)

    spark.stop()
    sc.stop()

    end = time.time()

    print('程序运行时间（秒）：', round(end - start, 2))



