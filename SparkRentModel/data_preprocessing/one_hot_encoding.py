#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: spark_one_hot_encoding.py
@time: 2018/04/{DAY}
"""

import pandas as pd
from resource.configing import config as cf

from pyspark.ml.feature import OneHotEncoder, StringIndexer
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

    if df_NULL.count() > 0:

        def udf_NULL(s):
            if s == 'NULL':
                return null_col_name
            else:
                return None

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

    index_name = col_name + 'Index'
    vector_name = col_name + 'Vec'
    stringIndexer = StringIndexer(inputCol=col_name, outputCol=index_name)
    model = stringIndexer.fit(df)
    indexed = model.transform(df)
    encoder = OneHotEncoder(dropLast=False, inputCol=index_name, outputCol=vector_name)
    encoded = encoder.transform(indexed)

    sub_encoded = encoded.select(base_col_name, index_name, vector_name)

    category_name = encoded.select(col_name, index_name)
    category_name = category_name.dropDuplicates()
    category_name = category_name.sort(index_name)
    category_name_collect = category_name.select(col_name).collect()
    category_name_list = []
    for row in category_name_collect:
        category_name_list.append(row[0])

    def transf(line):
        num_feature = (line[base_col_name], line[index_name])
        category_feature = tuple(line[vector_name])
        feature = num_feature + category_feature
        return feature

    df1 = sub_encoded.rdd.map(lambda line: transf(line))
    dd = df1.collect()
    pdf = pd.DataFrame(dd)

    num_cols_name = [base_col_name, index_name]
    num_cols_name.extend(category_name_list)
    columns = num_cols_name
    pdf.columns = columns

    sdf = spark.createDataFrame(pdf)


    return sdf


def oneHotAll(df):
    onHotFields = cf.get('null_no_processing')

    sdf = oneHot(df, "id", onHotFields[0])
    sdf = sdf.drop('directionIndex')

    for i in onHotFields[1:]:
        tmp_sdf = oneHot(df, "id", onHotFields[0])
        tmp_sdf = tmp_sdf.drop('directionIndex')
        sdf = sdf.join(tmp_sdf, on='id', how='inner')

    columns = df.columns
    for j in onHotFields:
        if j in columns:
            columns.remove(j)

    numerice_sdf = df.select(columns)
    total_df = numerice_sdf.join(sdf, 'id', 'inner')
    return total_df



