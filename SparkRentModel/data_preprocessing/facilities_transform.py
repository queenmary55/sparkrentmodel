#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: spark_template.py
@time: 2018/04/{DAY}
"""

import pandas as pd

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, StructField, IntegerType,FloatType


def tranFacilitiesField(df):
    from sklearn.feature_extraction.text import CountVectorizer

    facilities = df.select('facilities').collect()

    temp_list = []
    for i in facilities:
        temp_list.append(i[0])

    lst = []
    for i in temp_list:
        i = str(i)
        if i == 'None':
            lst.append('facilities_null')
        else:
            temp = ' '.join(i.split('|'))
            if '独立卫生间' in temp:
                temp = temp.replace('独立卫生间', '独卫')
            if '独立阳台' in temp:
                temp = temp.replace('独立阳台', '阳台')
            lst.append(temp)
    vectorizer = CountVectorizer(token_pattern='(?u)\\b\\w+\\b')
    facilities = vectorizer.fit_transform(lst)
    tran_arr = facilities.toarray()
    cols = vectorizer.get_feature_names()
    tran_df = pd.DataFrame(tran_arr, columns=cols)

    return tran_df


def spark_tranFacilitiesField(df):
    tran_df = tranFacilitiesField(df)  # pandas 的df
    facilities_fields = list(tran_df.columns)

    for i in facilities_fields:
        facilities_fields_iterator = iter(list(tran_df[i]))

        def udf_facilities_fields(s):
            return next(facilities_fields_iterator)

        transf_udf = udf(udf_facilities_fields, StringType())
        df = df.select(
            '*', transf_udf(df['facilities']).alias(i))

    df = df.drop('facilities')

    return df



if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os

    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[2]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('WARN')

    spark = SparkSession(sparkContext=sc)

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')

    df = spark_tranFacilitiesField(df)
    df.show(truncate=False)

    spark.stop()
    sc.stop()