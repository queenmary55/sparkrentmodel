#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: sparktest.py
@time: 2018/05/{DAY}
"""

import pandas as pd
from resource.configing import config as cf

def oneHotEncoding(df):
    df = df.toPandas()
    cols = df.columns
    onHotFields = cf.get('null_no_processing')

    for i in onHotFields:

        if i in cols.tolist():
            pass
        else:

            onHotFields.remove(i)

    differ_set = list(set(cols) - set(onHotFields))
    df_oneHot = df[onHotFields]
    df_no_oneHot = df[differ_set]
    df_oneHot = pd.get_dummies(df_oneHot, dummy_na=True)
    df = pd.concat([df_oneHot, df_no_oneHot], axis=1)

    df = spark.createDataFrame(df)

    return df


if __name__ == '__main__':
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

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')

    df = oneHotEncoding(df)

    df.show(truncate=False)

    spark.stop()
    sc.stop()










