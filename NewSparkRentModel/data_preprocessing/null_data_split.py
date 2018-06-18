#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: null_data_split.py
@time: 2018/04/{DAY}
"""

from resource.configing import config as cf
from sklearn.model_selection import train_test_split


"""
    df被分为两部分，值为空（null,在excel中是空白的形式）的部分temp_df_nan，第二部分是正常部分temp_df,
"""

def nullDataSplit(df, column):

    df = df.toPandas()#spark dataframe to pandas dataframe

    temp_nan = df[df[column].isnull()]

    if temp_nan.shape[0] > 0:
        # df = df.drop(temp_nan.index, inplace=True)这种事把删除了的行赋值给了df
        # 正确的做法要么是df.drop(temp_nan.index, inplace=True)不赋值
        # 要么df = df.drop(temp_nan.index)，不要inplace=True这个
        df = df.drop(temp_nan.index)
        # 上述方法得到的temp_df，其行索引已经不连续（因为中间有NaN的行的索引没有，pandas也不自动重新编码连续的行号，
        # 导致按自然数为行号来遍历时出错，因此需要手动重新建立连续的索引。）
        df.index = range(df.shape[0])

    length = df[column].shape[0]


    if temp_nan.shape[0] > 0:
        return df, temp_nan, length

    else:
        return df, length

def trainDataSplit(df):
    columns_list = df.columns.tolist()
    columns_list.remove('price')
    df_X = df[columns_list]
    df_y = df['price']
    train_size = int((df_X.shape[0]) * cf.get('train_size_rate'))

    X_train, X_test, y_train, y_test = train_test_split(df_X, df_y, train_size=train_size, random_state=4)

    return X_train, X_test, y_train, y_test



if __name__ == '__main__':
    from pyspark.sql import SparkSession
    import os

    os.environ['SPARK_HOME'] = 'D:/spark-2.3.0-bin-hadoop2.7'
    os.environ['HADOOP_HOME'] = 'D:/pyCharmSpace/SparkProject/winuntil'

    spark = SparkSession \
        .builder \
        .master('local[2]') \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    df = spark.read.csv('D:/ganji_beijing_data/ganji_beijing_pyspark.csv', header=True, encoding='gbk')
    df.cache()