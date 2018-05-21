#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: derive_features.py
@time: 2018/04/{DAY}
"""

import pandas as pd
import numpy as np

from math_functions.math import Math
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,FloatType


def div(df, col1, col2):
    # 把空值去掉
    df = df.filter(df[col1].isNotNull())
    df = df.filter(df[col2].isNotNull())

    # 去除点值为0的行
    df = df.filter(df[col1] != 0)
    df = df.filter(df[col2] != 0)

    col1_df = df.select(col1).collect()
    col2_df = df.select(col2).collect()

    col1_list = []
    col2_list = []
    for i in col1_df:
        col1_list.append(i[0])
    for j in col2_df:
        col2_list.append(j[0])

    div_result = []
    for k in range(len(col2_list)):
        tmp = round(float(col1_list[k]) / float(col2_list[k]),2)
        div_result.append(tmp)
    return div_result


class DeriveFeature(object):
    def __init__(self):
        pass


    """
        1、创建一列one_room_area
        2、rent_type为合租的不用计算,通过观察合租的面积就是一个房间的面积，因此只计算整租的情况
        3、在rent_type类型为整租的情况下，用面积area除以房间数room_num
    """

    @staticmethod
    def oneRoomArea(df):
        div_result = div(df,'area','room_num')
        div_resultIterator = iter(div_result)

        def udf_oneRoomArea(s):
            return float(next(div_resultIterator, '-1'))


        transf_udf = udf(udf_oneRoomArea,FloatType())
        df = df.select(
            '*', transf_udf(df['area']).alias('one_room_area'))

        return df


    @staticmethod
    def oneAreaPrice(df):
        div_result = div(df, 'price', 'area')
        div_resultIterator = iter(div_result)
        def udf_oneRoomArea(s):
            return float(next(div_resultIterator, '-1'))

        transf_udf = udf(udf_oneRoomArea,FloatType())
        df = df.select(
            '*', transf_udf(df['area']).alias('one_area_price'))

        return df


def derive(df):
    # 把空值去掉
    df = df.filter(df['area'].isNotNull())
    df = df.filter(df['price'].isNotNull())
    df = df.filter(df['room_num'].isNotNull())

    df = df.filter(df['area'] != 'NULL')
    df = df.filter(df['price'] != 'NULL')
    df = df.filter(df['room_num'] != 'NULL')



    df = DeriveFeature.oneRoomArea(df)
    df = DeriveFeature.oneAreaPrice(df)

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

    df = derive(df)
    df.show(truncate=False)



    spark.stop()
    sc.stop()
