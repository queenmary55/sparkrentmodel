#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: outlier.py
@time: 2018/04/{DAY}
"""


import pandas as pd
import numpy as np
from pyspark.sql.functions import udf,stddev,mean

from pyspark.sql.types import StringType,FloatType


# the method has been replaced in the XiGeMa of math.py
def threeSTD(df,column):

    df_non_nan = df.filter(df[column].isNotNull())
    avg = df_non_nan.select(mean(column).alias(column)).collect()
    avg = round(avg[0][column], 2)
    std = df_non_nan.select(stddev(column).alias(column)).collect()
    std = round(std[0][column], 2)


    down_limit = avg - (3 * std)
    up_limit = avg + (3 * std)

    def udf_XiGeMa(s):
        if (s == None) | (s == 'NULL'):
            return s
        else:
            s = float(s)
            if (s >= down_limit) & (s <= up_limit):
                return float(s)
            else:
                return None

    udf_transf = udf(udf_XiGeMa,FloatType())
    df = df_non_nan.select(
        '*', udf_transf(df_non_nan[column]).alias('temp_name'))
    df = df.drop(column)

    df = df.withColumnRenamed('temp_name', column)
    df = df.filter(df[column].isNotNull())

    return df


def outlierValue(df):

    df = df.filter((df['toilet_num'] > 0) & (df['toilet_num'] < 4))
    df = df.filter(df['floor_total'] > 2)
    df = df.filter((df['hall_num'] > 0) & (df['hall_num'] < 3))

    return df
