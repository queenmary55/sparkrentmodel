#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: spark_template.py
@time: 2018/04/{DAY}
"""


from random import choice
import pandas as pd
import numpy as np

from pyspark.sql.functions import udf,stddev,mean


class Math(object):
    def __init__(self):
        pass

    @staticmethod
    def mode(df, column):
        temp_df = df.filter(df[column].isNotNull())
        temp_df = temp_df.select(column).collect()
        lst = []
        for i in temp_df:
            lst.append(i[column])
        # 统计list中各个数值出现的次数
        count_dict = {}
        for i in lst:
            if i in count_dict:
                count_dict[i] += 1
            else:
                count_dict[i] = 1

        # 求出现次数的最大值
        max_appear = 0
        for v in count_dict.values():
            if v > max_appear:
                max_appear = v
        # if max_appear == 1:
        #     mode_num = choice(lst)

        mode_list = []
        for k, v in count_dict.items():
            if v == max_appear:
                mode_list.append(k)
        if len(mode_list) == 1:
            mode_num = mode_list[0]
        else:
            mode_num = choice(mode_list)

        return mode_num




    # pyspark版的西格玛法
    @staticmethod
    def XiGeMa(df, column, mutiple):
        df_non_nan = df.filter(df[column].isNotNull())
        avg = df_non_nan.select(mean(column).alias(column)).collect()
        avg = round(avg[0][column],2)
        std = df_non_nan.select(stddev(column).alias(column)).collect()
        std = round(std[0][column],2)

        if mutiple == 3:
            down_limit = avg - (3 * std)
            up_limit = avg + (3 * std)
        elif mutiple == 2:
            down_limit = avg - (2 * std)
            up_limit = avg + (2 * std)
        elif mutiple == 1:
            down_limit = avg - std
            up_limit = avg + std
        else:
            print('error: mutiple is only in [1,2,3]')

        def udf_XiGeMa(s):
            if (s == None) | (s == 'NULL'):
                return s
            else:
                s = float(s)
                if (s >= down_limit) & (s <= up_limit):
                    return s
                else:
                    return None
        udf_transf = udf(udf_XiGeMa)
        df = df_non_nan.select(
            '*', udf_transf(df_non_nan[column]).alias('temp_name'))
        df = df.drop(column)

        df = df.withColumnRenamed('temp_name', column)
        df = df.filter(df[column].isNotNull())

        return df

    #pandas 版的西格玛
    @staticmethod
    def pandas_XiGeMa(df, column, mutiple):
        df_non_nan = df[df[column].notnull()]
        df_nan = df[df[column].isnull()]
        m = np.mean(df_non_nan[column])
        std = np.std(df_non_nan[column])

        if mutiple == 3:
            down_limit = m - (3 * std)
            up_limit = m + (3 * std)
        elif mutiple == 2:
            down_limit = m - (2 * std)
            up_limit = m + (2 * std)
        elif mutiple == 1:
            down_limit = m - std
            up_limit = m + std
        else:
            pass

        remove_outlier = df_non_nan[(df_non_nan[column] >= down_limit) & (df_non_nan[column] <= up_limit)]
        df_non_outlier = df.loc[remove_outlier.index]

        # price缺失值所在的行直接删除
        if column == 'price':
            df = df_non_outlier
        else:
            df = pd.concat([df_non_outlier, df_nan])

        return df









