#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: test.py
@time: 2018/04/{DAY}
"""

import pandas as pd
import numpy as np
from RentModel.configing import config as cf


def direction(df):
    uniformity_dict = cf.get('uniformity_direction')
    uniformity_dict_keys = list(uniformity_dict.keys())

    df_non_nan = df[df['direction'].notnull()]

    for i in list(df_non_nan.index):
        if df['direction'].values[i].strip() in uniformity_dict_keys:
            value = uniformity_dict.get(df['direction'].values[i].strip())
            print("(df['decoration'].values[i].strip(),value)",(df['direction'].values[i].strip(),value))
            df['direction'].values[i] = value
        else:
            df['direction'].values[i] = np.nan

    return df


def decoration(df):
    df['decoration'][df['decoration'].isnull()] = df['decoration'].fillna(0)
    uniformity_decoration_dict = cf.get('uniformity_decoration')
    uniformity_decoration_dict_keys = list(uniformity_decoration_dict.keys())

    df_non_nan = df[df['decoration'].notnull()]

    for i in list(df_non_nan.index):
        if df['decoration'].values[i] in uniformity_decoration_dict_keys:
            value = uniformity_decoration_dict.get(df['decoration'].values[i])
            df['decoration'].values[i] = value
        else:
            df['decoration'].values[i] = 0

    return df



def threeSTD(df,column):
    df_non_nan = df[df[column].notnull()]
    df_nan = df[df[column].isnull()]
    m = np.mean(df_non_nan[column])
    std = np.std(df_non_nan[column])

    down_limit = m - (3 * std)
    up_limit = m + (3 * std)


    remove_outlier = df_non_nan[(df_non_nan[column] >= down_limit) & (df_non_nan[column] <= up_limit)]
    df_non_outlier = df.loc[remove_outlier.index]

    #price缺失值所在的行直接删除
    if column == 'price':
        df = df_non_outlier
    else:
        df = pd.concat([df_non_outlier,df_nan])

    return  df


def checkNullRate(df):
    df_rows = df.shape[0]
    colums = df.columns.tolist()

    if df_rows > 0:
        for i in range(len(colums)):
            df_non_nan = df[df[colums[i]].notnull()]
            rate = df_non_nan.shape[0]/df_rows
            if rate < 0.3:
                del df[colums[i]]
            else:
                pass
    else:
        print('the dataframe is null')

    return df




if __name__ == '__main__':

    df = pd.read_csv("D:/pyCharmSpace/DataCleanPython3V2/RentModel/new_data.csv", encoding='gbk')
    df = checkNullRate(df)
    df.to_csv('D:/pyCharmSpace/DataCleanPython3V2/RentModel/test_df.csv', encoding='gbk')
