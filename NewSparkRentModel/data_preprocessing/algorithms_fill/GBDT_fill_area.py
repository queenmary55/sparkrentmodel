#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: GBDT_fill_area.py
@time: 2018/04/{DAY}
"""


import pandas as pd
import numpy as np

from RentModel.configing import config as cf
from sklearn.ensemble import GradientBoostingRegressor as GBDT
from sklearn.model_selection import train_test_split


# for a few continuous values, example:area. random forest is used,because it is fast and good
# put it in the final execution,used in the main program
# test set had not used ,todo

def algorithmGBDT(df):

        df_zero = df[df['area'] == 0]  # area值为0时也需要填充
        df_nan = df[df['area'].isnull() == True]

        if df_nan.shape[0] > 0:

            if df_zero.shape[0] > 0:
                df_nan = pd.concat([df_nan, df_zero])
                df = df[df['area'] != 0]
            rate = df_nan.shape[0]/df.shape[0]

            # area空值和值为0的样本数小于10%就直接删除，否则用算法填充
            if rate < 0.1:
                df.drop(df_nan.index, inplace=True)
            else:
                df_non_nan = df[df['area'].isnull() == False]
                df_non_nan_raw = df_non_nan.copy()


                df_non_nan_y = df_non_nan['area']
                del df_non_nan['area']
                df_non_nan_X = df_non_nan

                train_size = int((df_non_nan_X.shape[0]) * cf.get('train_size_rate'))

                X_train, X_test, y_train, y_test = train_test_split(df_non_nan_X, df_non_nan_y, train_size = train_size, random_state=4)

                reg = GBDT(max_depth=10, n_estimators=200)
                reg.fit(X_train, y_train)

                del df_nan['area']
                df_nan['area'] = reg.predict(df_nan)

                df_nan = df_nan[df_non_nan_raw.columns.tolist()]  # 解决columns顺序对应问题
                df = pd.concat([df_non_nan_raw, df_nan])

            df.index = range(df.shape[0])

        return df