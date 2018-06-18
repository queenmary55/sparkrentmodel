#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: LR_fill_room_num.py
@time: 2018/04/{DAY}
"""
import pandas as pd
import numpy as np
from RentModel.configing import config as cf
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split


"""
        1、房间数特征的填充和其他的特征对null的填充不同
        2、房间数不能为0，这也是数据一致性处理的一部分
        3、需要对值为0的情况做类似null的情况的处理
        4、房间数的多少，这个特征与面积特征的相关性很大，可以根据面大小进行填充
        5、填充的方法可以是制定规则，按规则填充，也可以是建立模型进行预测，用预测值填充，这里考虑后者。
"""


# test set had not used ,todo

def linearRegression(df):
    """
    1、删除’area'和'room_num'都为0，null的情况，这种情况无法填充，直接去掉，删除这种情况，就只剩下三种情况了
    2、room_num不为0或者为不为null,area为0或为null,这种情况刚好和面的GBDT算法用来填充area
    3、room_num为0或者为null,area不为0和不为null,这种情况正是本方法要做的事情
    4、’area'和'room_num'都不为0，null的情况用来建立模型
    :param df:
    :return:
    """

    # 1、删除’area'和'room_num'都为0，null的情况
    area_room_num_all_null = df[((df['room_num'] == 0) | (df['room_num'].isnull().values==True)) &
                                ((df['area'] == 0) | (df['area'].isnull().values == True))]
    df.drop(area_room_num_all_null.index,inplace=True)


    # 2、room_num不为0和者为不为null,area为0或为null,这种情况刚好和面的GBDT算法用来填充area
    room_num_non_nan_and_zero = df[((df['room_num'] != 0) & (df['room_num'].isnull().values == False)) & (
            (df['area'] == 0) | (df['area'].isnull().values == True))]

    # 3、room_num为0或者为null,area不为0和不为null,这种情况正是本方法要做的事情
    room_num_nan_and_zero = df[((df['room_num'] == 0) | (df['room_num'].isnull().values == True)) & (
            (df['area'] != 0) & (df['area'].isnull().values == False))]

    # 4、’area'和'room_num'都不为0，null的情况用来建立模型

    rate = room_num_nan_and_zero.shape[0]/df.shape[0]

    if rate < 0.1:
        df.drop(room_num_nan_and_zero.index,inplace=True)

    else:
        df_non_nan_and_zero = df[((df['room_num'] != 0) & (df['room_num'].isnull().values==False)) & (
                    (df['area'] != 0) & (df['area'].isnull().values == False))]
        df_X = df_non_nan_and_zero['area']
        df_y = df_non_nan_and_zero['room_num']

        df_nan_and_zero = room_num_nan_and_zero
        df_pred_data_nan_and_zero = df_nan_and_zero[(df_nan_and_zero['area'].values !=0) &
                                                    (df_nan_and_zero['area'].isnull().values == False)]['area']


        train_size = int((df_X.shape[0]) * cf.get('train_size_rate'))

        X_train, X_test, y_train, y_test = train_test_split(df_X, df_y, train_size=train_size,
                                                            random_state=4)
        X_train, X_test, y_train, y_test = np.array(X_train).reshape(-1, 1), np.array(X_test).reshape(-1, 1), \
                                           np.array(y_train).reshape(-1, 1), np.array(y_test).reshape(-1, 1)

        reg = LinearRegression(normalize=True)
        reg.fit(X_train, y_train)

        df_nan_and_zero.loc[:, 'room_num'] = reg.predict(np.array(df_pred_data_nan_and_zero).reshape(-1, 1))
        df_nan_and_zero = df_nan_and_zero[df_non_nan_and_zero.columns.tolist()]  # 解决columns顺序对应问题

        for i in list(df_nan_and_zero.index):
            df_nan_and_zero.loc[i, 'room_num'] = round(df_nan_and_zero.loc[i, 'room_num'],0)

        df = pd.concat([df_non_nan_and_zero, df_nan_and_zero])
        df = pd.concat([df, room_num_non_nan_and_zero])


        df.index = range(df.shape[0])#避免后续索引不是从0开始的连续序列（因为前面的步骤也许会去掉一些行，所以行索引页会没有了）而错位了等问题的发生

    return df


# ===========================================================以下方法复杂并且考虑不周全

    # # 获取 target特征列'room_num'中不包含0值和null值的数据作来建立模型
    # df_room_num_zero = sub_df[sub_df['room_num'] == 0]  # room_num值为0时也需要填充
    # df_room_num_nan = sub_df[sub_df['room_num'].isnull().values == True]
    # if (df_room_num_zero.shape[0]>0) & (df_room_num_nan.shape[0]>0):
    #     df_nan = pd.concat([df_room_num_zero,df_room_num_nan])
    #     df = df[(df['room_num'] != 0) & (df['room_num'].isnull().values == False)]
    # elif (df_room_num_zero.shape[0]==0) & (df_room_num_nan.shape[0]>0):
    #     df_nan =df_room_num_nan
    #     df = df[df['room_num'].isnull().values==False]
    # elif (df_room_num_zero.shape[0]>0) & (df_room_num_nan.shape[0]==0):
    #     df_nan = df_room_num_zero
    #     df = df[df['room_num'] != 0]


    # # 只能拿area特征非空和非0的数据进行模型训练和预测
    # if (df[df['area'] == 0].shape[0] > 0) and (df[df['area'].isnull().values == True].shape[0] > 0):
    #     df = df[(df['area'] != 0) & (df['area'].isnull().values == False)]#这里用and会报错，用&就对了，为什么
    # elif (df[df['area'] == 0].shape[0] == 0) and (df[df['area'].isnull().values == True].shape[0] > 0):
    #     df = df[df['area'].isnull().values==False]
    # elif (df[df['area'] == 0].shape[0] > 0) and (df[df['area'].isnull().values == True].shape[0] == 0):
    #     df = df[df['area'] !=0]
    # else:
    #     pass
    #
    # print(list(df.isnull().any()[df.isnull().any()==True]))
    # sub_df = df[['area','room_num']]
    # df_zero = sub_df[sub_df['room_num'] == 0]  # room_num值为0时也需要填充
    # df_nan = sub_df[sub_df['room_num'].isnull() == True]
    #
    # df_columns = df.columns
    # big_sub_columns = set(df_columns.values) - set(('room_num', 'area'))
    # big_sub_df = df[list(big_sub_columns)]
    #
    #
    # if df_nan.shape[0] > 0:
    #
    #     if df_zero.shape[0]> 0:
    #         df_nan = pd.concat([df_nan, df_zero])
    #         sub_df = sub_df[sub_df['room_num'] != 0]
    #
    #
    #     df_non_nan = sub_df[sub_df['room_num'].isnull() == False]
    #     df_non_nan_raw = df_non_nan.copy()
    #
    #     df_non_nan_y = df_non_nan['room_num']
    #     del df_non_nan['room_num']
    #     df_non_nan_X = df_non_nan
    #
    #     train_size = int((df_non_nan_X.shape[0]) * cf.get('train_size_rate'))
    #
    #     X_train, X_test, y_train, y_test = train_test_split(df_non_nan_X, df_non_nan_y, train_size=train_size,
    #                                                         random_state=4)
    #
    #     reg = LinearRegression(normalize=True)
    #     reg.fit(X_train, y_train)
    #
    #     del df_nan['room_num']
    #     df_nan['room_num'] = reg.predict(df_nan)
    #
    #     df_nan = df_nan[df_non_nan_raw.columns.tolist()]  # 解决columns顺序对应问题
    #     sub_df = pd.concat([df_non_nan_raw, df_nan])
    #
    #     df = pd.concat([big_sub_df,sub_df],axis=1)
    #     df.index = range(df.shape[0])#避免后续索引不是从0开始的连续序列（因为前面的步骤也许会去掉一些行，所以行索引页会没有了）而错位了等问题的发生
    #
    # return df



