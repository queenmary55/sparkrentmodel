#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: txt_to-df.py
@time: 2018/04/{DAY}
"""
import pandas as pd
from RentModel.math_functions.math import Math
from RentModel.feature_project.derive_features import DeriveFeature

import random

def readTxtTransf(txt_file_path):
    with open(txt_file_path,'r',encoding='utf-8') as f:
        lines = f.readlines()
        lst = []
        for i in lines:
            temp = i.strip('\n').split('\t')
            if len(temp) == 21:
                lst.append(temp)
            else:
                pass
    return lst


def addColumns(df):
    a = '''id, crawl_time, district, zone, rent_type, room_type, price, area, room_num, hall_num, toilet_num, floor, floor_total, direction, decoration,facilities,
    pay_type, is_broker, agency_name, subway, bus'''
    a = a.split(',')
    df.columns = a
    columns = df.columns.tolist()
    for i in range(len(columns)):
        columns[i] = columns[i].strip()
    df.columns = columns
    return df

def saveSubDataFrame(txt_file_path):
    lst = readTxtTransf(txt_file_path)
    lenght = int(len(lst))

    # shuffle10次
    for i in range(10):
        random.shuffle(lst)

    # 取50万条作为test数据
    ganji_beijing_50wan_test = lst[-500000:]
    pd.DataFrame(ganji_beijing_50wan_test).to_csv('D:/ganji_beijing_data/ganji_beijing_50wan_test.csv',encoding='gbk')

    # 剩下的数据作为train数据，并且每5万条存为一个文件
    train_length = int((lenght-500000)/50000)
    for i in range(train_length+1):
        left = i * 250000
        right = left + 250000
        if i < train_length:
            df = pd.DataFrame(lst[left:right])
            df = addColumns(df)
            path = 'D:/ganji_beijing_data/ganji_beijing25wan_subset/ganji_beijing_' + str(i + 1) + 'th' + '_25wan_train' + '.csv'
            df.to_csv(path, encoding='gbk')
        else:
            df = pd.DataFrame(lst[left:-500000])
            df = addColumns(df)
            path = 'D:/ganji_beijing_data/ganji_beijing25wan_subset/ganji_beijing_' + str(i + 1) + 'th' + '_less_than_25wan_train' + '.csv'
            df.to_csv(path, encoding='gbk')
    return i


if __name__ == '__main__':

    import time

    start = time.time()

    txt_file_path = 'D:/ganji_beijing_data/ganji_beijing_origin_data.csv'

    # saveSubDataFrame(txt_file_path)

    lst = readTxtTransf(txt_file_path)
    addColumns(pd.DataFrame(lst)).to_csv('D:/ganji_beijing_data/txi_to_df_ganji_beijing_origin_data.csv',encoding='gbk')

    end = time.time()

    print('程序运行时间（秒）：', round(end - start, 2))








# df = pd.read_csv("D:/pyCharmSpace/zhenshi_data_processed4000000_4050000.csv",encoding='gbk')

# df = Math.XiGeMa(df, 'one_area_price', 3)
# remove_outlier = df[df['one_area_price'] >= 30]
# df_non_outlier = df.loc[remove_outlier.index]
# df = df_non_outlier
#
# df = Math.XiGeMa(df,'one_room_area', 3)
# remove_outlier = df[df['one_room_area'] >= 10]
# df_non_outlier = df.loc[remove_outlier.index]
# df = df_non_outlier
#
# df.to_csv("D:/pyCharmSpace/zhenshi_data_processed4000000_405000022.csv",encoding='gbk')



