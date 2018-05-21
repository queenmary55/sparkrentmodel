#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: createmodel_before_main.py
@time: 2018/04/{DAY}
"""


import pandas as pd

from RentModel.preprocessing_main import processingMain

from RentModel.algorithms.algorithms_collection import Algorithms
from RentModel.algorithms.stacking import regressionStacking



import time

"""
    main函数中数据处理顺序如下：
    1、执行数据预处理主函数
    2、调用算法进行模型训练
"""


def main(df):

    # df = processingMain(df)

    algorithm = Algorithms(df)

    # # 1、使用随机森林
    criterion_df, predict_result = algorithm.regressionRF()

    # # 2、使用深度神经网络
    # criterion_df, predict_result = algorithm.regressorDNN()
    #
    # # 3、使用lightgbm
    # criterion_df, predict_result = algorithm.lightGBM()

    # # 4、使用Stacking, 预测结果有问题
    # criterion_df, predict_result = regressionStacking(df)



    print('criterion_df===============================:::\n',criterion_df)

    return criterion_df, predict_result

if __name__ == '__main__':

    start = time.time()

    # # 无district
    # df = pd.read_csv("D:/pyCharmSpace/non_district_4000000_4050000.csv", encoding='gbk')
    # del df['one_area_price']
    # criterion_df, predict_result = main(df)
    # criterion_df.to_csv('D:/pyCharmSpace/non_district_criterion_df.csv', encoding='gbk')
    # predict_result.to_csv('D:/pyCharmSpace/non_district_predict_result.csv', encoding='gbk')
    #
    # # 有district
    # df = pd.read_csv("D:/pyCharmSpace/district_4000000_4050000.csv", encoding='gbk')
    # del df['one_area_price']
    # criterion_df, predict_result = main(df)
    # criterion_df.to_csv('D:/pyCharmSpace/district_criterion_df.csv', encoding='gbk')
    # predict_result.to_csv('D:/pyCharmSpace/district_predict_result.csv', encoding='gbk')
    #
    # # 有zone, district
    # df = pd.read_csv("D:/pyCharmSpace/zone_district_4000000_4050000.csv", encoding='gbk')
    # del df['one_area_price']
    # criterion_df, predict_result = main(df)
    # criterion_df.to_csv('D:/pyCharmSpace/zone_district_criterion_df.csv', encoding='gbk')
    # predict_result.to_csv('D:/pyCharmSpace/zone_district_predict_result.csv', encoding='gbk')

    # 有zone, district
    df = pd.read_csv("D:/pyCharmSpace/ganji_beijing25wan_subset/processed_ganji_beijing_1th_25wan_train.csv", encoding='gbk')

    del df['Unnamed: 0']
    del df['one_area_price']
    del df['crawl_time']
    del df['独卫']
    del df['one_room_area']
    criterion_df, predict_result = main(df)
    criterion_df.to_csv('D:/pyCharmSpace/agency_name_zone_district_criterion_df.csv', encoding='gbk')
    predict_result.to_csv('D:/pyCharmSpace/agency_name_zone_district_predict_result.csv', encoding='gbk')



    end = time.time()

    print('程序运行时间（秒）：',round(end-start,2))