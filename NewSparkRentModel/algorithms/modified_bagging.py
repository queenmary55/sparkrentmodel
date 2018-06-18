#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: modified_bagging.py
@time: 2018/05/{DAY}
"""
"""
    1、由数据量较大，不能一次读入到内存进行计算，利用随机森林算法的特点对算法稍加改进和融合，
       就可以达到小批量N次读入数据N次独立训练的目的
    2、每读入一次数据训练一个RF，计算每个RF结果的平均值作为最终结果
    3、每个训练好的RF模型都单独保留
    4、下一次读入的数据要覆盖掉上一次的数据，以避免读入内存的数据量过大
    5、可以考虑RandomForestRegressor, ExtraTreesRegressor一起使用，目前先只考虑RandomForestRegressor
    6、目前考虑合并每个RF的testset作为最后整个bagging的测试集，而单独的一个RF不做test，但要有oob_score =True
       使用oob_score参数的值作为评估单个RF模型的性能指标。
"""


import pandas as pd
import numpy as np
import os
import time

from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor

from RentModel.data_preprocessing.null_data_split import trainDataSplit
from RentModel.algorithms.predict_result_output import predictResultOutput
from sklearn.externals import joblib



rootdir = 'D:/ganji_beijing_data/'
list = os.listdir(rootdir)

df_test_X = None
df_test_y = None

model_dict = {}

for i in [2,3,4,5,6]:

    start = time.time()

    filename = 'processed_ganji_beijing_' + str(i) + 'th' + '_5wan_train' + '.csv'
    path = os.path.join(rootdir,filename)
    df = pd.read_csv(path, encoding='gbk')
    del df['Unnamed: 0']
    del df['one_area_price']
    del df['crawl_time']
    del df['独卫']
    del df['one_room_area']

    X_train, X_test, y_train, y_test = trainDataSplit(df)

    df_test_X = pd.concat([df_test_X,X_test])
    df_test_y = pd.concat([df_test_y, y_test])

    regressor = RandomForestRegressor(n_estimators=80, criterion='mse', n_jobs = -1,oob_score=True)
    regressor.fit(X_train, y_train)

    key = 'regressor' + str(i)
    value = regressor
    model_dict.setdefault(key, value) #or model_dict[key] = value

    # y_pred = regressor.predict(X_test)

    importance = pd.DataFrame(regressor.feature_importances_, index=X_train.columns)
    importance = importance.sort_values(importance.columns[0],ascending=False)

    importance_path = 'D:/ganji_beijing_data/' + str(i) + 'importance_df' + '.csv'
    importance.to_csv(importance_path, encoding='gbk')

    # criterion_df, predict_result = predictResultOutput(regressor, X_test, y_test, y_pred)

    save_model_path = 'D:/ganji_beijing_data/regressionRF.model' + str(i)
    joblib.dump(regressor,save_model_path)

    end = time.time()

    run_time = round(end - start, 2)

    print('完成第%d个模型，耗时：%d秒'%(i,run_time))

df_test_X.to_csv('D:/ganji_beijing_data/df_test_X.csv', encoding='gbk')
df_test_y.to_csv('D:/ganji_beijing_data/df_test_y.csv', encoding='gbk')

df_test_X.drop(df_test_X[df_test_X.isnull()].index)

y_pred_list = []
for j in model_dict.keys():
    regressor = model_dict.get(j)
    y_pred = regressor.predict(df_test_X)
    y_pred_list.append(y_pred)

arr = np.array(y_pred_list)

y_pred_df = pd.DataFrame(arr.T,columns= model_dict.keys())

average = pd.DataFrame(y_pred_df.mean(axis = 1),columns='result')

result_df = pd.concat([y_pred_df,average])


criterion_df, predict_result = predictResultOutput(df_test_y, average)

criterion_df.to_csv('D:/ganji_beijing_data/modified_result_criterion_df.csv', encoding='gbk')
result_df.to_csv('D:/ganji_beijing_data/modified_result.csv', encoding = 'gbk')


def predictResultOutput(y_test, y_pred):
    # 0、误差率
    num1 = 0
    num2 = 0
    num5 = 0
    num10 = 0
    num20 = 0

    result_tuple = zip(y_test.values, y_pred)
    for i in result_tuple:
        error_rate = abs((i[0] - i[1]) / i[0])
        if error_rate <= 0.01:
            num1 += 1
        elif error_rate <= 0.02:
            num2 += 1
        elif error_rate <= 0.05:
            num5 += 1
        elif error_rate <= 0.1:
            num10 += 1
        elif error_rate <= 0.2:
            num20 += 1
        else:
            pass

    # 1、误差小于等于20%的样本覆盖率
    error_coverage_rate20 = round((num20 + num10 + num5 + num2 + num1) / y_test.shape[0], 4)
    # 2、误差小于等于10%的样本覆盖率
    error_coverage_rate10 = round((num10 + num5 + num2 + num1) / y_test.shape[0], 4)
    # 3、误差小于等于5%的样本覆盖率
    error_coverage_rate5 = round((num5 + num2 + num1) / y_test.shape[0], 4)
    # 4、误差小于等于2%的样本覆盖率
    error_coverage_rate2 = round((num2 + num1) / y_test.shape[0], 4)
    # 5、误差小于等于1%的样本覆盖率
    error_coverage_rate1 = round(num1 / y_test.shape[0], 4)


    # 6、保存指标

    criterion_dict = {
        'R_square:': round(regressor.oob_score_, 3),
        'error_coverage_rate20:': error_coverage_rate20,
        'error_coverage_rate10:': error_coverage_rate10,
        'error_coverage_rate5:': error_coverage_rate5,
        'error_coverage_rate2:': error_coverage_rate2,
        'error_coverage_rate1:': error_coverage_rate1
    }

    criterion_df = pd.Series(criterion_dict, name='criterion')


    # 7、预测结果
    arr = np.array([y_test, y_pred])
    predict_result = pd.DataFrame(arr.T, columns=['y_test', 'y_pred'])

    print('criterion_df=========\n',criterion_df)

    return criterion_df, predict_result