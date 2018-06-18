#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@time: 2018/05/{DAY}
"""


import numpy as np
import pandas as pd

import lightgbm
from sklearn.metrics import mean_squared_error

from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor


def predictResultOutput(regressor, X_test, y_test, y_pred):
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
    if (isinstance(regressor,RandomForestRegressor) or
            isinstance(regressor, ExtraTreesRegressor) or
            isinstance(regressor, GradientBoostingRegressor) or
            isinstance(regressor, MLPRegressor)):
        criterion_dict = {
            'R_square:': round(regressor.score(X_test, y_test), 3),
            'error_coverage_rate20:': error_coverage_rate20,
            'error_coverage_rate10:': error_coverage_rate10,
            'error_coverage_rate5:': error_coverage_rate5,
            'error_coverage_rate2:': error_coverage_rate2,
            'error_coverage_rate1:': error_coverage_rate1
        }

        criterion_df = pd.Series(criterion_dict, name='criterion')
    else:
        criterion_dict = {
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

    return criterion_df, predict_result
