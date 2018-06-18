#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: stacking.py
@time: 2018/05/{DAY}
"""



import numpy as np
import pandas as pd
from sklearn.externals import joblib

from RentModel.data_preprocessing.null_data_split import trainDataSplit
from RentModel.algorithms.predict_result_output import predictResultOutput

from mlxtend.regressor import StackingRegressor

import lightgbm
from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.linear_model import LinearRegression, Lasso, Ridge

def regressionStacking(df):

    # StackingRegressor inputdata type is ndarray

    X_train, X_test, y_train, y_test = trainDataSplit(df)

    randomforest_regressor = RandomForestRegressor()

    # # lightgbm不是scikit-learn的包，mlxtend不支持
    # lgb_train = lightgbm.Dataset(X_train, y_train)
    # lgb_eval = lightgbm.Dataset(X_test, y_test, reference=lgb_train)
    #
    # # specify your configurations as a dict
    # params = {
    #     'task': 'train',
    #     'boosting_type': 'gbdt',
    #     'objective': 'regression',
    #     'metric': {'l2', 'auc'},
    #     'num_leaves': 2 ** 10,
    #     'learning_rate': 1.0,
    #     'feature_fraction': 0.9,
    #     'bagging_fraction': 0.8,
    #     'bagging_freq': 5,
    #     'verbose': 0
    # }
    # lightgbm_regressor = lightgbm.train(params,
    #                            lgb_train,
    #                            num_boost_round=20,
    #                            valid_sets=lgb_eval,
    #                            early_stopping_rounds=5)

    lasso_regressor = Lasso()

    dnn_regressor = MLPRegressor()

    linearRegression_regressor = LinearRegression()

    stacking_regressor = StackingRegressor(regressors=[
        randomforest_regressor, lasso_regressor, dnn_regressor],
        meta_regressor=linearRegression_regressor)

    stacking_regressor.fit(X_train, X_train)

    y_pred = stacking_regressor.predict(X_test)

    criterion_df, predict_result = predictResultOutput(stacking_regressor, X_test, y_test, y_pred)

    # save model
    joblib.dump(stacking_regressor,'stacking.model')

    return criterion_df, predict_result


