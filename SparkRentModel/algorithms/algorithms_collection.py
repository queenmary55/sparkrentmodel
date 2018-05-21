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

from sklearn.externals import joblib

from sklearn.model_selection import GridSearchCV

from sklearn.ensemble import RandomForestRegressor, ExtraTreesRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor

from RentModel.data_preprocessing.null_data_split import trainDataSplit
from RentModel.algorithms.predict_result_output import predictResultOutput


class Algorithms(object):
    def __init__(self, df):
        X_train, X_test, y_train, y_test = trainDataSplit(df)
        self.X_train = X_train
        self.X_test = X_test
        self.y_train = y_train
        self.y_test = y_test


    def regressionRF(self):
        # # param1 = {'n_estimators': [20, 40, 60, 80, 100, 220, 240, 260, 280, 300, 340, 380, 450, 500]}
        # param1 = {'n_estimators': [20, 40, 60, 80, 100]}
        # regressor = GridSearchCV(estimator=RandomForestRegressor(criterion='mse'),
        #                         param_grid=param1, n_jobs=3, cv=5)
        # regressor.fit(self.X_train, self.y_train)
        # print(regressor.scorer_ , regressor.best_params_, regressor.best_score_)

        regressor = RandomForestRegressor(n_estimators=80, criterion='mse', n_jobs = -1,oob_score=True)
        regressor.fit(self.X_train, self.y_train)

        y_pred = regressor.predict(self.X_test)

        importance = pd.DataFrame(regressor.feature_importances_, index=self.X_train.columns)
        importance = importance.sort_values(list(importance.columns)[0],ascending=False)
        importance.to_csv('D:/pyCharmSpace/25importance_df.csv', encoding='gbk')

        criterion_df, predict_result = predictResultOutput(regressor, self.X_test, self.y_test, y_pred)


        # save model
        joblib.dump(regressor,'D:/pyCharmSpace/regressionGBDT.model')

        return criterion_df, predict_result


    def regressionGBDT(self):
        regressor = GradientBoostingRegressor(loss='ls', learning_rate=0.001, n_estimators=200, subsample=1.0,
            criterion='friedman_mse', min_samples_split=2, min_samples_leaf=1,min_weight_fraction_leaf=0.0,
            max_depth=3, min_impurity_decrease=0.0, min_impurity_split=None, init=None, random_state=None,
            max_features=None, alpha=0.9)

        regressor.fit(self.X_train, self.y_train)

        y_pred = regressor.predict(self.X_test)
        criterion_df, predict_result = predictResultOutput(regressor, self.X_test, self.y_test, y_pred)

        # save model
        joblib.dump(regressor, 'D:/pyCharmSpace/regressionGBDT.model')

        return criterion_df, predict_result


    def regressorDNN(self):
        # solver='sgd' 老是报错测试数据有NaN值输入，实际上没有，估计此算法中间计算过程产生了NaN
        regressor = MLPRegressor(hidden_layer_sizes=(1600,1600,1600,100,100,100),
                                 learning_rate_init=0.0005, activation='relu', solver='adam',
                                 alpha=0.0001, batch_size=200)
        regressor.fit(self.X_train, self.y_train)

        y_pred = regressor.predict(self.X_test)
        criterion_df, predict_result = predictResultOutput(regressor, self.X_test, self.y_test, y_pred)

        print('regressor.n_iter_:', regressor.n_iter_)
        print('regressor.n_layers_:',regressor.n_layers_)

        # save model
        joblib.dump(regressor, 'regressorDNN.model')

        return criterion_df, predict_result

    def lightGBM(self):
        # create dataset for lightgbm
        lgb_train = lightgbm.Dataset(self.X_train, self.y_train)
        lgb_eval = lightgbm.Dataset(self.X_test, self.y_test, reference=lgb_train)

        # specify your configurations as a dict
        params = {
            'task': 'train',
            'boosting_type': 'gbdt',
            'objective': 'regression',
            'metric': {'l2', 'auc'},
            'num_leaves': 2**15,
            'learning_rate': 1.0,
            'feature_fraction': 0.9,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': 0
        }

        # train
        regressor = lightgbm.train(params,
                        lgb_train,
                        num_boost_round=20,
                        valid_sets=lgb_eval,
                        early_stopping_rounds=5)

        # save model to file

        regressor.save_model('model.txt')

        # predict
        y_pred = regressor.predict(self.X_test, num_iteration=regressor.best_iteration)

        # eval
        # print('The rmse of prediction is:', mean_squared_error(self.y_test, y_pred) ** 0.5)

        criterion_df, predict_result = predictResultOutput(regressor, self.X_test, self.y_test, y_pred)

        # save model
        joblib.dump(regressor, 'lightGBM.model')

        return criterion_df, predict_result



