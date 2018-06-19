#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: test11.py
@time: 2018/05/{DAY}
"""
import pandas as pd
from sklearn.externals import joblib

df = pd.read_csv("D:/ganji_beijing_data/processed_ganji_beijing_2th_5wan_train.csv",encoding='gbk')
X_test = df.iloc[0:100,:]
del X_test['Unnamed: 0']
del X_test['one_area_price']
del X_test['crawl_time']
del X_test['独卫']
del X_test['one_room_area']
del X_test['price']

model_path1 = 'D:/ganji_beijing_data/regressionRF.model2'
regressor1 = joblib.load(model_path1)
y_pred1= regressor1.predict(X_test)

print('============',type(y_pred1),y_pred.shape)
print(y_pred1)