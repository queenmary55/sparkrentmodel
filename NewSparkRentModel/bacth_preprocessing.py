#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: bacth_preprocessing.py
@time: 2018/05/{DAY}
"""
import os
import pandas as pd
import numpy as np
import time

from RentModel.preprocessing_main import processingMain

rootdir = 'D:/ganji_beijing_data/'
list = os.listdir(rootdir)
for i in range(1,len(list)-2):

    start = time.time()

    if i <= 99:
        filename = 'ganji_beijing_' + str(i) + 'th' + '_5wan_train' + '.csv'
        path = os.path.join(rootdir,filename)
    else:
        filename = 'ganji_beijing_' + str(i) + 'th' + '_less_than_5wan_train' + '.csv'
        path = os.path.join(rootdir, filename)

    df = pd.read_csv(path, encoding='gbk')

    columns = df.columns.tolist()
    for j in range(len(columns)):
        columns[j] = columns[j].strip()
    df.columns = columns

    del df['bus']
    del df['Unnamed: 0']
    del df['id']

    df = processingMain(df)
    processed_filename = 'processed_ganji_beijing_' + str(i) + 'th' + '_5wan_train' + '.csv'
    save_path = os.path.join(rootdir, processed_filename)

    df.to_csv(save_path, encoding='gbk')

    end = time.time()

    run_time = round(end - start, 2)

    print('已经完成第%d个, 耗用时间%d秒'%(i,run_time))


