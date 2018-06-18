#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: createmodel_before_main.py
@time: 2018/04/{DAY}
"""


# import pandas as pd
#
# preprocessing_main import processingMain
#
# algorithms.algorithms_collection import Algorithms
# algorithms.stacking import regressionStacking

import pandas as pd
from algorithms.spark_algorithms import rfRegressor
import preprocessing_main

"""
    main函数中数据处理顺序如下：
    1、执行数据预处理主函数
    2、调用算法进行模型训练
"""


def read(path_dir):

    files_rdd = sc.wholeTextFiles(path_dir)
    files_list = list(files_rdd.map(lambda file: [line for line in file[1].split('\n')]).collect())
    tmp = []
    col_names = files_list[1][0].split(',')
    tmp.append(col_names)

    for file in files_list:
        for line in file[1:]:
            if len(line) !=0:
                line = line.split(',')
                tmp.append([float(line[i]) if i != 0 else line[i] for i in range(len(line))])
            else:
                pass

    df = pd.DataFrame(tmp[1:],columns=tmp[0])
    sdf = spark.createDataFrame(df)
    return sdf


def main():
    pass
    # # df = processingMain(df)
    #
    # algorithm = Algorithms(df)
    #
    # # # 1、使用随机森林
    # criterion_df, predict_result = algorithm.regressionRF()
    #
    # # # 2、使用深度神经网络
    # # criterion_df, predict_result = algorithm.regressorDNN()
    # #
    # # # 3、使用lightgbm
    # # criterion_df, predict_result = algorithm.lightGBM()
    #
    # # # 4、使用Stacking, 预测结果有问题
    # # criterion_df, predict_result = regressionStacking(df)
    #
    #
    #
    # print('criterion_df===============================:::\n',criterion_df)
    #
    # return criterion_df, predict_result

if __name__ == '__main__':

    import time
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler


    # os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('WARN')
    spark = SparkSession(sparkContext=sc)

    start = time.time()

    # path_dir = '/root/ganji_beijing_pyspark.csv'
    # sdf = read(path_dir)

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')

    df = preprocessing_main(df)
    df = df.drop('id')

    columns = df.columns
    columns.remove('price')

    vecAssembler = VectorAssembler(inputCols=columns, outputCol="features")

    df = vecAssembler.transform(df)
    print('============************--------------=======================')
    df.show(truncate=False)
    df = df.withColumnRenamed('price','label')


    importance_map_df = rfRegressor(df)
    print('importance_map_df===', importance_map_df)


    end = time.time()

    print('程序运行时间（秒）：',round(end-start,2))

