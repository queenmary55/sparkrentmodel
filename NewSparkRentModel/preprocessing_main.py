#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: all_preprocessing.py
@time: 2018/05/{DAY}
"""

from features_projects.one_hot_encoding import *
from features_projects.facilities_transform import tranFacilitiesFieldNewMethod
from data_preprocessing.null_fill import NullAndDuplications

from features_projects.data_uniformity import dataUniform
from features_projects.derive_features import *
from data_preprocessing.check_null_rate import checkNullRate
from data_preprocessing.outlier import outlierValue

"""
    processing_main函数中数据处理顺序如下：
    0、检查空值率，并将空置率大于70%的列去掉
    1、空值填充（不包括area)和重复数据删除
    2、facilities字段编码
    3、数据一致性处理
    4、oneHotEncoding编码拍
    5、除area外的异常值处理
    5、area特征空值填充
    6、派生变量的异常值处理   
"""


def processingMain(df):
    print('第1个流程：检查空值率，并将空置率大于70%的列去掉------before:',df.count(),df.columns)
    df = checkNullRate(df)
    print('第1个流程：检查空值率，并将空置率大于70%的列去掉------after:', df.count(), df.columns)

    nad = NullAndDuplications()
    df = nad.finalFillNull(df)
    print('第2个流程：完成空值填充========================')

    df = tranFacilitiesFieldNewMethod(df)
    print('第3个流程：完成facilities字段编码====================')

    df = dataUniform(df)
    print('第4个流程：完成数据一致性处理================')

    df = oneHotAll(df)
    print('第5个流程：完成one-hot编码====================')

    df = outlierValue(df)
    print('第6个流程：完成异常值处理=======================')

    df = derive(df)
    print('第7个流程：完成派生变量的生成及其异常值的处理==================')

    return df



if __name__ == '__main__':

   
    import time
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os
    from algorithms.spark_algorithms import rfRegressor
    from pyspark.ml.feature import VectorAssembler
    from pyspark.sql.types import FloatType
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel


    os.environ["PYSPARK_PYTHON"]="/home/hadoop/.pyenv/versions/anaconda3-4.2.0/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/home/hadoop/.pyenv/versions/anaconda3-4.2.0/bin/python"

    conf=SparkConf().setAppName("pyspark rentmodel_new").setMaster("yarn-client").set("spark.driver.memory", "4g").set("spark.executor.instances", "6").set("spark.executor.memory", "3g").set("spark.executor.cores", '6')
   # sparkConf = SparkConf() \
    #    .setAppName('pyspark rentmodel') \
        # .setMaster('local[*]')
    sc = SparkContext(conf=conf)

    sc.setLogLevel('ERROR')
#    spark = SparkSession(sparkContext=sc)
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start = time.time()

#    df = spark.read.csv('/user/limeng/ganji_beijing_pyspark.csv', header=True, encoding='utf-8')
#    df = spark.sql("select * from olap.new_ganji_beijing")
#    df = spark.read.table('olap.ganji_beijing_table')
    #df = spark.sql("select * from olap.new_ganji_beijing where district='大兴'")

    df = spark.sql("select * from dm.fangtianxia_beijing where district='大兴'")
    df = df.drop('bus')
    print('df.dtypes==========',df.dtypes) 
    print('========',df.count())
    df = df.drop('crawl_time')

    cols = df.columns
    for i in cols:
        if df.filter(df[i] == 'NULL').count() > 0:
            print('NULL_column====',df.filter(df[i] == 'NULL').count())

    df = processingMain(df)
	print('第8个流程：数据处理完毕并示数据====================')
    df.show()
	
    df.write.mode('overwrite').parquet('/user/limeng/data/fangtianxia_daxing.parquet')
	print('第9个流程：处理后数据存储（写入）完毕====================')
    #df.write.mode("overwrite").options(header="true").csv('/user/limeng/data/procesded_data/newallganji28')
	
    spark.stop()
    sc.stop()

    end = time.time()

    print('程序运行时间（分钟）：', round((end - start)/60, 2))
	
	
	
