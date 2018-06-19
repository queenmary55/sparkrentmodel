#!usr/bin/env python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: spark_template.py
@time: 2018/04/{DAY}
"""

import pandas as pd

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StringType, StructField, IntegerType,FloatType


def tranFacilitiesField(df):
    from sklearn.feature_extraction.text import CountVectorizer

    facilities = df.select('facilities').collect()

    temp_list = []
    for i in facilities:
        temp_list.append(i[0])

    lst = []
    for i in temp_list:
        i = str(i)
        if i == 'None':
            lst.append('facilities_null')
        else:
            temp = ' '.join(i.split('|'))
            if '独立卫生间' in temp:
                temp = temp.replace('独立卫生间', '独卫')
            if '独立阳台' in temp:
                temp = temp.replace('独立阳台', '阳台')
            lst.append(temp)
    vectorizer = CountVectorizer(token_pattern='(?u)\\b\\w+\\b')
    facilities = vectorizer.fit_transform(lst)
    tran_arr = facilities.toarray()
    cols = vectorizer.get_feature_names()
    tran_df = pd.DataFrame(tran_arr, columns=cols)

    return tran_df


def spark_tranFacilitiesField(df):
    tran_df = tranFacilitiesField(df)  # pandas 的df
    facilities_fields = list(tran_df.columns)

    for i in facilities_fields:
        facilities_fields_iterator = iter(list(tran_df[i]))

        def udf_facilities_fields(s):
            return float(next(facilities_fields_iterator))

        transf_udf = udf(udf_facilities_fields, StringType())
        df = df.select(
            '*', transf_udf(df['facilities']).alias(i))

    df = df.drop('facilities')

    return df


def tranFacilitiesFieldNewMethod(df):
    from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
    from pyspark.sql.types import ArrayType,StringType

    def udf_facilities(s):
        try:
            s = s.strip().strip('\t')
            if s == None:
                return '其他'
            elif (s != None) & (s != 'NULL'):
                lst = s.split('|')
                for i in range(len(lst)):
                    if lst[i] != '':
                        lst[i] = lst[i].strip().strip('\t')
                        if (lst[i] == '可') | (lst[i] == '饭') | (lst[i] == '做饭') | (lst[i] == '可*饭'):
                            lst[i] = '可做饭'
                        if lst[i] == '独立卫生间':
                            lst[i] = '独卫'
                        if lst[i] == '独立阳台':
                            lst[i] = '阳台'
                        if (lst[i] == '车位/车库') | (lst[i] == '车库'):
                            lst[i] = '车位'
                        if lst[i] == '煤气/天然气':
                            lst[i] = '煤气'
                        if lst[i] == '水':
                            lst[i] = '热水器'
                        if lst[i] == '露台/花园':
                            lst[i] = '露台'
                        if lst[i] == '电':
                            if '电视' not in lst:
                                lst[i] = '电视'
                            elif '电话' not in lst:
                                lst[i] = '电话'
                            elif '电梯' not in lst:
                                lst[i] = '电梯'
                            elif '家电' not in lst:
                                lst[i] = '家电'
                            else:
                                lst[i] = '电视'
                        if lst[i] == '有线电视':
                            lst[i] = '电视'
                        if lst[i] == '暂无资料':
                            lst[i] = '其他'
                if lst.count('') == 0:
                    pass
                elif lst.count('') == 1:
                    lst.remove('')
                elif lst.count('') > 1:
                    for j in range(lst.count('')):
                        lst.remove('')
                else:
                    pass

                return lst
            elif s == 'NULL':
                return '其他'
            else:
                return '其他'
        except Exception as e:
            return '其他'


    trans_udf = udf(udf_facilities,ArrayType(StringType()))

    df = df.na.fill('其他', 'facilities')
    df = df.filter(df['facilities'] != 'NULL')

    df = df.select('*',trans_udf(df['facilities']).alias('tmp_name'))
    df = df.drop('facilities')
    df = df.withColumnRenamed('tmp_name','facilities')

    print('encoding before===================')
    df.show(truncate=False)

    cv = CountVectorizer(inputCol='facilities', outputCol='facilities_vectors')
    model = cv.fit(df)
    df = model.transform(df)

    all_models_path = '/user/limeng/save_models/'
    countVectorizer_Path = all_models_path + "/count-vectorizer"
    countVectorizer_model_Path = all_models_path + "/count-vectorizer-model"
    cv.save(countVectorizer_Path)
    model.save(countVectorizer_model_Path)

    # loaded_count_vectorizer_model = CountVectorizerModel.load('/root/save_data_processed_models/count-vectorizer-model')
    # # loaded_count_vectorizer = CountVectorizer.load('/root/save_data_processed_models/count-vectorizer')
    # df = loaded_count_vectorizer_model.transform(df)
    # df = df.drop('facilities')
    # print('encoding after===================')
    # df.show(truncate=False)
    print('model.vocabulary====',model.vocabulary)

    return df


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os

    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[2]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('WARN')

    spark = SparkSession(sparkContext=sc)

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')
    # df.show(truncate=False)
    # df = spark_tranFacilitiesField(df)


    df = tranFacilitiesFieldNewMethod(df)
    df.show(truncate=False)


    spark.stop()
    sc.stop()