#!usr/bin/ python
#- * - coding:utf-8 - * -

from pyspark.ml.feature import OneHotEncoder, StringIndexer,StringIndexerModel
from pyspark.sql.functions import udf


"""
    1、one-hot编码字段的处理
    2、facilities字段的处理
    3、指定数值字段的处理
    4、.................
    5、read_parquet
    
"""

# 1 one-hot编码字段的处理
def newDataOneHot(df):
    temp_path = '/root/oneHotModels_61/'
    one_hot_cols = ['agency_name','direction','rent_type','district', 'pay_type','zone']
    from random import choice

    for i in one_hot_cols:
        df = df.filter(df[i].isNotNull())
        df = df.filter(df[i] != 'NULL')

        # stringIndexer_model中已经添加handleInvalid='skip'，因此会自动忽略编码中没有字符串
        # 因此,需要在已经有的字符串中随机选择一个来替换没有的这个新字符串
        loadStringIndexerModel = StringIndexerModel.load(temp_path + 'stringIndexer_model' + i)
        labels = loadStringIndexerModel.labels
        columns = df.columns

        def udf_no_exist(s):
            try:
                if s in labels:
                    return s
                else:
                    return choice(labels)
            except Exception:
                return choice(labels)

        tran_udf = udf(udf_no_exist)

        # df = df.withColumn('temp_name',df[i]).drop(i)
        # df = df.withColumnRenamed('temp_name',i)
        # df = df.rdd.map(lambda x: (x[0:-1], x[-1] if x[-1] in labels else '其他')).toDF(df.columns)

        df = df.select('*',tran_udf(df[i]).alias('tmp_name')).drop(i)
        df = df.withColumnRenamed('tmp_name',i)
        df = df.select(columns)

        indexed = loadStringIndexerModel.transform(df)
        loadOneHotEncoderModel = OneHotEncoder.load(temp_path + i + '_new')
        df = loadOneHotEncoderModel.transform(indexed)

        index_name = i + 'Index'
        df = df.drop(i)
        df = df.drop(index_name)

    return df

# 2 facilities字段的处理
def newDataFacilities(df):
    from pyspark.ml.feature import CountVectorizerModel
    from random import choice

    loaded_count_vectorizer_model = CountVectorizerModel.load('/root/save_data_processed_models/count-vectorizer-model')
    # 如果在loaded_count_vectorizer_model.vocabulary中没有的词语，loaded_count_vectorizer_model会自动忽略
    # 因此, 如果改行已经具有所有的词汇，则删掉这个新的词汇。否则需要在已经有的
    # 词汇中随机选择一个来替换没有的这个新字符串，并保证替换后不改行没有重复的词汇
    vocabularies = loaded_count_vectorizer_model.vocabulary

    from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
    from pyspark.sql.types import ArrayType, StringType

    def udf_facilities(s):
        try:
            s = s.strip().strip('\t')
            if s == None:
                return choice(vocabularies)
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
                            lst[i] = choice(vocabularies)

                if lst.count('') == 0:
                    pass
                elif lst.count('') == 1:
                    lst.remove('')
                elif lst.count('') > 1:
                    for j in range(lst.count('')):
                        lst.remove('')
                else:
                    pass

                # 词汇中随机选择一个来替换没有的这个新字符串，并保证替换后不改行没有重复的词汇
                lst = list(set(lst))
                if len(lst) > len(vocabularies):
                    lst = list(set(vocabularies) & set(lst))
                for g in range(len(lst)):
                    if lst[g] not in vocabularies:
                        random_vocabulary = choice(vocabularies)
                        if random_vocabulary not in lst:
                            lst[g] = random_vocabulary

                return lst
            elif s == 'NULL':
                return choice(vocabularies)
            else:
                return choice(vocabularies)
        except Exception as e:
            return choice(vocabularies)

    trans_udf = udf(udf_facilities, ArrayType(StringType()))

    df = df.na.fill(choice(vocabularies), 'facilities')
    df = df.filter(df['facilities'] != 'NULL')

    df = df.select('*', trans_udf(df['facilities']).alias('tmp_name'))
    df = df.drop('facilities')
    df = df.withColumnRenamed('tmp_name', 'facilities')

    df = df.filter(df['facilities'].isNotNull())
    df = loaded_count_vectorizer_model.transform(df)
    df = df.drop('facilities')

    return df


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os
    import time

    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')
    sc = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel('ERROR')
    spark = SparkSession(sparkContext=sc)

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')
    print('orign_data============')
    df.show()

    # df = newDataOneHot(df)
    df = newDataFacilities(df)
    print('new_data===============')
    df.show()