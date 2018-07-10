#!usr/bin/ python
#- * - coding:utf-8 - * -


from pyspark.sql.functions import udf

def newDataFacilities(df,model_path):
    from pyspark.ml.feature import CountVectorizerModel
    from random import choice

    loaded_count_vectorizer_model = CountVectorizerModel.load(model_path + 'count-vectorizer-model')

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

    df = newDataFacilities(df)
    print('new_data===============')
    df.show()
