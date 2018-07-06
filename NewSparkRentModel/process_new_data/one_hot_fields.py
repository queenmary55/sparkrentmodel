#!usr/bin/ python
#- * - coding:utf-8 - * -


from pyspark.ml.feature import OneHotEncoder, StringIndexer,StringIndexerModel
from pyspark.sql.functions import udf

def newDataOneHot(df,col):
    temp_path = '/root/save_data_processed_models/'

    from random import choice

    loadStringIndexerModel = StringIndexerModel.load(temp_path + 'stringIndexer_model' + col)
    labels = loadStringIndexerModel.labels
    print('labels=========',col, labels)
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

    df = df.select('*',tran_udf(df[col]).alias('tmp_name')).drop(col)
    df = df.withColumnRenamed('tmp_name',col)
    df = df.select(columns)

    indexed = loadStringIndexerModel.transform(df)
    print('----------------=======================-----------',temp_path + 'onehot_encoder' + col)
    indexed.show()
    loadOneHotEncoderModel = OneHotEncoder.load(temp_path + 'onehot_encoder' + col)
    print('loadOneHotEncoderModel.getInputCol()()----------',loadOneHotEncoderModel.getParam('inputCol'))
    df = loadOneHotEncoderModel.transform(indexed)

    index_name = col + 'Index'
    df = df.drop(col)
    df = df.drop(index_name)

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

    df = newDataOneHot(df)
    print('new_data===============')
    df.show()