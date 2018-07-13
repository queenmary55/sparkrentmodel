#!usr/bin/ python
#- * - coding:utf-8 - * -


from pyspark.ml.feature import OneHotEncoder, StringIndexer,StringIndexerModel
from pyspark.sql.functions import udf

def newDataOneHot(df,onehot_model_path,col):

    from random import choice

    loadStringIndexerModel = StringIndexerModel.load(onehot_model_path + 'stringIndexer_model' + col)
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

    df = df.select('*',tran_udf(df[col]).alias('tmp_name')).drop(col)
    df = df.withColumnRenamed('tmp_name',col)
    df = df.select(columns)

    indexed = loadStringIndexerModel.transform(df)
    loadOneHotEncoderModel = OneHotEncoder.load(onehot_model_path + col + '_new')
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

    df = newDataOneHot(df)

    df.show()