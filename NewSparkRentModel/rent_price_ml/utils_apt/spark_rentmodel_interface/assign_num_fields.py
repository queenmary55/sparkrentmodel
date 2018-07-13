#!usr/bin/ python
#- * - coding:utf-8 - * -

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,FloatType
from rent_price_ml.utils_apt.spark_rentmodel_interface.udf_methods import UDFMethods


def assingNumber(df):

    for i in ['floor','decoration']:
        # 值转换
        udf_assignZero = udf(UDFMethods.udf_NULL_assignZero, FloatType())

        df_nan = df.filter(df[i].isNull())
        df_null = df.filter(df[i] == 'NULL')
        if df_nan.count() > 0:
            df = df.na.fill(0, i)  # 为什么填充不了，仍然是空，但不报错？？？？？
        if df_null.count() > 0:
            df = df.select(
                '*', udf_assignZero(df[i]).alias('temp_name'))
            df = df.drop(i)
            df = df.withColumnRenamed('temp_name', i)

        # floor 值转换
        if i == 'floor':

            udf_floor_assingNumber = udf(UDFMethods.udf_floor,FloatType())

            df = df.select(
                '*', udf_floor_assingNumber(df[i]).alias('temp_name'))
            df = df.drop(i)
            df = df.withColumnRenamed('temp_name', i)

        # decoration 值转换
        elif i == 'decoration':
            udf_decoration_assingNumber = udf(UDFMethods.udf_decoration,FloatType())

            df = df.select(
                '*', udf_decoration_assingNumber(df[i]).alias('temp_name'))
            df = df.drop(i)
            df = df.withColumnRenamed('temp_name', i)

        else:
            pass

    return df


if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
    import os

    import sys
    print(sys.path)

    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[2]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('WARN')

    spark = SparkSession(sparkContext=sc)

    df = spark.read.csv('/root/ganji_beijing_pyspark.csv', header=True, encoding='gbk')

    df = assingNumber(df)
    df.show(truncate=False)

    spark.stop()
    sc.stop()