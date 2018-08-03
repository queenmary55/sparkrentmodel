#!usr/bin/ python
#- * - coding:utf-8 - * -

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,FloatType
from rent_price_ml.utils_apt.spark_rentmodel_interface.udf_methods import UDFMethods


def assingNumber(tmp_input_dict_data,key):
    try:
        if tmp_input_dict_data[key] == None:
            tmp_input_dict_data[key] = 0.0
    except Exception as e:
        tmp_input_dict_data[key] = 0.0

    # floor 值转换
    if key == 'floor':
        try:
            if tmp_input_dict_data[key] == '低':
                tmp_input_dict_data[key] = 1.0
            elif tmp_input_dict_data[key] == '中':
                tmp_input_dict_data[key] = 2.0
            elif tmp_input_dict_data[key] == '高':
                tmp_input_dict_data[key] = 3.0
            else:
                tmp_input_dict_data[key] = 0.0
        except Exception as e:
            tmp_input_dict_data[key] = 0.0

    # decoration 值转换
    if key == 'decoration':
        try:
            if tmp_input_dict_data[key] == '毛坯':
                tmp_input_dict_data[key] = 0.0
            elif tmp_input_dict_data[key] == '简':
                tmp_input_dict_data[key] = 2.0
            elif tmp_input_dict_data[key] == '精':
                tmp_input_dict_data[key] = 3.0
            elif tmp_input_dict_data[key] == '豪':
                tmp_input_dict_data[key] = 4.0
            elif tmp_input_dict_data[key] == '豪':
                tmp_input_dict_data[key] = 5.0
            else:
                tmp_input_dict_data[key] = 0.0
        except Exception as e:
            tmp_input_dict_data[key] = 0.0

    return tmp_input_dict_data


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