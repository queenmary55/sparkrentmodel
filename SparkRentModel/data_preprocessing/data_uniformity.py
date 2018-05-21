#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: data_uniformity.py
@time: 2018/04/{DAY}
"""


"""
    1、数据一致性处理，理论上每个特征的都要做一致性处理
    2、空值填充时有的特征已经做了一致性处理，因此在这里就不需要再处理此类特征
"""

from resource.configing import config as cf
from math_functions.math import Math
from pyspark.sql.functions import udf
from data_preprocessing.udf_methods import UDFMethods
from pyspark.sql.types import FloatType,StringType


class DataUniformity(object):
    def __init__(self):
        pass

    @staticmethod
    def direction(df):
        udf_transf = udf(UDFMethods.udf_direction,StringType())

        df = df.select(
            '*', udf_transf(df['direction']).alias('temp_name'))

        df = df.drop('direction')
        df = df.withColumnRenamed('temp_name', 'direction')
        return df



    @staticmethod
    def floorTotal(df):
        floor_total_mode = Math.mode(df,'floor_total')

        def udf_floor_total(s):
            s = float(s)
            if (s < 0) | (s > cf.get('uniformity_floor_total_max')):
                return float(floor_total_mode)
            else:
                return float(s)
        udf_transf = udf(udf_floor_total,FloatType())

        df = df.select(
            '*', udf_transf(df['floor_total']).alias('temp_name'))

        df = df.drop('floor_total')
        df = df.withColumnRenamed('temp_name', 'floor_total')

        return df



    @staticmethod
    def isBroker(df):
        is_broker_mode = Math.mode(df,'is_broker')

        def udf_is_broker(s):
            s = int(s)
            if s not in (0,1):
                return float(is_broker_mode)
            else:
                return float(s)
        udf_transf = udf(udf_is_broker,FloatType())

        df = df.select(
            '*', udf_transf(df['is_broker']).alias('temp_name'))

        df = df.drop('is_broker')
        df = df.withColumnRenamed('temp_name', 'is_broker')

        return df


    @staticmethod
    def rentType(df):

        udf_transf = udf(UDFMethods.udf_rentType,StringType())

        df = df.select(
            '*', udf_transf(df['rent_type']).alias('temp_name'))

        df = df.drop('rent_type')
        df = df.withColumnRenamed('temp_name', 'rent_type')

        return df


    # 目前ganji北京的这个特征全为Null
    @staticmethod
    def roomType(df):
        udf_transf = udf(UDFMethods.udf_room_type,StringType())

        df = df.select(
            '*', udf_transf(df['room_type']).alias('temp_name'))

        df = df.drop('room_type')
        df = df.withColumnRenamed('temp_name', 'room_type')

        return df


    @staticmethod
    def payType(df):
        udf_transf = udf(UDFMethods.udf_payType,StringType())

        df = df.select(
            '*', udf_transf(df['pay_type']).alias('temp_name'))

        df = df.drop('pay_type')

        df = df.withColumnRenamed('temp_name', 'pay_type')
        return df


    @staticmethod
    def agencyName(df):

        udf_transf = udf(UDFMethods.udf_agencyName,StringType())

        df = df.select(
            '*', udf_transf(df['agency_name']).alias('temp_name'))

        df = df.drop('agency_name')
        df = df.withColumnRenamed('temp_name', 'agency_name')

        return df

    @staticmethod
    def zone(df):

        udf_transf_zone = udf(UDFMethods.udf_zone,StringType())

        df = df.select(
            '*', udf_transf_zone(df['zone']).alias('temp_name'))
        df = df.drop('zone')
        df = df.withColumnRenamed('temp_name', 'zone')

        return df



def dataUniform(df):
    uniformity_fields = cf.get('uniformity_fields')
    cols = df.columns

    for column in uniformity_fields:

        if column in cols:

            df = df.filter(df[column].isNotNull())

            if column == 'direction':
                df = DataUniformity.direction(df)
            elif column == 'floor_total':
                df = DataUniformity.floorTotal(df)

            elif column == 'is_broker':
                df = DataUniformity.isBroker(df)
            elif column == 'rent_type':
                df = DataUniformity.rentType(df)
            elif column == 'room_type':
                df = DataUniformity.roomType(df)
            elif column == 'pay_type':
                df = DataUniformity.payType(df)
            elif column in ['price','score','house_count','area']:
                df = Math.XiGeMa(df, column, 3)
            elif column == 'agency_name':
                df = DataUniformity.agencyName(df)
            elif column == 'zone':
                df = DataUniformity.zone(df)
            else:
                print('the feature need not be processed')

        else:
            pass

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
    df = dataUniform(df)

    df.show(truncate=False)

    spark.stop()
    sc.stop()











