#!usr/bin/ python
# - * - coding:utf-8 - * -

"""
@author: limeng
@file: null_and_duplication.py
@time: 2018/04/{DAY}
"""



from math_functions.math import Math
from features_projects.date_to_day import *
from resource.configing import config as cf

from pyspark.sql.functions import udf
from data_preprocessing.udf_methods import UDFMethods

from pyspark.sql.types import StringType,FloatType



class NullAndDuplications(object):
    def __init__(self):
        pass

    def drop_duplication(self, df):
        df = df.dropDuplicates()

        # 删除值全为空的行
        df = df.dropna(how='all')

        return df

    def dateDayNull(self,df):
        df = dateToDayTansform(df)
        return df

    def howFill(self, df, colums):
        if colums in cf.get('null_processing_delete'):
            df = FillMethods.delete(df, colums)

        elif colums in cf.get('null_processing_assignMean'):
            df = FillMethods.assignMean(df, colums)

        elif colums == cf.get('null_processing_property_fee'):
            df = FillMethods.property_fee(df,colums)

        elif (colums in cf.get('null_processing_assingNumber')) or (colums == cf.get('null_processing_assingNumber_complete_time')):
            df = FillMethods.assingNumber(df, colums)

        elif colums in cf.get('null_processing_assignZero'):
            df = FillMethods.assignZero(df, colums)

        elif colums in cf.get('null_processing_assignMode'):
            df = FillMethods.assignMode(df, colums)
        elif colums == 'crawl_time':
            df = self.dateDayNull(df)

        else:
            pass
        return df

    def fillNull(self,df):

        null_columns = []
        for i in df.columns:
            df_nan = df.filter(df[i].isNull())
            df_null = df.filter(df[i] == 'NULL')
            df_nan_null_len = df_nan.count() + df_null.count()
            if df_nan_null_len > 0:
                null_columns.append(i)

        if len(null_columns) == 0:
            pass

        elif len(null_columns) == 1:
            df = self.howFill(df,null_columns)

        elif len(null_columns) >= 2:
            for i in null_columns:
                df = self.howFill(df, i)
        else:
            pass

        return df

    def finalFillNull(self, df):
        df = self.drop_duplication(df)
        df = self.fillNull(df)

        return df


class FillMethods(object):
    def __init__(self):
        pass

    @staticmethod
    def delete(df, null_processing_delete):
        df_nan = df.filter(df[null_processing_delete].isNull())
        df_null = df.filter(df[null_processing_delete] == 'NULL')
        if df_nan.count() > 0:
            df = df.filter(df[null_processing_delete].isNotNull())
        if df_null.count() > 0:
            df = df.filter(df[null_processing_delete] != 'NULL')
        return df

    @staticmethod
    def assignMean(df, null_processing_assignMean):
        df_non_null = df.filter(df[null_processing_assignMean].isNotNull())
        avg = (df_non_null.groupBy().mean(null_processing_assignMean)).collect()
        df = df.na.fill(avg[0]['avg(price)'],null_processing_assignMean)

        return df

    @staticmethod
    def assignZero(df, null_processing_assignZero):

        udf_assignZero = udf(UDFMethods.udf_NULL_assignZero, StringType())

        df_nan = df.filter(df[null_processing_assignZero].isNull())
        df_null = df.filter(df[null_processing_assignZero] == 'NULL')
        if df_nan.count() > 0:
            df = df.na.fill(0, null_processing_assignZero) # 为什么填充不了，仍然是空，但不报错？？？？？
        if df_null.count() > 0:
            df = df.select(
                '*',udf_assignZero(df[null_processing_assignZero]).alias('temp_name'))
            df = df.drop(null_processing_assignZero)
            df = df.withColumnRenamed('temp_name',null_processing_assignZero)
        return df


    @staticmethod
    def assignMode(df, null_processing_assignMode):

        mode_num = Math.mode(df, null_processing_assignMode)

        df_nan = df.filter(df[null_processing_assignMode].isNull())
        df_null = df.filter(df[null_processing_assignMode] == 'NULL')

        def udf_fill_Null(s):
            # s = s.strip().strip('\n').strip('\t')
            if s == 'NULL':
                return mode_num
            else:
                return s
        transf_udf = udf(udf_fill_Null,StringType())

        if df_nan.count() > 0:
            df = df.na.fill(mode_num, null_processing_assignMode)

        if df_null.count() > 0:
            df = df.select(
                '*', transf_udf(df[null_processing_assignMode]).alias('temp_name'))
            df = df.drop(null_processing_assignMode)
            df = df.withColumnRenamed('temp_name', null_processing_assignMode)

        return df


    # 现在没有小区主数据，所以没有这个feature,暂时不用管
    # @staticmethod
    # def property_fee(df,null_processing_property_fee):
    #     temp_nan = df[df[null_processing_property_fee].isnull() == True]
    #
    #     if temp_nan.shape[0] > 0:
    #         df = df.drop(temp_nan.index)
    #         df.index = range(df.shape[0])
    #
    #     length = df[null_processing_property_fee].shape[0]
    #
    #     for i in range(length):
    #         try:
    #             if df[null_processing_property_fee].values[i].strip() == str(0):
    #                 pass
    #
    #             elif '至' in str(df[null_processing_property_fee].values[i]):
    #                 temp = df[null_processing_property_fee][i].split('至')
    #                 average = round(np.mean([float(j) for j in temp]), 2)
    #                 df[null_processing_property_fee].values[i] = average
    #             elif isinstance(float(df[null_processing_property_fee].values[i]),float):#有可能出现非数字字符串，故需要异常处理一下
    #                 df[null_processing_property_fee].values[i] = float(df[null_processing_property_fee].values[i])
    #
    #             else:
    #                 df[null_processing_property_fee].values[i] = 0
    #         except Exception as e:
    #             print(e)
    #             break
    #
    #
    #     property_fee_mean = round(np.mean(df[null_processing_property_fee]), 2)
    #
    #     if temp_nan.shape[0] > 0:
    #         temp_nan[null_processing_property_fee] = temp_nan[null_processing_property_fee].fillna(property_fee_mean)
    #         df = pd.concat([df, temp_nan], ignore_index=True)
    #         return df
    #     else:
    #         return df


    @staticmethod
    def assingNumber(df, null_processing_assingNumber):
        # 年份与权值的映射
        year_map_num_dict = {}
        for v, k in enumerate(range(cf.get('year_map_num_start'), cf.get('year_map_num_end') + 1)):
            year_map_num_dict[k] = v

        # 空值的填充
        udf_assignZero = udf(UDFMethods.udf_NULL_assignZero, FloatType())

        df_nan = df.filter(df[null_processing_assingNumber].isNull())
        df_null = df.filter(df[null_processing_assingNumber] == 'NULL')
        if df_nan.count() > 0:
            df = df.na.fill(0, null_processing_assingNumber)  # 为什么填充不了，仍然是空，但不报错？？？？？
        if df_null.count() > 0:
            df = df.select(
                '*', udf_assignZero(df[null_processing_assingNumber]).alias('temp_name'))
            df = df.drop(null_processing_assingNumber)
            df = df.withColumnRenamed('temp_name', null_processing_assingNumber)


        # tuple_df = nullDataSplit(df, null_processing_assingNumber)
        #
        # if len(tuple_df) == 2:
        #     df, length = tuple_df
        # else:
        #     df, temp_nan, length = tuple_df
        #     temp_nan.loc[:, null_processing_assingNumber] = temp_nan.loc[:, null_processing_assingNumber].fillna(cf.get('fill_fillna_value'))

        # complete_time 把年份映射成相应的权值
        # 暂时没有小区主数据，所以目前可以不考虑这个特征
        # if null_processing_assingNumber == cf.get('null_processing_assingNumber_complete_time'):
        #     for i in range(length):
        #         if (int(df[null_processing_assingNumber].values[i]) < 1950) or (int(df[null_processing_assingNumber].values[i]) > 2019):
        #             df[null_processing_assingNumber].values[i] = 0
        #         else:
        #             df[null_processing_assingNumber].values[i] = year_map_num_dict[int(df[null_processing_assingNumber][i])]

        # 值转换
        if null_processing_assingNumber in cf.get('null_processing_assingNumber'):

            # floor 值转换
            if null_processing_assingNumber == 'floor':

                udf_floor_assingNumber = udf(UDFMethods.udf_floor,FloatType())

                df = df.select(
                    '*', udf_floor_assingNumber(df[null_processing_assingNumber]).alias('temp_name'))
                df = df.drop(null_processing_assingNumber)
                df = df.withColumnRenamed('temp_name', null_processing_assingNumber)

            # decoration 值转换
            elif null_processing_assingNumber == 'decoration':

                udf_decoration_assingNumber = udf(UDFMethods.udf_decoration,FloatType())

                df = df.select(
                    '*', udf_decoration_assingNumber(df[null_processing_assingNumber]).alias('temp_name'))
                df = df.drop(null_processing_assingNumber)
                df = df.withColumnRenamed('temp_name', null_processing_assingNumber)

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

    instance = NullAndDuplications()
    df = instance.finalFillNull(df)
    df.show(truncate=False)

    spark.stop()
    sc.stop()





















