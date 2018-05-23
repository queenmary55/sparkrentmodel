#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: date_to_day.py
@time: 2018/04/{DAY}
"""

import pandas as pd
from resource.configing import config as cf

from dateutil import parser
from pyspark.sql.types import StringType,FloatType


"""
    考虑以爬虫时间来计算，计算爬虫时间到当前日期的的天数，now_date是给定的当前日期，放在配置文件config.py中。
"""

now_date =cf['now_date']
def dateToDay(s):
    try:
        datetime_struct = parser.parse(s)
        # print('datetime_struct;',type(datetime_struct))
        # s=time.strftime('%Y-%m-%d %H:%M:%S', datetime_struct)
        # s = time.mktime(time.strptime(s,"%Y-%m-%d %H:%M:%S"))
        s = datetime_struct.strftime('%Y-%m-%d %H:%M:%S')
        date_diff = pd.to_datetime(now_date) - pd.to_datetime(s)
        return float(date_diff.days)
    except Exception as e:
        # print('input time format of origin data is error:', e)
        return s



def dateToDayTansform(df):
    # 爬虫时间时爬虫程序给的，一般都会有，只是以防万一，这里就直接将没有的值所在的行删除
    df = df.filter(df['crawl_time'].isNotNull())
    df = df.filter(df['crawl_time'] != 'NULL')

    from pyspark.sql.functions import udf

    date_transform = udf(dateToDay,FloatType())

    df = df.select(
        '*',
        date_transform(df['crawl_time']).alias('time_len'))
    df = df.drop('crawl_time')
    df = df.withColumnRenamed('time_len','crawl_time')
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

    df = dateToDayTansform(df)

    df.show(truncate=False)

    spark.stop()
    sc.stop()

