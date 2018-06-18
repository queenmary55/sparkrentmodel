#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: check_null_rate.py
@time: 2018/05/{DAY}
"""


def checkNullRate(df):

    """"
            其中.count()方法的*参数（列名的位置）表示该方法计算所有的列。且前面的*表示.agg()方法将该列表处理为一组独立的参数传递给函数.
            可以考虑用这个来做：
            temp = df.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in df.columns])
            其中count只统计非空的值的行数1 - (fn.count(c)就是空值的行数。
        """

    df_rows = df.count()
    colums = list(df.columns)

    if df_rows > 0:
        for i in colums:
            # 注意：1、在excel中值显示为NULL的pyspark读进来是字符串的'NULL'这个，因此isNull函数无法识别,之所以这样是因为这一列
            # 的数值类型是string,所以null也变成了字符串的NULL了,如果此列是int则，此列在excel即使显示是NULL也会被isnull识别。
            # 2、在excel中值显示为空白的，isNull函数能识别

            df_NULL = df.filter(df[i]=='NULL')
            df_nan = df.filter(df[i].isNull())

            rate = (df_nan.count() + df_NULL.count())/df_rows
            if rate > 0.7:
                df = df.drop(i)
            else:
                pass
    else:
        print('the dataframe is null---------------------')

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
    df = checkNullRate(df)

    df.show(truncate=False)

    spark.stop()
    sc.stop()