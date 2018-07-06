#!usr/bin/ python
#- * - coding:utf-8 - * -


from pyspark.sql.functions import lit

def numFields(df,col):
    df = df.na.fill(0.0,col)

    df_NULL = df.filter(df[col] == 'NULL')
    if df_NULL.count() > 0:
        df = df.withColunm('tmp',lit(0)).drop(col)
        df = df.withColumnRenamed('tmp',col)
    return df
