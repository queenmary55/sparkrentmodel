from pyspark.sql import SparkSession

from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession
import os

os.environ['SPARK_HOME'] = 'D:/spark-2.3.0-bin-hadoop2.7'
os.environ['HADOOP_HOME'] = 'D:/pyCharmSpace/SparkProject/winuntil'


sparkConf = SparkConf()\
    .setAppName('pyspark rentmodel')\
    .setMaster('local[2]')
sc = SparkContext(conf=sparkConf)

sc.setLogLevel('WARN')

spark = SparkSession(sparkContext=sc)

l = [('Alice', 1)]


print(spark.createDataFrame(l).collect())
spark.createDataFrame(l, ['name', 'age']).show()
















# spark = SparkSession \
#     .builder \
#     .master('spark://47.94.39.30:8888')\
#     .appName("Python Spark SQL basic example") \
#     .config("spark.some.config.option", "some-value") \
#     .getOrCreate()




# df = spark.read.json("resource/people.json")
# df.show()
# df.select(df['name'], df['age']).show()


# from pyspark import SparkConf, SparkContext
# from pyspark.sql import HiveContext
# 
# conf = (SparkConf()
#         .setMaster("47.94.39.30:888")
#         .setAppName("My app")
#         .set("spark.executor.memory", "1g"))
# sc = SparkContext(conf=conf)
# sqlContext = HiveContext(sc)
# # my_dataframe = sqlContext.sql("Select count(1) from logs.fmnews_dim_where")
# # my_dataframe.show()


#
# from pyspark.sql import SparkSession
# from pyspark import SparkContext, SparkConf
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
# import os
# os.environ['SPARK_HOME'] = 'D:/spark-2.3.0-bin-hadoop2.7'
# os.environ['HADOOP_HOME'] = 'D:/pyCharmSpace/SparkProject/winuntil'
# sparkConf = SparkConf() \
#     .setAppName('pyspark rentmodel') \
#     .setMaster('local[2]')
# sc = SparkContext(conf=sparkConf)
# sc.setLogLevel('WARN')
# spark = SparkSession(sparkContext=sc)
#
# df = spark.read.csv('D:/ganji_beijing_data/ganji_beijing_pyspark.csv', header=True, encoding='gbk')
# print('df.dtypes=====', df.dtypes)
# df_nan = df.filter(df['price'].isNotNull())
# print(df_nan.count())
# df_nan.show()

#这种方式格式不会错，但要注意，指定读取的schema格式要与从hive中查询时指定的格式一致，之前就是把price这些在hive中指定存储为float
#的格式，在读取的schema中错误地指定为int类型，导致出错，浪费了很长时间才反应过来，唉！！！ 又填得一坑
# s = """
# _c0 STRING, id STRING, crawl_time STRING, district STRING, zone STRING, rent_type STRING,room_type STRING,
# price FLOAT,area FLOAT, room_num INT, hall_num INT, toilet_num INT,floor STRING, floor_total STRING,
# direction STRING, decoration STRING,facilities STRING,pay_type STRING,is_broker INT, agency_name STRING,
# subway STRING, bus STRING"""
#
# df = spark.read.csv('D:/ganji_beijing_data/ganji_beijing_pyspark.csv',schema=s, header=True, encoding='gbk')
    # df.cache()

    # from pyspark.sql import functions as fun
    # from pyspark.sql.types import LongType

    # # df = df.agg(df.price.cast('LongType'))
    # print('df.dtypes=====',df.dtypes)
    #
    # df_nan = df.filter(df['price'].isNotNull())
    # print(df_nan.count())
    # df_nan.show()

    # print('df_nan.count()11',df_nan.count())
    # df.select('price','area').show(truncate=False)
    # df = FillMethods.assignMean(df,'price')
    # df_nan = df.filter(df['price'].isNotNull())
    #
    # print('df_nan.count()22', df_nan.count())
    #
    # df.select('price','area').show(truncate = False)


    # df.write.csv('D:/ganji_beijing_data/processed_ganji_beijing_pyspark.csv', header=True)

    # spark.stop()





    # # 最原始的办法，使用RDD来转换,老是报错，没法！！！！
    #
    # from pyspark.sql.types import *
    #
    # schema = StructType([
    #         StructField("_c0", StringType(), True),StructField("id", StringType(), True),
    #         StructField("crawl_time", StringType(), True),StructField("district", StringType(), True),
    #         StructField("zone", StringType(), True),StructField("rent_type", StringType(), True),
    #         StructField("room_type", StringType(), True),StructField("price", IntegerType(), True),
    #         StructField("area", IntegerType(), True),StructField("room_num", IntegerType(), True),
    #         StructField("hall_num", IntegerType(), True),StructField("toilet_num", IntegerType(), True),
    #         StructField("floor", StringType(), True),StructField("floor_total", IntegerType(), True),
    #         StructField("decoration", StringType(), True),StructField("facilities", StringType(), True),
    #         StructField("pay_type", StringType(), True),StructField("is_broker", IntegerType(), True),
    #         StructField("agency_name", StringType(), True),StructField("subway", StringType(), True),
    #         StructField("bus", StringType(), True)])
    #
    # def map_fun(line):
    #     lst = line.split(',')
    #     return Row(lst[0], lst[1],lst[2],lst[3],lst[4],lst[5],lst[6],lst[7],lst[8],lst[9],lst[10],
    #             lst[11],lst[12],lst[13],lst[14],lst[15],lst[16],lst[17],lst[18],lst[19],lst[20],lst[21])
    #
    # rdd = sc.textFile('D:/ganji_beijing_data/ganji_beijing_pyspark_non_header.csv').map(map_fun)
    #
    # df = spark.createDataFrame(rdd,schema)
    # print(df.count())
    # print(df.dtypes)
    # spark.stop()
    # sc.stop()
