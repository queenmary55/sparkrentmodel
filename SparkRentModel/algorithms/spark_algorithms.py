import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# from pyspark.mllib.linalg import Vectors, VectorUDT
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor
from algorithms.importance import importance_features_map

os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

sparkConf = SparkConf() \
    .setAppName('pyspark rentmodel') \
    .setMaster('local[*]')
sc = SparkContext.getOrCreate(sparkConf)

sc.setLogLevel('WARN')
spark = SparkSession(sparkContext=sc)

df = spark.read.csv('/root/processed/part-00000-c6414b22-aa6e-4195-ac59-406c32cf18ce.csv',header = True, encoding='utf-8')
df = df.drop('crawl_time')

# trainingData=df.rdd.map(lambda x:(Vectors.dense([float(i) for i in x[0:-1]]), float(x[-1])),VectorUDT()).toDF(["features", "label"])
trainingData=df.rdd.map(lambda x:(Vectors.dense([float(i) for i in x[0:-1]]), float(x[-1]))).toDF(["features", "label"])

trainingData.show()
rf = RandomForestRegressor()
model = rf.fit(trainingData)


importance_features_map(df,model,'price')



