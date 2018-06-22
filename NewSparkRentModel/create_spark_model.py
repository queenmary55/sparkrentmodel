import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os

# from pyspark.mllib.linalg import Vectors, VectorUDT

from pyspark.sql.functions import udf
import pandas as pd
import numpy as np
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel

os.environ["PYSPARK_PYTHON"]="/home/hadoop/.pyenv/versions/anaconda3-4.2.0/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"]="/home/hadoop/.pyenv/versions/anaconda3-4.2.0/bin/python"

#conf=SparkConf().setAppName("pyspark rentmodel").setMaster("spark://10.81.233.21:7077")
conf=SparkConf().setAppName("pyspark rentmodel_new").setMaster("yarn-client").set("spark.driver.memory", "6g").set('spark.driver.extraJavaOptions','5g').set('spark.storage.memoryFraction','0.4').set("spark.executor.instances", "6").set("spark.executor.memory", "4g").set('spark.yarn.am.memoryOverhead','410').set("spark.executor.cores", '6').set("spark.yarn.queue", "batch")
#sparkConf = SparkConf().setAppName('pyspark rentmodel')#.setMaster('local[*]')
sc = SparkContext(conf=conf)

sc.setLogLevel('ERROR')
spark = SparkSession(sparkContext=sc)

def read_parquet(parquet_path):
    parquet_df = spark.read.parquet(parquet_path)
   
    parquet_df= parquet_df.drop('id')
    parquet_df = parquet_df.drop('one_area_price')
    parquet_df= parquet_df.drop('agency_nameVec')
    parquet_df= parquet_df.drop('districtVec')
    parquet_df= parquet_df.drop('room_type')
    parquet_df.show(truncate=False)
    print('parquet_df.count()==========11',parquet_df.count(),parquet_df.columns)
    for i in parquet_df.columns:
        if ('Vec'not in i) & ('facilities_vectors' not  in i):

            if parquet_df.filter(parquet_df[i].isNull()).count() > 0:

                parquet_df = parquet_df.na.fill(0,i)
            elif parquet_df.filter(parquet_df[i] == 'NULL').count() > 0:

                parquet_df = parquet_df.filter(parquet_df[i] != 'NULL')

            parquet_df = parquet_df.select('*',parquet_df[i].cast('float').alias('tmp_name')).drop(i)
            parquet_df = parquet_df.withColumnRenamed('tmp_name',i)
        parquet_df = parquet_df.filter(parquet_df[i].isNotNull())
        print('parquet_df.count()==========22',i,parquet_df.count())

    columns = parquet_df.columns
    columns.remove('price')
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, StringIndexerModel
    from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
    model_path = "/user/limeng/ganji_daxing_save_models/"
    columns_list = []
    for i in columns:
        if i == 'facilities_vectors':
            loadedCountVectorizerModel = CountVectorizerModel.load(model_path + 'count-vectorizer-model')
            temp = loadedCountVectorizerModel.vocabulary
            columns_list.extend(temp)
        elif i == 'rent_typeVec':
            loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelrent_type')
            temp = loadedStringIndexerModel.labels
            columns_list.extend(temp)
        elif i == 'agency_nameVec':
            loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelagency_name')
            temp = loadedStringIndexerModel.labels
            columns_list.extend(temp)
        elif i == 'directionVec':
            loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modeldirection')
            temp = loadedStringIndexerModel.labels
            columns_list.extend(temp)
        elif i == 'zoneVec':
            loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelzone')
            temp = loadedStringIndexerModel.labels
            columns_list.extend(temp)
        elif i == 'pay_typeVec':
            loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modelpay_type')
            temp = loadedStringIndexerModel.labels
            columns_list.extend(temp)
        elif i == 'districtVec':
            loadedStringIndexerModel = StringIndexerModel.load(model_path + 'stringIndexer_modeldistrict')
            temp = loadedStringIndexerModel.labels
            columns_list.extend(temp)
        else:
            columns_list.append(i)

    vecAssembler = VectorAssembler(inputCols=columns, outputCol="features")
    parquet_df = vecAssembler.transform(parquet_df).select('features','price')

    parquet_df = parquet_df.withColumnRenamed('price', 'label')
     

    return parquet_df,columns_list


def importance_features_map(columns_list, regressor):

    importances_values = regressor.featureImportances
    importances_values = importances_values.toArray()

    arr = np.array([columns_list, importances_values])
    importance_map_df = pd.DataFrame(arr.T, columns=['X_columns', 'importances_values'])
    importance_map_df.apply(pd.to_numeric, errors='ignore')
    importance_map_df = importance_map_df.sort_values(by='importances_values', ascending=False)

    return importance_map_df


start = time.time()
parquet_path = '/user/limeng/data/ganji_daxing.parquet'
df, columns_list = read_parquet(parquet_path)

print('=====================')
df.show()

(trainingData, testData) = df.randomSplit([0.7, 0.3])
rf = RandomForestRegressor(numTrees=20,maxDepth=15,impurity="variance")
print('model_train_start======================')
model = rf.fit(trainingData)
model.save('/user/limeng/data/ganji_daxing_RF_model')

#model = RandomForestRegressionModel.load('/user/limeng/data/fangtianxia_daxing_RF_model')
predict_value = model.transform(testData)
print('predict==============')
predict_value.show(truncate=False)

predict_value_rate = predict_value.rdd.map(lambda x:(x[1],x[2],abs(x[1]-x[2])/x[1])).toDF(['label',' prediction','rsidual_rate'])
print('predict_value_rate-----------------------------')
predict_value_rate = predict_value_rate.sort("rsidual_rate", ascending=False)

predict_value_rate.write.mode("overwrite").options(header="true").csv('/user/limeng/fangtianxia_daxing_predict_result')

#predict_value_rate.write.mode('overwrite').parquet('/user/limeng/data/fangtianxia_daxing_predict_result.parquet')
#spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")
#spark.sql("set hive.exec.dynamic.partition=true")
#spark.sql('use dm')
#predict_value_rate.registerTempTable("table1")
#spark.sql('insert overwrite table ganji_predcit_result select * from table1')
#predict_value_rate.write.format("parquet").mode("overwrite").saveAsTable('dm.ganji_daxing_predict')

predict_value_rate.show(truncate =False)
num = predict_value_rate.count()
num20 = predict_value_rate.filter(predict_value_rate["rsidual_rate"] <= 0.2).count()
num10 = predict_value_rate.filter(predict_value_rate["rsidual_rate"] <= 0.1).count()

cover_rate20 = round(num20/num,3)
cover_rate10 = round(num10/num, 3)
print('cover_rate20:',cover_rate20,'\n','cover_rate10:',cover_rate10)

importance_map_df = importance_features_map(columns_list, model)
importance_map_df = spark.createDataFrame(importance_map_df)
importance_map_df.write.mode("overwrite").options(header="true").csv('/user/limeng/importance_map_df')
#print('model.featureImportances===========',importance_map_df)


end = time.time()

print('程序运行时间（秒）:', round(end - start, 2))


sc.stop()
spark.stop()
