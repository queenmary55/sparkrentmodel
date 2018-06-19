import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np

from pyspark.sql.functions import udf
import pandas as pd
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


def read(path_dir):

    files_rdd = sc.wholeTextFiles(path_dir)
    files_list = list(files_rdd.map(lambda file: [line for line in file[1].split('\n')]).collect())
    tmp = []
    col_names = files_list[1][0].split(',')
    tmp.append(col_names)

    for file in files_list:
        for line in file[1:]:
            if len(line) !=0:
                line = line.split(',')
                tmp.append([float(line[i]) if i != 0 else line[i] for i in range(len(line))])
            else:
                pass

    df = pd.DataFrame(tmp[1:],columns=tmp[0])
    sdf = spark.createDataFrame(df)
    return sdf


def rfRegressor(df):
    df = df.withColumn('tmp_price',df['price'])
    df = df.drop('price')
    df = df.withColumnRenamed('tmp_price','price')



    feature_label = df.rdd.map(lambda x: (Vectors.dense([float(i) for i in x[0:-1]]),
                                         float(x[-1]))).toDF(["features", "label"])

    (trainingData, testData) = feature_label.randomSplit([0.7, 0.3])

    rf = RandomForestRegressor()

    model = rf.fit(trainingData)

    importance_map_df = importance_features_map(df, model, 'price')


    # Make predictions.
    predictions = model.transform(testData)
    predict_df = predictions.select("prediction", "label")
    predict_df = predict_df.withColumn('rate',(predict_df['prediction']-predict_df['label'])/predict_df['label'])

    def udf_rate(s):
        return round(abs(s),3)
    udf_rate = udf(udf_rate)

    predict_df = predict_df.select('*',udf_rate(predict_df['rate']).alias('rates')).drop('rate')

    predict_df.show()

    model.save("/root/myModelPath1")
    sameModel = RandomForestRegressionModel.load("/root/myModelPath1")

    same_predict_df = sameModel.transform(testData)
    print('=======================================')
    same_predict_df.show()

    return  importance_map_df, model


def read_parquet(parquet_path):
    parquet_df = spark.read.parquet(parquet_path)

    parquet_df = parquet_df.drop('id')
    parquet_df = parquet_df.drop('one_area_price')
    # parquet_df = parquet_df.drop('agency_nameVec')l
    # parquet_df = parquet_df.drop('one_room_area')
    # parquet_df = parquet_df.drop('directionVec')

    print('df.columns======', parquet_df.columns,parquet_df.count())
    # parquet_df.show()
    for i in parquet_df.columns:
        if ('Vec'not in i) & ('facilities_vectors' not  in i):
            if parquet_df.filter(parquet_df[i].isNull()).count() > 0:
                parquet_df = parquet_df.na.fill(0,i)
            elif parquet_df.filter(parquet_df[i] == 'NULL').count() > 0:
                parquet_df = parquet_df.filter(parquet_df[i] != 'NULL')
            parquet_df = parquet_df.select('*',parquet_df[i].cast('float').alias('tmp_name')).drop(i)
            parquet_df = parquet_df.withColumnRenamed('tmp_name',i)
        parquet_df = parquet_df.filter(parquet_df[i].isNotNull())

    columns = parquet_df.columns
    columns.remove('price')


    from pyspark.ml.feature import OneHotEncoder, StringIndexer, StringIndexerModel
    from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
    model_path = "/root/save_data_processed_models/"
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
        else:
            columns_list.append(i)

    vecAssembler = VectorAssembler(inputCols=columns, outputCol="features")
    parquet_df = vecAssembler.transform(parquet_df).select('features','price')

    parquet_df = parquet_df.withColumnRenamed('price', 'label')

    return parquet_df, columns_list


def importance_features_map(columns_list, regressor):

    importances_values = regressor.featureImportances
    importances_values = importances_values.toArray()

    arr = np.array([columns_list, importances_values])
    importance_map_df = pd.DataFrame(arr.T, columns=['X_columns', 'importances_values'])
    importance_map_df = importance_map_df.sort_values(by='importances_values', ascending=False)

    return importance_map_df



if __name__ == '__main__':
    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')
    sc = SparkContext.getOrCreate(sparkConf)

    sc.setLogLevel('ERROR')
    spark = SparkSession(sparkContext=sc)

    start = time.time()



    parquet_path = '/root/daxing.parquet'

    df, columns_list = read_parquet(parquet_path)

    (trainingData, testData) = df.randomSplit([0.7, 0.3])
    rf = RandomForestRegressor(numTrees=20, maxDepth=30, seed=1,maxMemoryInMB=4096, impurity="variance")
    print('xxxxxxxxxxxxxxxxxxx=========')
    model = rf.fit(trainingData)

    # all_models_path = '/root/save_models/'
    # rf_path = all_models_path + "rf"
    # rf_model_path = all_models_path + "rf_model"
    #
    # rf.save(rf_path)
    # model.save(rf_model_path)

    predict_value = model.transform(testData)

    # load_rf = RandomForestRegressor.load(rf_path)
    # load_rf_model = RandomForestRegressionModel.load(rf_model_path)
    # predict_value = load_rf_model.transform(testData)

    predict_value = model.transform(testData)
    print('predict==============')
    predict_value.show(truncate=False)

    predict_value_rate = predict_value.rdd.map(lambda x:(x[1],x[2],abs(x[1]-x[2])/x[1])).toDF(['label',' prediction','rsidual_rate'])
    predict_value_rate.write.mode("overwrite").options(header="true").csv('/root/data/loadmodel_predic_values')
    print('predict_value_rate-----------------------------')
    predict_value_rate = predict_value_rate.sort("rsidual_rate", ascending=True)
    # predict_value_rate.write.saveAsTable('label_predict_absolute_error_rate',mode='overwrite')

    num = predict_value_rate.count()
    num20 = predict_value_rate.filter(predict_value_rate["rsidual_rate"] <= 0.2).count()
    num10 = predict_value_rate.filter(predict_value_rate["rsidual_rate"] <= 0.1).count()

    print('==========',num,num20,num10)
    cover_rate20 = round(num20/num,3)
    cover_rate10 = round(num10/num, 3)
    print('cover_rate20:', cover_rate20, '\n', 'cover_rate10:', cover_rate10)

    importance_map_df = importance_features_map(columns_list, model)
    # importance_map_df.to_csv('/root/data/loadmodel_importance.csv')
    print('importance_map_df=======\n', importance_map_df)

    end = time.time()

    print('程序运行时间（分钟）：', round((end - start)/60, 2))

    # rf = RandomForestRegressor(maxMemoryInMB=1024, impurity="variance")
    # numTrees = [80, 100, 150, 200, 300]
    # maxDepth = [6,8,10,12,14]
    # numFolds = 5
    # paramGrid = (ParamGridBuilder()
    #              .addGrid(rf.numTrees, numTrees)\
    #              .addGrid(rf.maxDepth, maxDepth)\
    #              .build())
    # cv = CrossValidator(estimator=rf,
    #                     estimatorParamMaps=paramGrid,
    #                     numFolds=numFolds)
    # cvModel = cv.fit(trainingData)
    # predict_value = cvModel.transform(testData)

    sc.stop()
    spark.stop()





