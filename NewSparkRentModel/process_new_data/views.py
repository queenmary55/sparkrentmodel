#!usr/bin/ python
# - * - coding:utf-8 - * -

from pyspark.ml.feature import OneHotEncoder, StringIndexer, StringIndexerModel
from pyspark.sql.functions import udf

import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import json
#from django.http import JsonResponse
#from django.views.decorators.http import require_POST

"""
    1.获取前端传来的数据并处理
    2.字段的处理
        1.1 one-hot编码字段的处理:'agency_name','direction','rent_type','district', 'pay_type','zone'
        1.2 facilities字段的处理: facilities
        1.3 指定数值字段的处理: decoration,floor 
        1.4 num_fields: area, room_num, hall_num, toilet_num, floor_total, is_broker, one_room_area
    3.加载机器学习模型
    4.将结果返回前端
"""

from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel, GBTRegressionModel

from process_new_data.agency_name_fangtianxia_config import agency_name_conf as gnc_fangtianxia
from process_new_data.agency_name_ganji_config import agency_name_conf as gnc_ganji
from process_new_data.zone_58_config import zone_conf as zc_58
from process_new_data.zone_fangtianxia_config import zone_conf as zc_fangtianxia
from process_new_data.zone_ganji_config import zone_conf as zc_ganji
from process_new_data.zone_three_config import zone_conf as zc_three

from process_new_data.assign_num_fields import assingNumber
from process_new_data.facilities_fields import newDataFacilities
from process_new_data.num_fields import numFields
from process_new_data.one_hot_fields import newDataOneHot
from pyspark.sql.functions import lit


def fieldsProcess(input_dict_data,path):

    # 1.model need all fields:
    all_fields_list = ['agency_name','direction','rent_type','district', 'pay_type','zone',
                       'facilities','decoration','floor','price','area','room_num','hall_num',
                       'toilet_num', 'floor_total', 'is_broker']
    oneHot_fields_list = ['agency_name','direction','rent_type','district', 'pay_type','zone']
    num_fields_list = ['area', 'room_num', 'hall_num', 'toilet_num', 'floor_total', 'is_broker']
    assign_num_fields_list = ['decoration','floor']
    facilities_field = 'facilities'

    keys_list = list(input_dict_data.keys())

    # 2.to check the existance fields in all_fields_list from input dataframe
    exist_fields = set(all_fields_list) & set(keys_list)

    # 3.to check the no existance fields in all_fields_list from input dataframe
    no_exist_fields = set(all_fields_list) - exist_fields

    # 4.processing null value
    for g in exist_fields:
        v = input_dict_data[g]
        if (g in oneHot_fields_list) & (v == None):
            input_dict_data[g] = '其他'
        if (g == 'area') & (v == None):
            print('area can not be null value !')
            break
        if (g in ['room_num', 'hall_num', 'toilet_num']) & (v == None):
            input_dict_data[g] = 1.0
        if (g == 'floor_total') & (v == None):
            input_dict_data[g] = 5.0
        if (g == 'is_broker') & (v == None):
            input_dict_data[g] = 0.0
        if (g in assign_num_fields_list) & (v == None):
            input_dict_data[g] = 0.0
        if (g == 'facilities') & (v == None):
            input_dict_data[g] = '其他'


    # 5. to check that the data is illegal
    for k in exist_fields:
        print('k----------------',k)
        if k in num_fields_list:
            value = input_dict_data[k]
            print('value==============',k,value)
            if (isinstance(value,int) | (isinstance(value,float))):
                if value < 0:
                    print('the value is illegal')
                    break

                if ((k == 'is_broker') & (value not in [0,1])) | ((k == 'is_broker') & (value not in [0.0,1.0])):
                    print("is_broker's value must be 0(or 0.0) or 1(or 1.0)")
                    break

            else:
                print(k + 'value must be int or float type')
        elif k == 'facilities':
            try:
                tmp = input_dict_data[k].split("|")
                print('tmp==========',tmp)
            except Exception as e:
                print('it can not split to use "|" if facilities is only a value, else it must be splited to use "|" ')
        else:
            pass

    # 6.create the 'one_room_area'
    if input_dict_data['rent_type'] == '整租':
        input_dict_data['one_room_area'] = round(input_dict_data['area']/input_dict_data['room_num'],2)
    if input_dict_data['rent_type'] == '合租':
        input_dict_data['one_room_area'] = input_dict_data['area']

    # 7. data's uniformity('zone' and 'agency_name')
    fangtianxia_agency_name = gnc_fangtianxia.get('agency_name')
    ganji_agency_name = gnc_ganji.get('agency_name')
    zone_58 = zc_58.get('zone')
    zone_fangtianxia = zc_fangtianxia.get('zone')
    zone_ganji = zc_ganji.get('zone')
    zone_three = zc_three.get('zone')

    if 'fangtianxia' in path:
        fangtianxia_agency_name_keys = fangtianxia_agency_name.keys()
        fangtianxia_zone_keys = zone_fangtianxia.keys()
        if input_dict_data['agency_name'] in fangtianxia_agency_name_keys:
            input_dict_data['agency_name'] = fangtianxia_agency_name_keys[input_dict_data['agency_name']]
        else:
            input_dict_data['agency_name'] = '其他'

        if input_dict_data['zone'] in fangtianxia_zone_keys:
            input_dict_data['zone'] = fangtianxia_zone_keys[input_dict_data['zone']]
        else:
            input_dict_data['zone'] = '其他'

    if 'ganji' in path:
        ganji_agency_name_keys = ganji_agency_name.keys()
        ganji_zone_keys = zone_ganji.keys()
        if input_dict_data['agency_name'] in ganji_agency_name_keys:
            input_dict_data['agency_name'] = ganji_agency_name_keys[input_dict_data['agency_name']]
        else:
            input_dict_data['agency_name'] = '其他'

        if input_dict_data['zone'] in ganji_zone_keys:
            input_dict_data['zone'] = ganji_zone_keys[input_dict_data['zone']]
        else:
            input_dict_data['zone'] = '其他'

    if 'baifenzhi20' in path:
        del input_dict_data['agency_name']
        keys = zone_58.keys()
        if input_dict_data['zone'] in keys:
            input_dict_data['zone'] = zone_58[input_dict_data['zone']]
        else:
            input_dict_data['zone'] = '其他'

    if 'baifenzhi15' in path:
        del input_dict_data['agency_name']
        keys = zone_three.keys()
        if input_dict_data['zone'] in keys:
            input_dict_data['zone'] = zone_three[input_dict_data['zone']]
        else:
            input_dict_data['zone'] = '其他'

    # 8.to transform the input_dict_data into spark's DataFrame
    print('input_dict_data==========2222', input_dict_data)
    df = spark.createDataFrame([input_dict_data])

    # 9.the data transform
    one_hot_cols1 = ['direction', 'rent_type', 'district', 'pay_type', 'zone']
    one_hot_cols2 = ['agency_name', 'direction', 'rent_type', 'district', 'pay_type', 'zone']

    model_path1 = '/user/limeng/20180703/save_models_ALL_58_beijingbaifenzhi20/'
    model_path2 = '/user/limeng/20180703/save_models_three_beijingbaifenzhi15/'
    model_path3 = '/user/limeng/20180703/save_models_ALL_fangtianxia_beijing/'
    model_path4 = '/user/limeng/20180703/save_models_ALL_ganji_beijing/'
    for i in exist_fields:
        if i in oneHot_fields_list:
            print('path---------------',i,path)
            if ('baifenzhi' in path) & (i in one_hot_cols1):
                print('baifenzhi----------------')
                if 'baifenzhi20' in path:
                    df = newDataOneHot(df, model_path1,i)
                if 'baifenzhi15' in path:
                    df = newDataOneHot(df, model_path2,i)

            elif (('fangtianxia' in path) | ('ganji' in path)) & (i in one_hot_cols2):
                if 'fangtianxia' in path:
                    df = newDataOneHot(df, model_path3,i)
                if 'ganji' in path:
                    df = newDataOneHot(df, model_path4,i)
            else:
                pass

        elif i in num_fields_list:
            df = numFields(df,i)
        elif i in assign_num_fields_list:
            df = assingNumber(df)
        elif i == facilities_field:
            if 'baifenzhi20' in path:
                df = newDataFacilities(df, model_path1)
            if 'baifenzhi15' in path:
                df = newDataFacilities(df, model_path2)
            if 'fangtianxia' in path:
                df = newDataFacilities(df, model_path3)
            if 'ganji' in path:
                df = newDataFacilities(df, model_path4)
        else:
            pass

    for j in no_exist_fields:
        if (j in oneHot_fields_list) | (j == facilities_field):
            df = df.withColumn(i,lit('其他'))
        elif (j in num_fields_list) | (j in assign_num_fields_list):
            df = df.withColumn(j, lit(0.0))
        else:
            pass

    # let the order is the same to the data of training model
    print('sucess==============================')
    df.show(truncate=False)
    if 'baifenzhi' in path:
        df = df.select(['hall_num', 'toilet_num', 'floor', 'decoration', 'facilities_vectors', 'floor_total',
                    'is_broker', 'directionVec', 'zoneVec', 'rent_typeVec', 'districtVec',
                    'pay_typeVec', 'area', 'price', 'room_num', 'one_room_area'])
    if ('fangtianxia' in path) | ('ganji' in path):
        df = df.select(['hall_num', 'toilet_num', 'floor', 'decoration', 'facilities_vectors', 'floor_total',
                    'is_broker', 'directionVec', 'zoneVec', 'rent_typeVec', 'agency_nameVec', 'districtVec',
                    'pay_typeVec', 'area', 'price', 'room_num', 'one_room_area'])
    return  df

def loadModel(input_dict_data,path):
    df = fieldsProcess(input_dict_data,path)

    if 'gbdt' in path:
        loadmodel = GBTRegressionModel.load(path)
    else:
        loadmodel = RandomForestRegressionModel.load(path)

    predict_result = loadmodel.transform(df)

    return predict_result

def rentPricePredict(input_dict_data):

    model_path = ['/user/limeng/20180703/limeng/data/model_ALL_58_beijing_baifenzhi20_rf20',
                  '/user/limeng/20180703/limeng/data/model_ALL_fangtianxia_beijing_rf22',
                  '/user/limeng/20180703/limeng/data/model_ALL_ganji_beijing_gbdt20',
                  '/user/limeng/20180703/limeng/data/model_three_beijingbaifenzhi15_rf19'
                  ]
    result = []
    for i in model_path:
        df = loadModel(input_dict_data,i)
        print('1111111111===========')
        df.show()
        try:
            num = df.count()
            if num == 1:
                row = df.select('prediction').collect()
                price = row[0]['prediction']
            else:
                pass
        except Exception as e:
            print('error:', e)
        finally:
            result.append(price)

    mean_result = round(sum(result)/4, 2)
    resp = {'predict_price': mean_result}

#    response = JsonResponse(resp)
#    return response



if __name__ == '__main__':
   # os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

   # sparkConf = SparkConf() \
    #    .setAppName('pyspark rentmodel') \
     #   .setMaster('local[*]')


    import os
    os.environ["PYSPARK_PYTHON"]="/home/hadoop/.pyenv/versions/anaconda3-4.2.0/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"]="/home/hadoop/.pyenv/versions/anaconda3-4.2.0/bin/python"

    conf=SparkConf().setAppName("pyspark rentmodel_interface").setMaster("yarn-client").set("spark.driver.memory", "3g")\
        .set('spark.yarn.am.memory','3g').set("spark.executor.instances", "6").set("spark.executor.memory", "3g")\
        .set("spark.executor.cores", '4')#.set("spark.yarn.queue", "batch")#.set('yarn.scheduler.maximum-allocation-mb','5g')
    sc = SparkContext.getOrCreate(conf = conf)

    sc.setLogLevel('ERROR')
    spark = SparkSession(sparkContext=sc)

    start = time.time()

    input_dict_data = {'agency_name':'北京影天', 'direction':'南', 'rent_type':'合租', 'district':'怀柔',
                       'pay_type':'押一付三', 'zone':'怀柔城区','facilities':'床|衣柜|沙发|电视|洗衣机|空调|热水器|宽带|暖气',
                       'decoration':'中', 'floor':'高', 'price':650, 'area':20, 'room_num':3, 'hall_num':1,'toilet_num':1,
                       'floor_total':6, 'is_broker':1}
    print('input_dict_data==========1',input_dict_data)
    df = rentPricePredict(input_dict_data)

    end = time.time()

    print('程序运行时间（分钟）：', round((end - start) / 60, 2))

    sc.stop()
    spark.stop()
