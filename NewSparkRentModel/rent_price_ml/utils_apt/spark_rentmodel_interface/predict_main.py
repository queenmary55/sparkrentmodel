#!usr/bin/ python
# - * - coding:utf-8 - * -


import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import copy

from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel, GBTRegressionModel

from rent_price_ml.utils_apt.spark_rentmodel_interface.agency_name_fangtianxia_config import agency_name_conf as gnc_fangtianxia
from rent_price_ml.utils_apt.spark_rentmodel_interface.agency_name_ganji_config import agency_name_conf as gnc_ganji
from rent_price_ml.utils_apt.spark_rentmodel_interface.zone_58_config import zone_conf as zc_58
from rent_price_ml.utils_apt.spark_rentmodel_interface.zone_fangtianxia_config import zone_conf as zc_fangtianxia
from rent_price_ml.utils_apt.spark_rentmodel_interface.zone_ganji_config import zone_conf as zc_ganji
from rent_price_ml.utils_apt.spark_rentmodel_interface.zone_three_config import zone_conf as zc_three

from rent_price_ml.utils_apt.spark_rentmodel_interface.assign_num_fields import assingNumber
from rent_price_ml.utils_apt.spark_rentmodel_interface.facilities_fields import newDataFacilities
from rent_price_ml.utils_apt.spark_rentmodel_interface.num_fields import numFields
from rent_price_ml.utils_apt.spark_rentmodel_interface.one_hot_fields import newDataOneHot
from pyspark.sql.functions import lit
from pyspark.ml.feature import VectorAssembler


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


def fieldsProcess(tmp_input_dict_data,path):

    # 1.model need all fields:
    all_fields_list = ['agency_name','direction','rent_type','district', 'pay_type','zone',
                       'facilities','decoration','floor','price','area','room_num','hall_num',
                       'toilet_num', 'floor_total', 'is_broker']
    oneHot_fields_list = ['agency_name','direction','rent_type','district', 'pay_type','zone']
    num_fields_list = ['area', 'room_num', 'hall_num', 'toilet_num', 'floor_total', 'is_broker']
    assign_num_fields_list = ['decoration','floor']
    facilities_field = 'facilities'

    keys_list = list(tmp_input_dict_data.keys())

    # 2.to check the existance fields in all_fields_list from input dataframe
    exist_fields = set(all_fields_list) & set(keys_list)

    # 3.to check the no existance fields in all_fields_list from input dataframe
    no_exist_fields = set(all_fields_list) - exist_fields

    # 4.processing null value
    for g in exist_fields:
        v = tmp_input_dict_data[g]
        if (g in oneHot_fields_list) & (v == ''):
            tmp_input_dict_data[g] = '其他'

        if (g == 'area') & (v == None):
            try:
                valueError = 1/0
            except Exception as e:
                tmp_input_dict_data[g] = 20
                print('area can not be null value ! the default value is 20.0')

        if (g in ['room_num', 'hall_num', 'toilet_num']) & (v == None):
            tmp_input_dict_data[g] = 1.0
        if (g == 'floor_total') & (v == None):
            tmp_input_dict_data[g] = 5.0
        if (g == 'is_broker') & (v == None):
            tmp_input_dict_data[g] = 0.0
        if (g in assign_num_fields_list) & (v == ''):
            tmp_input_dict_data[g] = 0.0
        if (g == 'facilities') & (v == ''):
            tmp_input_dict_data[g] = '其他'


    # 5. to check that the data is illegal
    for k in exist_fields:
        if k in num_fields_list:
            value = tmp_input_dict_data[k]
            if (isinstance(value,int) | (isinstance(value,float))):
                if value < 0:
                    try:
                        valueError = 1 / 0
                    except Exception as e:
                        if ((k == 'is_broker') & (value not in [0, 1])) | (
                                (k == 'is_broker') & (value not in [0.0, 1.0])):
                            tmp_input_dict_data[g] = 0.0
                            print("is_broker's value must be 0(or 0.0) or 1(or 1.0),the default value is 0.0")
                        else:
                            tmp_input_dict_data[g] = 1.0
                            print('the %s value is illegal, it must be >= 0,the default value is 1.0' % k)

            else:
                print(k + 'value must be int or float type')
        elif k == 'facilities':
            try:
                tmp = tmp_input_dict_data[k].split("|")
            except Exception as e:
                print('it can not split to use "|" if facilities is only a value, else it must be splited to use "|" ')
        else:
            pass

    # 6.create the 'one_room_area'
    if tmp_input_dict_data['rent_type'] == '整租':
        tmp_input_dict_data['one_room_area'] = round(tmp_input_dict_data['area']/tmp_input_dict_data['room_num'],2)
    if tmp_input_dict_data['rent_type'] == '合租':
        tmp_input_dict_data['one_room_area'] = tmp_input_dict_data['area']
    if tmp_input_dict_data['rent_type'] == '其他':
        tmp_input_dict_data['one_room_area'] = round(tmp_input_dict_data['area'] / tmp_input_dict_data['room_num'], 2)
        if tmp_input_dict_data['one_room_area'] < 30:
            tmp_input_dict_data['one_room_area'] = tmp_input_dict_data['area']
    else:
        pass

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
        if tmp_input_dict_data['agency_name'] in fangtianxia_agency_name_keys:
            tmp_input_dict_data['agency_name'] = fangtianxia_agency_name[tmp_input_dict_data['agency_name']]
        else:
            tmp_input_dict_data['agency_name'] = '其他'

        if tmp_input_dict_data['zone'] in fangtianxia_zone_keys:
            tmp_input_dict_data['zone'] = zone_fangtianxia[tmp_input_dict_data['zone']]
        else:
            tmp_input_dict_data['zone'] = '其他'

    if 'ganji' in path:
        ganji_agency_name_keys = ganji_agency_name.keys()
        ganji_zone_keys = zone_ganji.keys()
        if tmp_input_dict_data['agency_name'] in ganji_agency_name_keys:
            tmp_input_dict_data['agency_name'] = ganji_agency_name[tmp_input_dict_data['agency_name']]
        else:
            tmp_input_dict_data['agency_name'] = '其他'

        if tmp_input_dict_data['zone'] in ganji_zone_keys:
            tmp_input_dict_data['zone'] = zone_ganji[tmp_input_dict_data['zone']]
        else:
            tmp_input_dict_data['zone'] = '其他'

    if 'baifenzhi20' in path:
        del tmp_input_dict_data['agency_name']
        keys = zone_58.keys()
        if tmp_input_dict_data['zone'] in keys:
            tmp_input_dict_data['zone'] = zone_58[tmp_input_dict_data['zone']]
        else:
            tmp_input_dict_data['zone'] = '其他'

    if 'baifenzhi15' in path:
        del tmp_input_dict_data['agency_name']
        keys = zone_three.keys()
        if tmp_input_dict_data['zone'] in keys:
            tmp_input_dict_data['zone'] = zone_three[tmp_input_dict_data['zone']]
        else:
            tmp_input_dict_data['zone'] = '其他'

    # 8.to transform the tmp_input_dict_data into spark's DataFrame
    df = spark.createDataFrame([tmp_input_dict_data])

    # 9.the data transform
    one_hot_cols1 = ['direction', 'rent_type', 'district', 'pay_type', 'zone']
    one_hot_cols2 = ['agency_name', 'direction', 'rent_type', 'district', 'pay_type', 'zone']

    model_path1 = '../preprocess_models/save_models_ALL_58_beijingbaifenzhi20/'
    model_path3 = '../preprocess_models/save_models_ALL_fangtianxia_beijing/'
    model_path4 = '../preprocess_models/save_models_ALL_ganji_beijing/'

    for j in no_exist_fields:
        if (j in oneHot_fields_list) | (j == facilities_field):
            df = df.withColumn(j,lit('其他'))
        elif (j in num_fields_list) | (j in assign_num_fields_list):
            df = df.withColumn(j, lit(0.0))
        else:
            pass

    for i in exist_fields:
        if i in oneHot_fields_list:
            if ('baifenzhi' in path) & (i in one_hot_cols1):
                if 'baifenzhi20' in path:
                    df = newDataOneHot(df, model_path1,i)
                # if 'baifenzhi15' in path:
                #     df = newDataOneHot(df, model_path2,i)

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
            # if 'baifenzhi15' in path:
            #     df = newDataFacilities(df, model_path2)
            if 'fangtianxia' in path:
                df = newDataFacilities(df, model_path3)
            if 'ganji' in path:
                df = newDataFacilities(df, model_path4)
        else:
            pass


    # let the order is the same to the data of training model
    if 'baifenzhi' in path:
        df = df.select(['hall_num', 'toilet_num', 'floor', 'decoration', 'facilities_vectors', 'floor_total',
                    'is_broker', 'directionVec', 'zoneVec', 'rent_typeVec', 'districtVec',
                    'pay_typeVec', 'area', 'room_num', 'one_room_area'])
    if ('fangtianxia' in path) | ('ganji' in path):
        df = df.select(['hall_num', 'toilet_num', 'floor', 'decoration', 'facilities_vectors', 'floor_total',
                    'is_broker', 'directionVec', 'zoneVec', 'rent_typeVec', 'agency_nameVec', 'districtVec',
                    'pay_typeVec', 'area', 'room_num', 'one_room_area'])
    return  df

def loadModel(input_dict_data,path):
    tmp_input_dict_data = copy.deepcopy(input_dict_data)
    df = fieldsProcess(tmp_input_dict_data,path)
    vecAssembler = VectorAssembler(inputCols=df.columns, outputCol="features")
    df = vecAssembler.transform(df)

    if 'gbdt' in path:
        loadmodel = GBTRegressionModel.load(path)
    else:
        loadmodel = RandomForestRegressionModel.load(path)

    predict_result = loadmodel.transform(df)

    return predict_result

def rentPricePredict(input_dict_data):

    model_path = ['../preprocess_models/model_ALL_ganji_beijing_gbdt20',
                  '../preprocess_models/model_ALL_fangtianxia_beijing_gbdt20',
                  '../preprocess_models/model_ALL_58_beijing_baifenzhi20_rf19'
                 ]
    result = []
    for i in model_path:
        df = loadModel(input_dict_data,i)
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

    mean_result = round(sum(result)/3, 2)
    resp = {'predict_price': mean_result}
	
    return resp



if __name__ == '__main__':
    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
       .setAppName('pyspark rentmodel') \
       .setMaster('local[*]')

    sc = SparkContext.getOrCreate(conf = sparkConf)

    sc.setLogLevel('ERROR')
    spark = SparkSession(sparkContext=sc)

    start = time.time()

    input_dict_data = {'agency_name':'北京影天', 'direction':'南', 'rent_type':'整租', 'district':'怀柔',
                       'pay_type':'押一付三', 'zone':'怀柔城区','facilities':'床|衣柜|沙发|电视|洗衣机|空调|热水器|宽带|暖气',
                       'decoration':'豪', 'floor':'高', 'area':None, 'room_num':None, 'hall_num':1,'toilet_num':1,
                       'floor_total':10, 'is_broker':0,'hahahha':'jheihi'}
    df = rentPricePredict(input_dict_data)

    end = time.time()

    print('程序运行时间（分钟）：', round((end - start) / 60, 2))

    sc.stop()
    spark.stop()