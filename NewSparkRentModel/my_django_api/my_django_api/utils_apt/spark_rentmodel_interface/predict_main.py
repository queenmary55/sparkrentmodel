#!usr/bin/ python
# - * - coding:utf-8 - * -


import time
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os
import copy
from random import choice
import pandas as pd

from pyspark.ml.regression import GBTRegressionModel
from my_django_api.utils_apt.spark_rentmodel_interface.agency_name_fangtianxia_config import agency_name_conf as gnc_fangtianxia
from my_django_api.utils_apt.spark_rentmodel_interface.agency_name_ganji_config import agency_name_conf as gnc_ganji
from my_django_api.utils_apt.spark_rentmodel_interface.zone_58_config import zone_conf as zc_58
from my_django_api.utils_apt.spark_rentmodel_interface.zone_fangtianxia_config import zone_conf as zc_fangtianxia
from my_django_api.utils_apt.spark_rentmodel_interface.zone_ganji_config import zone_conf as zc_ganji


from my_django_api.utils_apt.spark_rentmodel_interface.assign_num_fields import assingNumber
from my_django_api.utils_apt.spark_rentmodel_interface.facilities_fields import newDataFacilities
from my_django_api.utils_apt.spark_rentmodel_interface.num_fields import numFields
from pyspark.sql.functions import lit
from pyspark.ml.feature import VectorAssembler

from pyspark.sql import Row
from pyspark.ml.linalg import SparseVector

from my_django_api.utils_apt.one_hot_labels import one_hot_labels_ganji,one_hot_labels_fangtianxia


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



# 1.model need all fields:
all_fields_list = ['agency_name','direction','rent_type','district', 'pay_type','zone',
                   'facilities','decoration','floor','price','area','room_num','hall_num',
                   'toilet_num', 'floor_total', 'is_broker']
oneHot_fields_list = ['agency_name','direction','rent_type','district', 'pay_type','zone']
num_fields_list = ['area', 'room_num', 'hall_num', 'toilet_num', 'floor_total', 'is_broker']
assign_num_fields_list = ['decoration','floor']
facilities_field = 'facilities'

facilities_vocab_ganji = ['床', '空调', '热水器', '洗衣机', '冰箱', '暖气', '电视', '可做饭',
                          '宽带网', '衣柜', '沙发', '阳台', '独卫', '微波炉']

# facilities_vocab_fangtianxia = ['热水器', '空调', '暖气', '冰箱', '床', '洗衣机', '宽带', '电视', '家具', '煤气',
#                             '电梯', '电话', '车位', '厨具', '储藏室/地下室', '其他', '微波炉', '阁楼', '露台',
#                             '家电', '阳台', '独卫', '阳光房', '游泳池', '沙发', '衣柜']

# facilities_vocab_58 = ['床', '热水器', '空调', '冰箱', '洗衣机', '暖气', '宽带', '电视', '衣柜',
#                    '阳台', '沙发', '独卫', '可做饭']


def fieldsProcess(tmp_input_dict_data):

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
                    if ((k == 'is_broker') & (value not in [0, 1])) | (
                            (k == 'is_broker') & (value not in [0.0, 1.0])):
                        tmp_input_dict_data[g] = 0.0
                        raise ("is_broker's value must be 0(or 0.0) or 1(or 1.0),the default value is 0.0")
                    else:
                        tmp_input_dict_data[g] = 1.0
                        raise ('the %s value is illegal, it must be >= 0,the default value is 1.0' % k)
            else:
                print(k + 'value must be int or float type')
        elif k == 'facilities':
            try:
                tmp = tmp_input_dict_data[k].split("|")
            except Exception as e:
                raise('it can not split to use "|" if facilities is only a value, else it must be splited to use "|" ')
        else:
            pass

    # 给不存在字段赋值
    for j in no_exist_fields:
        if (j in oneHot_fields_list) | (j == facilities_field):
            tmp_input_dict_data[j] = '其他'
        elif (j in num_fields_list) | (j in assign_num_fields_list):
            tmp_input_dict_data[j] = 0.0
        else:
            pass

    # 指定数字字段的值
    for i in exist_fields:
        if i in num_fields_list:
            tmp_input_dict_data = numFields(tmp_input_dict_data, i)

        elif i in assign_num_fields_list:
            tmp_input_dict_data = assingNumber(tmp_input_dict_data, i)

# 6.create the 'one_room_area'
    if tmp_input_dict_data['rent_type'] == '整租':
        tmp_input_dict_data['one_room_area'] = round(tmp_input_dict_data['area']/tmp_input_dict_data['room_num'],2)
    if tmp_input_dict_data['rent_type'] == '合租':
        tmp_input_dict_data['one_room_area'] = tmp_input_dict_data['area']
    if tmp_input_dict_data['rent_type'] not in ['整租','合租']:
        tmp_input_dict_data['one_room_area'] = round(tmp_input_dict_data['area'] / tmp_input_dict_data['room_num'], 2)
        if tmp_input_dict_data['one_room_area'] < 30:
            tmp_input_dict_data['one_room_area'] = tmp_input_dict_data['area']
    else:
        pass

    return tmp_input_dict_data


def data_uniformity(tmp_input_dict_data,path):
    tmp_input_dict_data = fieldsProcess(tmp_input_dict_data)

    # input_data_fangtianxia = copy.deepcopy(tmp_input_dict_data)
    input_data_ganji = copy.deepcopy(tmp_input_dict_data)
    # input_data_58 = copy.deepcopy(tmp_input_dict_data)

    # 7. data's uniformity('zone' and 'agency_name')
    # fangtianxia_agency_name = gnc_fangtianxia.get('agency_name')
    ganji_agency_name = gnc_ganji.get('agency_name')
    # zone_58 = zc_58.get('zone')
    # zone_fangtianxia = zc_fangtianxia.get('zone')
    zone_ganji = zc_ganji.get('zone')


    # if 'fangtianxia' in path[1]:
    #     fangtianxia_agency_name_keys = fangtianxia_agency_name.keys()
    #     fangtianxia_zone_keys = zone_fangtianxia.keys()
    #     if input_data_fangtianxia['agency_name'] in fangtianxia_agency_name_keys:
    #         input_data_fangtianxia['agency_name'] = fangtianxia_agency_name[input_data_fangtianxia['agency_name']]
    #     else:
    #         input_data_fangtianxia['agency_name'] = '其他'
    #
    #     if input_data_fangtianxia['zone'] in fangtianxia_zone_keys:
    #         input_data_fangtianxia['zone'] = zone_fangtianxia[input_data_fangtianxia['zone']]
    #     else:
    #         input_data_fangtianxia['zone'] = '其他'

    if 'ganji' in path[0]:
        ganji_agency_name_keys = ganji_agency_name.keys()
        ganji_zone_keys = zone_ganji.keys()
        if input_data_ganji['agency_name'] in ganji_agency_name_keys:
            input_data_ganji['agency_name'] = ganji_agency_name[input_data_ganji['agency_name']]
        else:
            input_data_ganji['agency_name'] = '其他'

        if input_data_ganji['zone'] in ganji_zone_keys:
            input_data_ganji['zone'] = zone_ganji[input_data_ganji['zone']]
        else:
            input_data_ganji['zone'] = '其他'

    # if 'baifenzhi20' in path[2]:
    #     del input_data_58['agency_name']
    #     keys = zone_58.keys()
    #     if input_data_58['zone'] in keys:
    #         input_data_58['zone'] = zone_58[input_data_58['zone']]
    #     else:
    #         input_data_58['zone'] = '其他'


    # return input_data_fangtianxia, input_data_ganji, input_data_58
    return input_data_ganji



def data_transform(tmp_input_dict_data, path):
    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')

    sc = SparkContext.getOrCreate(conf=sparkConf)

    sc.setLogLevel('ERROR')
    spark = SparkSession(sparkContext=sc)


    # input_data_fangtianxia, input_data_ganji, input_data_58 = data_uniformity(tmp_input_dict_data, path)
    input_data_ganji = data_uniformity(tmp_input_dict_data, path)
    preprocess_model_path ={'stringIndexer_model_ganji':
                                [
                                 r'../preprocess_models/save_models_ALL_ganji_beijing/stringIndexer_modelagency_name',
                                 r'../preprocess_models/save_models_ALL_ganji_beijing/stringIndexer_modeldirection',
                                 r'../preprocess_models/save_models_ALL_ganji_beijing/stringIndexer_modeldistrict',
                                 r'../preprocess_models/save_models_ALL_ganji_beijing/stringIndexer_modelpay_type',
                                 r'../preprocess_models/save_models_ALL_ganji_beijing/stringIndexer_modelrent_type',
                                 r'../preprocess_models/save_models_ALL_ganji_beijing/stringIndexer_modelzone'],
                            'stringIndexer_model_fangtianxia':
                                ['../preprocess_models/save_models_ALL_fangtianxia_beijing/stringIndexer_modelagency_name',
                                 '../preprocess_models/save_models_ALL_fangtianxia_beijing/stringIndexer_modeldirection',
                                 '../preprocess_models/save_models_ALL_fangtianxia_beijing/stringIndexer_modeldistrict',
                                 '../preprocess_models/save_models_ALL_fangtianxia_beijing/stringIndexer_modelpay_type',
                                 '../preprocess_models/save_models_ALL_fangtianxia_beijing/stringIndexer_modelrent_type',
                                 '../preprocess_models/save_models_ALL_fangtianxia_beijing/stringIndexer_modelzone'],
                            'stringIndexer_model_58':
                                [
                                 '../preprocess_models/save_models_ALL_58_beijingbaifenzhi20/stringIndexer_modeldirection',
                                 '../preprocess_models/save_models_ALL_58_beijingbaifenzhi20/stringIndexer_modeldistrict',
                                 '../preprocess_models/save_models_ALL_58_beijingbaifenzhi20/stringIndexer_modelpay_type',
                                 '../preprocess_models/save_models_ALL_58_beijingbaifenzhi20/stringIndexer_modelrent_type',
                                 '../preprocess_models/save_models_ALL_58_beijingbaifenzhi20/stringIndexer_modelzone']}


    # def transf_58(stringIndexer_model_path):
    #     direction = None
    #     zone = None
    #     rent_type = None
    #     district = None
    #     pay_type = None
    #
    #     input_data_58['facilities'] = newDataFacilities(input_data_58,facilities_vocab_58)
    #     tmp_dic = dict()
    #     for f in input_data_58['facilities']:
    #         index = facilities_vocab_58.index(f)
    #         tmp_dic[index] = 1.0
    #     facilities = SparseVector(len(facilities_vocab_58), tmp_dic)
    #
    #     for i in stringIndexer_model_path:
    #
    #         if 'direction' in i:
    #             labels_list = one_hot_labels_58['direction']
    #             length = len(labels_list)
    #             field_value = input_data_58['direction']
    #             if field_value not in labels_list:
    #                 value_index = labels_list.index(choice(labels_list))
    #             else:
    #                 value_index = labels_list.index(field_value)
    #             direction = SparseVector(length, {value_index: 1.0})
    #         elif 'zone' in i:
    #             labels_list = one_hot_labels_58['zone']
    #             length = len(labels_list)
    #             field_value = input_data_58['zone']
    #             if field_value not in labels_list:
    #                 value_index = labels_list.index(choice(labels_list))
    #             else:
    #                 value_index = labels_list.index(field_value)
    #             zone = SparseVector(length, {value_index: 1.0})
    #         elif 'rent_type' in i:
    #             labels_list = one_hot_labels_58['rent_type']
    #             length = len(labels_list)
    #             field_value = input_data_58['rent_type']
    #             if field_value not in labels_list:
    #                 value_index = labels_list.index(choice(labels_list))
    #             else:
    #                 value_index = labels_list.index(field_value)
    #             rent_type = SparseVector(length, {value_index: 1.0})
    #         elif 'district' in i:
    #             labels_list = one_hot_labels_58['district']
    #             length = len(labels_list)
    #             field_value = input_data_58['district']
    #             if field_value not in labels_list:
    #                 value_index = labels_list.index(choice(labels_list))
    #             else:
    #                 value_index = labels_list.index(field_value)
    #             district = SparseVector(length, {value_index: 1.0})
    #         elif 'pay_type' in i:
    #             labels_list = one_hot_labels_58['pay_type']
    #             length = len(labels_list)
    #             field_value = input_data_58['pay_type']
    #             if field_value not in labels_list:
    #                 value_index = labels_list.index(choice(labels_list))
    #             else:
    #                 value_index = labels_list.index(field_value)
    #             pay_type = SparseVector(length, {value_index: 1.0})
    #         else:
    #             pass
    #
    #
    #     new_data_58 = sc.parallelize(
    #         [Row(hall_num=input_data_58['hall_num'], toilet_num=input_data_58['toilet_num'],
    #              floor=input_data_58['floor'],decoration=input_data_58['decoration'],
    #              facilities_vectors=facilities, floor_total=input_data_58['floor_total'],
    #              is_broker=input_data_58['is_broker'],direction=direction, zone=zone,rent_type=rent_type,
    #              district=district, pay_type=pay_type,area=input_data_58['area'], room_num=input_data_58['room_num'],
    #              one_room_area=input_data_58['one_room_area'])], 15)
    #     return new_data_58


    def transf_gan_fang(stringIndexer_model_path,facilities_vocab,one_hot_labels):
        direction = None
        zone = None
        rent_type = None
        agency_name = None
        district = None
        pay_type = None

        input_data['facilities'] = newDataFacilities(input_data, facilities_vocab)
        tmp_dic = dict()
        for f in input_data['facilities']:
            index = facilities_vocab.index(f)
            tmp_dic[index] = 1.0
        facilities = SparseVector(len(facilities_vocab), tmp_dic)

        for i in stringIndexer_model_path:

            if 'direction' in i:
                labels_list = one_hot_labels['direction']
                length = len(labels_list)
                field_value = input_data['direction']
                if field_value not in labels_list:
                    value_index = labels_list.index(labels_list[0])
                else:
                    value_index = labels_list.index(field_value)
                direction = SparseVector(length, {value_index: 1.0})
            elif 'zone' in i:
                labels_list = one_hot_labels['zone']
                length = len(labels_list)
                field_value = input_data['zone']
                if field_value not in labels_list:
                    value_index = labels_list.index(labels_list[0])
                else:
                    value_index = labels_list.index(field_value)
                zone = SparseVector(length, {value_index: 1.0})
            elif 'rent_type' in i:
                labels_list = one_hot_labels['rent_type']
                length = len(labels_list)
                field_value = input_data['rent_type']
                if field_value not in labels_list:
                    value_index = labels_list.index(labels_list[0])
                else:
                    value_index = labels_list.index(field_value)
                rent_type = SparseVector(length, {value_index: 1.0})
            elif 'agency_name' in i:
                labels_list = one_hot_labels['agency_name']
                length = len(labels_list)
                field_value = input_data['agency_name']
                if field_value not in labels_list:
                    value_index = labels_list.index(labels_list[0])
                else:
                    value_index = labels_list.index(field_value)
                agency_name = SparseVector(length, {value_index: 1.0})
            elif 'district' in i:
                labels_list = one_hot_labels['district']
                length = len(labels_list)
                field_value = input_data['district']
                if field_value not in labels_list:
                    value_index = labels_list.index(labels_list[0])
                else:
                    value_index = labels_list.index(field_value)
                district = SparseVector(length, {value_index: 1.0})
            elif 'pay_type' in i:
                labels_list = one_hot_labels['pay_type']
                length = len(labels_list)
                field_value = input_data['pay_type']
                if field_value not in labels_list:
                    value_index = labels_list.index(labels_list[0])
                else:
                    value_index = labels_list.index(field_value)
                pay_type = SparseVector(length, {value_index: 1.0})
            else:
                pass

        new_data_x = sc.parallelize(
            [Row(hall_num=input_data['hall_num'], toilet_num=input_data['toilet_num'], floor=input_data['floor'],
             decoration=input_data['decoration'],facilities_vectors=facilities,
             floor_total=input_data['floor_total'],is_broker=input_data['is_broker'], direction=direction,
             zone=zone, rent_type=rent_type,agency_name=agency_name,district=district, pay_type=pay_type,
             area=input_data['area'],room_num=input_data['room_num'],one_room_area=input_data['one_room_area'])], 16)

        return new_data_x

    # path_list_58 = preprocess_model_path['stringIndexer_model_58']
    # new_data_58 = transf_58(path_list_58)

    # path_list_fangtianxia = preprocess_model_path['stringIndexer_model_fangtianxia']
    # input_data = input_data_fangtianxia
    # new_data_fangtianxia = transf_gan_fang(path_list_fangtianxia,facilities_vocab_fangtianxia,one_hot_labels_fangtianxia)


    input_data = input_data_ganji
    path_list_ganji = preprocess_model_path['stringIndexer_model_ganji']
    new_data_ganji = transf_gan_fang(path_list_ganji,facilities_vocab_ganji,one_hot_labels_ganji)


    # return new_data_58, new_data_fangtianxia, new_data_ganji
    return new_data_ganji

    sc.stop()
    spark.stop()



def loadModel(input_dict_data,path):
    os.environ['SPARK_HOME'] = '/root/spark-2.1.1-bin'

    sparkConf = SparkConf() \
        .setAppName('pyspark rentmodel') \
        .setMaster('local[*]')

    sc = SparkContext.getOrCreate(conf=sparkConf)

    sc.setLogLevel('ERROR')
    spark = SparkSession(sparkContext=sc)


    # new_data_58, new_data_fangtianxia, new_data_ganji = data_transform(input_dict_data, path)
    new_data_ganji = data_transform(input_dict_data, path)
    # print('new_data_58=========',new_data_58)
    # start11 = time.time()
    # new_data_58_df = spark.createDataFrame(new_data_58)
    # end11 = time.time()
    # print('end11 - start11', end11 - start11)

    # start22 = time.time()
    # new_data_fangtianxia_df = spark.createDataFrame(new_data_fangtianxia)
    # end22 = time.time()
    # print('spark.createDataFrame(new_data_fangtianxia):', end22 - start22)

    #start33 = time.time()
    new_data_ganji_df = spark.createDataFrame(new_data_ganji)
    #end33=time.time()
    #print('spark.createDataFrame(new_data_ganji):',end33 - start33)

    # start44 = time.time()
    # vecAssembler_58 = VectorAssembler(inputCols=new_data_58_df.columns, outputCol="features")
    # new_data_58_df = vecAssembler_58.transform(new_data_58_df)
    # end44 = time.time()
    # print('end44 - start44', end44 - start44)

    # start55 = time.time()
    # vecAssembler_fangtianxia = VectorAssembler(inputCols=new_data_fangtianxia_df.columns, outputCol="features")
    # new_data_fangtianxia_df = vecAssembler_fangtianxia.transform(new_data_fangtianxia_df)
    # end55 = time.time()
    # print('vecAssembler_fangtianxia:', end55 - start55)

    #start66 = time.time()
    vecAssembler_ganji = VectorAssembler(inputCols=new_data_ganji_df.columns, outputCol="features")
    new_data_ganji_df = vecAssembler_ganji.transform(new_data_ganji_df)
    #end66 = time.time()
    #print('vecAssembler_ganji:',end66 - start66)

    #start77 = time.time()
    loadmodel_ganji = GBTRegressionModel.load(path[0])
    # loadmodel_fangtianxia = GBTRegressionModel.load(path[1])
    # loadmodel_58 = RandomForestRegressionModel.load(path[2])
    #end77 = time.time()
    #print('load models:', end77 - start77)

    #start88 = time.time()
    predict_result_ganji = loadmodel_ganji.transform(new_data_ganji_df)
    #end88 = time.time()
    #print('predict_result_ganji:', end88 - start88)

    # start10 = time.time()
    # predict_result_fangtianxia = loadmodel_fangtianxia.transform(new_data_fangtianxia_df)
    # end10 = time.time()
    # print('predict_result_fangtianxia:', end10 - start10)

    # start99 = time.time()
    # predict_result_58 = loadmodel_58.transform(new_data_58_df)
    # end99 = time.time()
    # print('predict_result_58', end99 - start99)

    # return predict_result_ganji, predict_result_fangtianxia, predict_result_58
    return predict_result_ganji

    sc.stop()
    spark.stop()


def rentPricePredict(input_dict_data):
	start11 = time.time()

    # model_path = ['../preprocess_models/model_ALL_ganji_beijing_gbdt20',
    #               '../preprocess_models/model_ALL_fangtianxia_beijing_gbdt20',
    #               '../preprocess_models/model_ALL_58_beijing_baifenzhi20_rf19']

    # model_path = ['/root/my_django_api/my_django_api/utils_apt/preprocess_models/model_ALL_ganji_beijing_gbdt20',
    #              '/root/my_django_api/my_django_api/utils_apt/preprocess_models/model_ALL_fangtianxia_beijing_gbdt20',
    #              '/root/my_django_api/my_django_api/utils_apt/preprocess_models/model_ALL_58_beijing_baifenzhi20_rf19']
	
	model_path = ['/root/my_django_api/my_django_api/utils_apt/preprocess_models/model_ALL_ganji_beijing_gbdt20']

    # predict_result_ganji, predict_result_fangtianxia, predict_result_58 = loadModel(input_dict_data,model_path)
    predict_result_ganji = loadModel(input_dict_data, model_path)

    # result = []
    # # for i in [predict_result_ganji, predict_result_fangtianxia, predict_result_58]:
    #     try:
    #         num = i.count()
    #         if num == 1:
    #             row = i.select('prediction').collect()
    #             price = row[0]['prediction']
    #         else:
    #             pass
    #     except Exception as e:
    #         print('error:', e)
    #     finally:
    #         result.append(price)

    # mean_result = round(sum(result)/3, 2)

    try:
        num = predict_result_ganji.count()
        if num == 1:
            row = predict_result_ganji.select('prediction').collect()
            price = row[0]['prediction']
        else:
            pass
    except Exception as e:
        raise('error:', e)

    resp = {'predict_price': price}
    print('predict_price------------------',resp)
    end11 = time.time()
    print('resp:', end11 - start11)

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

    input_dict_data = {'agency_name':'北京影天', 'direction':'南', 'rent_type':'其他', 'district':'怀柔',
                       'pay_type':'押一付三', 'zone':'怀柔城区','facilities':'床|衣柜|沙发|电视|洗衣机|空调|热水器|宽带|暖气',
                       'decoration':'豪', 'floor':'高', 'area':50, 'room_num':1, 'hall_num':1,'toilet_num':1,
                       'floor_total':10, 'is_broker':0,'hahahha':'jheihi'}
    # df = rentPricePredict(input_dict_data)
    #
    model_path = ['../preprocess_models/model_ALL_ganji_beijing_gbdt20',
                  '../preprocess_models/model_ALL_fangtianxia_beijing_gbdt20',
                  '../preprocess_models/model_ALL_58_beijing_baifenzhi20_rf19']

    # data_transform(input_dict_data, model_path)
    rentPricePredict(input_dict_data)



    end = time.time()

    print('程序运行时间：', end - start)

    sc.stop()
    spark.stop()