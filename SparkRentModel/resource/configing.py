#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: configing.py
@time: 2018/04/{DAY}
"""


# from pymongo import MongoClient
from datetime import datetime
import numpy as np
config = {
    # 'mongo_engine' : MongoClient('127.0.0.1', 27016)['huifenqi_mining']['comm_poi_clean'],
    'save_df_path': "./",
    'now_date': datetime.now(),
    'null_no_processing': ['agency_name', 'direction','rent_type','district', 'pay_type','zone'], #'broker_id', 'agency_name','room_type',暂时不放入
    'null_processing_delete': ['price','custom_id'],
    'null_processing_assignMean': ['score','house_count',],
    'null_processing_byAlgorithmRF': 'area',
    'null_processing_property_fee': 'property_fee',
    'null_processing_assingNumber': ['floor','decoration'],
    'null_processing_assingNumber_complete_time': 'complete_time',
    'null_processing_assignZero': [' 医院',' 地铁',' 学校',' 旅游景点',' 酒店',' 银行',], # 汉字字段前有一个空格,可以在程序中使用strip函数去掉空格，这里就可以不用空格了，后续加上
    'null_processing_assignMode': ['building_count','toilet_num','hall_num',
                                    'is_broker','floor_total',' 休闲娱乐',' 公交',' 美食',' 购物',
                                    'distance1000','distance1500','distance2000','distance500',],
    'null_processing_assignRoomNum': 'room_num',
    'year_map_num_start': 1950,
    'year_map_num_end': datetime.now().year,
    # 'fill_replace_value' : 0,
    'fill_fillna_value': 0,
    'train_size_rate': 0.9,
    'uniformity_direction': ['东','南','西','北','东西','西东','南北','北南',
                              '东南','南东','东北','北东','西北','北西',np.nan],
    # 'non_uniformity_price' : ['面议','区间的情况、、、'], # 暂时不考虑做
    'uniformity_floor_total_max': 100,
    'uniformity_room_type': ['主','隔断','单租','次'],# 1、目前考虑使用one-hot,可以考虑制定数字变为有序值。2、问问爬虫时做过这个处理没有：把“次卧”转换“次”，“主卧”转换成“主”。如果没有，需要做这一步操作
    'uniformity_fields': ['direction','floor_total','is_broker','rent_type','room_type','pay_type',
                          'price','score','house_count','area','agency_name','zone'],

    'uniformity_rent_type': ['整租','合租'],

    # keys是原始值
    'uniformity_pay_type': {"押三付六": "押三付半年","(押一付三 )":"押一付三","可支持分期月付":"其他","押零付十二":"押零付年","付五":"押一付四","押0付11":"押零付十一",
    "(押一付一) 分期付,0压力 月供:436 元 利息:36 我要申请":"其他","(押一付半年)":"押一付半年","押三付十二":"押三付年","付八":"押二付半年",
    "(押一付一)":"押一付一","押2付5":"押二付五","季付":"押零付三","(押二付一) (快速贷款)":"押二付一","付十一":"押零付十一","无押付半年":"押零付半年",
    "付七":"押一付半年","押二付十二":"押二付年","(押一付一) (快速贷款)":"押一付一","押一付十二":"押一付年",
    "(押一付三 - 租房贷) 分期付,0压力 月供:835.67 元 利息:69 我要申请":"其他",
    "半年付不押":"押零付半年","押零付六":"押零付半年","押一付六":"押一付半年","(押二付一)":"押二付一","付一":"押零付一","付六":"押零付半年",
    "(押二付三)":"押二付三","(押一付三)":"押一付三","(押一付三) (快速贷款)":"押一付三","(押一付二)":"押一付二","付四":"押一付三","付三":"押零付三",
    "月付":"押零付一","半年付":"押零付半年","押零付一年":"押零付年","(押一付年)":"押一付年","付十二":"押零付年","押一付半年peizhi[]":"押一付半年",
    "押3付4":"押三付四","(押一付三 - 租房贷)":"押一付三","押二付六":"押二付半年","付二":"押零付二","年付不押":"押零付半年","pay_type":"其他",
    "押一付一年":"押一付年","年付":"押零付年","押一年付":"押一付年",
    "押一付一":"押一付一","押一付四":"押一付四","押一付十一":"押一付十一","押一付三":"押一付三","押一付年":"押一付年","押一付二":"押一付二","押一付半年":"押一付半年",
    "押三付一":"押三付一","押三付三":"押三付三","押三付年":"押三付年","押三付二":"押三付二","押三付半年":"押三付半年","押零付一":"押零付一","押零付三":"押零付三",
    "押零付年":"押零付年","押零付二":"押零付二","押零付半年":"押零付半年","押二付一":"押二付一","押二付四":"押二付四","押二付三":"押二付三","押二付年":"押二付年",
    "押二付二":"押二付二","押二付半年":"押二付半年","其他":"其他","年付":"押零付年","面议":"面议","不限":"不限","押零付半年":"押零付半年",
    },

    'uniformity_decoration': {"豪": 6, "豪装": 6, "简": 3, "简装": 3, "精": 5, "精装": 5, "毛坯": 1, "普": 2, "普装": 2, "未知": 0, "中": 4, "中装": 4},
    'uniformity_direction': {"北": "北", "西": "西", "朝西南": "西南", "朝北": "北", "朝西": "西", "西南": "西南", "朝东南": "东南", "南北": "南北", "朝西北": "西北", "东南": "东南",
    "东": "东", "西北": "西北", "朝东北": "东北", "南": "南", "朝东": "东", "朝南": "南", "东北": "东北", "东西": "东西"},
}