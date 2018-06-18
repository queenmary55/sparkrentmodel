#! /usr/bin/python
# -* - coding: UTF-8 -* -

from pymongo import MongoClient
from pandas import DataFrame
import math

""""
    1、从mongodb中得到的是json格式的数据，需要解析为dataframe
    2、由于数据量较大，需要借助于生成器方法一批一批地处理
"""

def read_data(mongo_engine):
    count = mongo_engine.find({}).count()
    page = int(math.ceil(count / 10000))
    last_id = None
    for i in xrange(page):
        if i == 0:
            data = list(mongo_engine.find({}, {
                "_id":0,
                "address":1,
                "attribute":1,
                "attribute_num":1,
                "correlate_id":1,
                "crawl_time":1,
                "distance":1,
                'detail':1,
                "tag":1
            }).limit(10000))
        else:
            data = list(mongo_engine.find({'_id': {'$gt': last_id}}, {
                "_id": 0,
                "address": 1,
                "attribute": 1,
                "attribute_num": 1,
                "correlate_id": 1,
                "crawl_time": 1,
                "distance": 1,
                'detail': 1,
                "tag": 1
            }).limit(10000)
)
        if data:
            last_id = data[-1].get('_id')

        #使用生成器
        yield data

