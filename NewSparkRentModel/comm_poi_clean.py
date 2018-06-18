#! /usr/bin/python
# -* - coding: UTF-8 -* -

import pandas as pd
import numpy as np
import  pymongo

# client = MongoClient("mongodb://limeng:limeng@10.30.134.215:27016/")
# db = client.huifenqi_ext

mongo_client = pymongo.MongoClient(host="10.30.134.215", port=27016)
mongo_db = mongo_client["huifenqi_ext"]
# if not PRJ_DEBUG:
mongo_db.authenticate("limeng", "limeng", source='admin')

db = mongo_db["tmp_apt_baletu"]
temp = db.find({"city":"西安"},{"city":1}).count()
print(temp)

