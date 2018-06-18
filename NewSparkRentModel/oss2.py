#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: oss2.py
@time: 2018/05/{DAY}
"""

import os
import oss2

access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'LTAI47ht8Ecw2SZ0')

access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', '4C72EebkdiEo11iiNNj7LFhGID0lIG')

bucket_name = os.getenv('OSS_TEST_BUCKET', 'data-bigdata')

endpoint = os.getenv('OSS_TEST_ENDPOINT', 'oss-cn-beijing-internal.aliyuncs.com')

for param in (access_key_id, access_key_secret, bucket_name, endpoint):
    assert '<' not in param, '请设置参数：' + param

bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

key = 'rentmode_limeng/ganji_beijing_table.txt'

# Download
textfile = bucket.get_object(key).read()



