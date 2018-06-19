#! /usr/bin/python
# -* - coding: UTF-8 -* -


import pandas as pd
import query_mongodb
from RentModel import configing as cf
from resolving_json import ResolvingJson
from values_to_fields_df import AddFields

class Main(object):
    def __init__(self,mongo_engine):
        self.mongo_engine = mongo_engine

    def big_df(self):
        a = 0
        df = pd.DataFrame(None)
        data_gen = query_mongodb.read_data(self.mongo_engine)
        while 1:
            try:
                data = data_gen.next()
                temp_df1 = ResolvingJson(data).dic_to_df()
                temp_df2 = AddFields.trans_fields(temp_df1)
                temp_df3 = AddFields.end_df(temp_df2)

                #这样df会不会太大了，是否考虑在非测试的时候存入mysql中
                df = df.append(temp_df3)
                a += 10000
                print ('Finish %d rows' % a)
            except Exception as e:
                print(e)
                break
            else:
                return df


if __name__ == '__main__':
    main = Main(cf.get("mongo_engine"))
    df = main.big_df()
    df.to_csv(cf.get("save_df_path") + "all_poi.csv")

