#! /usr/bin/python
# -* - coding: UTF-8 -* -

import numpy as np
import pandas as pd

from value_processing import address, distance


class AddFields(object):
    def __init__(self):
        pass
    @staticmethod
    def trans_fields(self,df):
        df["address_int"]= df['"address" '].apply(address)
        df["distance_dicr"] = df['"distance" '].apply(distance)
        return df


    #单个ObjectId
    @staticmethod
    def mult_to_one(self,temp_df,correlate_id):
        dataFrame = pd.DataFrame(None)
        dic1 = {}
        for i in set(temp_df['"attribute" ']):
            attr = temp_df[temp_df['"attribute" ']==i]
            k = i
            v = int(attr.shape[0])
            if k in dic1:
                    dic1.get(k).append(v)
            else:
                dic1.setdefault(k,[]).append(v)
        df_dic1 = pd.DataFrame(dic1)

        t = temp_df["address_int"].groupby(temp_df["distance_dicr"]).sum()
        dic = {}
        for j in range(len(t)):
            key = "distance" + str(t.index[j])
            value = int(t.values[j])
            if key in dic:
                    dic.get(key).append(value)
            else:
                dic.setdefault(key,[]).append(value)
        df_dic = pd.DataFrame(dic)

        comb_df_dic = pd.concat([df_dic1,df_dic],axis = 1)
        dataFrame = dataFrame.append(comb_df_dic)
        dataFrame["correlate_id"] = correlate_id
        return dataFrame

    @staticmethod
    def end_df(self,df):
        corr_id = list(set(df['"correlate_id" ']))
        end_df = pd.DataFrame(None)
        for j in corr_id:
            temp_df = df[df['"correlate_id" '] == j]
            temp_data = self.mult_to_one(temp_df,j)
            end_df = end_df.append(temp_data)

        end_df.index = range(end_df.shape[0])
        end_df.head()
        end_df.index = range(end_df.shape[0])
        return end_df

