#! /usr/bin/python
# -*- conding: UTF-8 -*-
import pandas as pd

"""
    1、从mongodb中查询到的数据是json格式，需要解析为dataframe的形式
    2、首先将一个花括号解析为一行
    3、再将得到的列表转换为字典的形式，因为字典很好转换为dataframe
"""
class ResolvingJson(object):
    def __init__(self,lists):
        self.lists = lists

    # 把每一个花括号内的内容取出来作为列表的一行
    def multrow_to_singlerow(self):
        lines = self.lists
        lst = []
        left = 0
        right = 0
        for line in lines:
            if line == "{\n":
                left = lines.index("{\n")
                del lines[left]

            elif line == "}\n":
                right = lines.index("}\n")
                del lines[right]
                temp = lines[left:right]
                lst.append(temp)

        lst.append(lines[left:len(lines) - 1])  # 把最后一个没有读取到的字典加进来
        return lst



    # 把lst列表中的每一个行解析为一个字典
    def lst_to_dic(self):
        lst = self.multrow_to_singlerow()
        dic = {}
        for i in lst:
            for items in i:
                items = items.strip("\n")
                items = items.strip(",")
                items = items.strip()  # lstrip()是去除左边空格，rstrip()是去除右边空格，strip是去除两边的空格
                items_split = items.split(":")
                #         if (len(items_split) !=2) and (len(items_split) !=4):
                #             print(len(items_split),items_split)
                if len(items_split) == 0:
                    key = None
                    value = None

                elif len(items_split) == 1:
                    key = items_split[0]
                    value = None

                elif len(items_split) == 2:
                    key = items_split[0]
                    value = items_split[1]

                else:  # "create_time"和"update_time"由于有多个单引号，造成了len(items_split)==4
                    key = items_split[0]
                    multCols = items_split[1:]
                    multValues = ""
                    for col in multCols:
                        multValues += str(col)
                    value = multValues

                if key in dic:
                    dic.get(key).append(value)
                else:
                    dic.setdefault(key, []).append(value)
        return dic

    @staticmethod
    def dic_to_df(self):
        dic = self.lst_to_dic()
        df = pd.DataFrame(dic)

        #去掉列名前的空格，列名有双层引号，去掉一层
        columns = []
        for i in df.columns:
            temp = i[1:len(i)-2]
            columns.append(temp)

        return df
# if __name__ == '__main__':
#     df = ResolvingJson.dic_to_df()
#     df.to_csv(cf.save_df_path)

