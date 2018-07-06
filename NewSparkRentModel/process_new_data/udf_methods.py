#!usr/bin/ python
#- * - coding:utf-8 - * -


class UDFMethods(object):
    def __init__(self):
        pass

    @staticmethod
    def udf_NULL_assignZero(s):
        # s = s.strip().strip('\n').strip('\t')
        try:
            if s == 'NULL':
                return 0.0
            else:
                return s
        except Exception:
            return 0.0

    @staticmethod
    def udf_floor(s):
        # s = s.strip().strip('\n').strip('\t')
       try:
           if s == 0.0:
               pass
           elif s == '低':
               return float(1)
           elif s == '中':
               return float(2)
           elif s == '高':
               return float(3)
           else:
               return float(0)
       except Exception:
           return 0.0

    @staticmethod
    def udf_decoration(s):
        # s = s.strip().strip('\n').strip('\t')
        try:
            if s == 0.0:
                pass
            elif s == '毛坯':
                return float(0)
            elif s == '简':
                return float(2)
            elif s == '中':
                return float(3)
            elif s == '精':
                return float(4)
            elif s == '豪':
                return float(5)
            else:
                return float(0)
        except Exception:
            return 0.0

    @staticmethod
    def udf_direction(s):
        direction_range = {None:"其他","Null":"其他","北": "北", "西": "西", "朝西南": "西南", "朝北": "北", "朝西": "西",
                           "西南": "西南", "朝东南": "东南", "南北": "南北", "朝西北": "西北", "东南": "东南","东": "东",
                           "西北": "西北", "朝东北": "东北", "南": "南", "朝东": "东", "朝南": "南", "东北": "东北",
                           "东西": "东西"}

        try:
            if s in list(direction_range.keys()):
                return direction_range.get(s)
            else:
                return '其他'
        except Exception:
            return '其他'

    @staticmethod
    def udf_rentType(s):
        rent_type_range = ['整租','合租']

        try:
            if s in rent_type_range:
                return s
            else:
                return '其他'
        except Exception:
            return '其他'


    @staticmethod
    def udf_payType(s):

        uniformity_rent_type_dict = {None:"其他","Null":"其他","押三付六": "押三付半年","(押一付三 )":"押一付三",
            "可支持分期月付":"其他","押零付十二":"押零付年","付五":"押一付四","押0付11":"押零付十一",
            "(押一付一) 分期付,0压力 月供:436 元 利息:36 我要申请":"其他","(押一付半年)":"押一付半年","押三付十二":"押三付年",
            "付八":"押二付半年","(押一付一)":"押一付一","押2付5":"押二付五","季付":"押零付三","(押二付一) (快速贷款)":"押二付一",
            "付十一":"押零付十一","无押付半年":"押零付半年", "付七":"押一付半年","押二付十二":"押二付年",
            "(押一付一) (快速贷款)":"押一付一","押一付十二":"押一付年",
            "(押一付三 - 租房贷) 分期付,0压力 月供:835.67 元 利息:69 我要申请":"其他","半年付不押":"押零付半年",
            "押零付六":"押零付半年","押一付六":"押一付半年","(押二付一)":"押二付一","付一":"押零付一","付六":"押零付半年",
            "(押二付三)":"押二付三","(押一付三)":"押一付三","(押一付三) (快速贷款)":"押一付三","(押一付二)":"押一付二",
            "付四":"押一付三","付三":"押零付三","月付":"押零付一","半年付":"押零付半年","押零付一年":"押零付年",
            "(押一付年)":"押一付年","付十二":"押零付年","押一付半年peizhi[]":"押一付半年","押3付4":"押三付四",
            "(押一付三 - 租房贷)":"押一付三","押二付六":"押二付半年","付二":"押零付二","年付不押":"押零付半年","pay_type":"其他",
            "押一付一年":"押一付年","年付":"押零付年","押一年付":"押一付年","押一付一":"押一付一","押一付四":"押一付四",
            "押一付十一":"押一付十一","押一付三":"押一付三","押一付年":"押一付年","押一付二":"押一付二","押一付半年":"押一付半年",
            "押三付一":"押三付一","押三付三":"押三付三","押三付年":"押三付年","押三付二":"押三付二","押三付半年":"押三付半年",
            "押零付一":"押零付一","押零付三":"押零付三","押零付年":"押零付年","押零付二":"押零付二","押零付半年":"押零付半年",
            "押二付一":"押二付一","押二付四":"押二付四","押二付三":"押二付三","押二付年":"押二付年","押二付二":"押二付二",
            "押二付半年":"押二付半年","其他":"其他","年付":"押零付年","面议":"面议","不限":"不限","押零付半年":"押零付半年"}

        uniformity_rent_type_dict_keys = list(uniformity_rent_type_dict.keys())

        try:
            if s in uniformity_rent_type_dict_keys:
                return uniformity_rent_type_dict.get(s)
            else:
                return '其他'
        except Exception:
            return '其他'