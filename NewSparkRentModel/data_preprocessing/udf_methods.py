#!usr/bin/ python
#- * - coding:utf-8 - * -

"""
@author:limeng
@file: udf_methods.py
@time: 2018/05/{DAY}
"""

from resource.configing import config as cf
from resource.ganji_agency_name_config import agency_name_conf
from resource.ganji_zone_config import zone_conf



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
        direction_range = cf.get('uniformity_direction')

        try:
            if s in list(direction_range.keys()):
                return direction_range.get(s)
            else:
                return '其他'
        except Exception:
            return '其他'

    @staticmethod
    def udf_rentType(s):
        rent_type_range = cf.get('uniformity_rent_type')

        try:
            if s in rent_type_range:
                return s
            else:
                return '其他'
        except Exception:
            return '其他'

    @staticmethod
    def udf_room_type(s):
        room_type_range = cf.get('uniformity_room_type')
        try:
            if s in room_type_range:
                return s
            else:
                return '其他'
        except Exception:
            return '其他'


    @staticmethod
    def udf_payType(s):

        uniformity_rent_type_dict = cf.get('uniformity_pay_type')
        uniformity_rent_type_dict_keys = list(uniformity_rent_type_dict.keys())

        try:
            if s in uniformity_rent_type_dict_keys:
                return uniformity_rent_type_dict.get(s)
            else:
                return '其他'
        except Exception:
            return '其他'

    @staticmethod
    def udf_agencyName(s):
        uniformity_agencyName_dict = agency_name_conf.get('agency_name')
        uniformity_agencyName_dict_keys = list(uniformity_agencyName_dict.keys())

        try:
            if s in uniformity_agencyName_dict_keys:

                return uniformity_agencyName_dict.get(s)
            else:
                return '其他'
        except Exception:
            return '其他'

    @staticmethod
    def udf_zone(s):

        uniformity_zone_dict = zone_conf.get('zone')

        uniformity_zone_dict_keys = list(uniformity_zone_dict.keys())

        try:
            if s in uniformity_zone_dict_keys:
                return uniformity_zone_dict.get(s)
            else:
                return '其他'
        except Exception:
            return '其他'

    @staticmethod
    def udf_tranFacilities(s):

        try:
            if s == None:
                return '其他'

            else:
                if '独立卫生间' in s:
                    temp = s.replace('独立卫生间', '独卫')
                if '独立阳台' in s:
                    temp = temp.replace('独立阳台', '阳台')
                else:
                    temp = s
                temp = s.split('|')
                return temp
        except Exception:
            return '其他'








