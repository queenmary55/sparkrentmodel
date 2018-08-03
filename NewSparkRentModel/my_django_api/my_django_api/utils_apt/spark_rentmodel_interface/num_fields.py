#!usr/bin/ python
# - * - coding:utf-8 - * -


def numFields(tmp_input_dict_data,key):
    try:
        if tmp_input_dict_data[key] == None:
            tmp_input_dict_data[key] = 0.0
    except Exception as e:
        tmp_input_dict_data[key] = 0.0

    return tmp_input_dict_data
