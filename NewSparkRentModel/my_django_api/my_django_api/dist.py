# coding: utf-8

__author__ = 'limeng@huizhaofang.com'

from .utils_apt.spark_rentmodel_interface.predict_main import rentPricePredict


def calc_dist(data):
    '''

    :param data:
    :return: number
    '''

    resp = rentPricePredict(data)

    return resp


def check_params_dist(data):
    pass
