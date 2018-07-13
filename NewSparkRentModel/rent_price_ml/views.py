# coding: utf-8

from __future__ import division
import json
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from libs.response_code import RET, error_map
from libs.log_tool import WriteLog
from apt import calc_apt, check_params_apt
from dist import calc_dist, check_params_dist


def calc_price(price_apt, price_dist):
    if price_apt and price_dist:
        rent_price = (price_apt + price_dist) / 2
    else:
        rent_price = price_apt or price_dist
    return rent_price


# Create your views here.
@require_POST
def predict_rent_price(request):
    '''
    利用模型估计房源价格
    :param request:
    :return:
    '''

    code = RET.OK
    rent_price_apt = None
    rent_price_dist = None
    rent_price = None

    try:
        data = json.loads(request.body).get('data', None)
        param_valid_apt = check_params_apt(data)
        param_valid_dist = check_params_dist(data)
        if param_valid_apt:
            rent_price_apt = calc_apt(data)
        if param_valid_dist:
            rent_price_dist = calc_dist(data)
        if not param_valid_apt and not param_valid_dist:
            code = RET.PARAMERR
        rent_price = calc_price(rent_price_apt, rent_price_dist)

    except Exception as e:
        WriteLog.write_error_log(e)
        code = RET.SERVERERR

    finally:
        desc = error_map[code]
        resp = {'status': {'code': code, 'description': desc},
                'result':
                    {'apt': rent_price_apt, 'dist': rent_price_dist, 'rent_price': rent_price}}
        response = JsonResponse(resp)
    return response
