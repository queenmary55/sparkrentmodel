# -*- coding: utf-8 -*-

from my_django_api.utils_apt.spark_rentmodel_interface import predict_main
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
import json


# Create your views here.
@csrf_exempt
def predict_rent_price(request):
    data = json.loads(request.body.decode('utf-8')).get('data', None)

    print('request_data-------------',type(data), data)

    # print('request.body=========', request.body)
    # request = {'agency_name': '北京影天', 'direction': '南', 'rent_type': '其他', 'district': '怀柔',
    #                    'pay_type': '押一付三', 'zone': '怀柔城区', 'facilities': '床|衣柜|沙发|电视|洗衣机|空调|热水器|宽带|暖气',
    #                    'decoration': '豪', 'floor': '高', 'area': 50, 'room_num': 1, 'hall_num': 1, 'toilet_num': 1,
    #                    'floor_total': 10, 'is_broker': 0, 'hahahha': 'jheihi'}
    resp = predict_main.rentPricePredict(data)

    response = JsonResponse(resp)
    print('response-----------',response)
    return response
    # return JsonResponse({"result": 0, "msg": "执行成功"})