# -*- coding: utf-8 -*-

from my_django_api.utils_apt.spark_rentmodel_interface import predict_main
from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods



# Create your views here.
@csrf_exempt
def predict_rent_price(request_data):

    resp = predict_main.rentPricePredict(request_data)

    response = JsonResponse(resp)
    print('response-----------',response)
    return response