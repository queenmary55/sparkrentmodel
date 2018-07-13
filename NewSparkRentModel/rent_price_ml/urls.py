# coding: utf-8


from django.conf.urls import url, patterns
import views

urlpatterns = patterns(
    '',
    url(r'predict_rent_price', views.predict_rent_price)
)
