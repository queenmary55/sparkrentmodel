#! /usr/bin/python
# -* - coding: UTF-8 -* -

import numpy as np
import pandas as pd

"""
拆分地址便于统计其数量
"""

def address(s):
    length = len(s.split(";"))
    return length


"""
把距离离散化
"""
def distance(num):
    if num <= 500:
        num = 500
    elif num >500 and num <=1000:
        num = 1000
    elif num >1000 and num <= 1500:
        num = 1500
    elif num > 1500:
        num = 2000
    else:
        pass
    return num