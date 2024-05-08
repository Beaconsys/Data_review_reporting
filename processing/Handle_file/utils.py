import sys
import os
from collections import defaultdict
import datetime
import redis
import threading
from functools import reduce
import re, math
import csv
import multiprocessing
import json


def str2time(timeStr):
    """
    format time
    :param timeStr: time string
    :return: %Y-%m-%d %H:%M:%S
    """
    return datetime.datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S")


def time2str(timestamp):
    """
    timestamp to time string
    :param timestamp: timestamp
    :return: string
    """
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def minus(x, y):
    """
    two list(tuple) elements are minus to generate a new list
     x and y is list or tuple, their length is same.
    :param x: tuple or list
    :param y: tuple or list
    :return: []
    """
    r = []
    for i, j in zip(x, y):
        minus_result = i - j
        r.append(minus_result)
    return r


def division(x, d):
    """
    each item in the list is divided by the divisor
    :param x: tuple or list
    :param d: divided
    :return: []
    """
    if d > 0:
        r = [round2decimal(v / d) for v in x]
    else:
        r = x
    return r


def round2decimal(num):
    """
    format float with 2 decimal points
    :param num: float number
    :return:
    """
    return round(num, 2)


def add(x, y):
    """
     two list(tuple) elements are added to generate a new list
     x and y is list or tuple, their length is same.
    :param x: tuple or list
    :param y: tuple or list
    :return: []
    """
    r = []
    for i, j in zip(x, y):
        i = float(i) if isinstance(i, str) else i
        j = float(j) if isinstance(j, str) else j
        add_result = i + j
        add_result = round2decimal(add_result)
        r.append(add_result)
    return r


class HandleDtaThread(threading.Thread):
    def __init__(self, func, args):
        super(HandleDtaThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


def get_day_list(st, et):
    st = datetime.datetime.strptime(st, '%Y-%m-%d')
    et = datetime.datetime.strptime(et, '%Y-%m-%d')
    day = datetime.timedelta(days=1)
    res = []
    while st <= et:
        res.append(st.strftime('%Y-%m-%d'))
        st += day
    return res


def utc2local(utcTime):
    """
    convert UTC to CST
    :param utcTime:
    :return:
    """
    utc_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    _datet = datetime.datetime.strptime(utcTime, utc_format)
    localtime = (_datet +
                 datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
    return localtime


def get_day_path(day):
    path = day.replace('-', '/')
    return path
