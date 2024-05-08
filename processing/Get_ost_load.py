import sys
from utils import *
import datetime
import json
import os
import csv
import pandas as pd
JSONPATH = 'ost_analyse/'
ANAPATH = '/ost_hour_ana/'
#with open(path, 'r') as f:


def ana_data(st, et):
    days = get_day_list(st, et)
    for day in days:
        if day.endswith('0'):
            print(day)
        ana_data_everyday(day)


def ana_data_everyday(day):
    d_path = day.replace('-', '/')
    day_path = os.path.join(JSONPATH, d_path)
    if not os.path.exists(day_path):
        return
    for ostjson in os.listdir(day_path):
        ana_data_everyost(day_path, ostjson)


def ana_data_everyost(day_path, ostjson):
    res = {}
    ost = ostjson.split('.')[0]
    ostpath = os.path.join(day_path, ostjson)
    data = pd.read_csv(ostpath)
    for d in data.itertuples():
        d = d[1:]
        htime = d[0]
        htime = htime.rsplit(':', maxsplit=2)[0]
        htime += ':00:00'
        data_sumr_jud = int(d[3]) / (1024.0 * 1024.0)
        data_sumw_jud = int(d[4]) / (1024.0 * 1024.0)
        data_sumr = 0 if data_sumr_jud > 400 else data_sumr_jud
        data_sumw = 0 if data_sumw_jud > 400 else data_sumw_jud
        data_sum = data_sumr + data_sumw
        if htime not in res:
            res[htime] = []
        res[htime].append(data_sum)
    anadata = handle_hour_ost(res)
    if ostjson == '000b.json':
        print(anadata)
    anapath = os.path.join(ANAPATH, ostjson)
    with open(anapath, 'a', newline='') as f:
        writer = csv.writer(f)
        for hourdata in anadata:
            writer.writerow(hourdata)


def handle_hour_ost(res):
    anadata = []
    for key, values in res.items():
        htime = key
        peak = 300
        realpeak = max(values)
        volumn = sum(values)
        load1 = round(volumn / (peak * 3600), 4)
        low_edge = 0.3 * peak
        mid_edge = 0.5 * peak
        high_edge = 0.7 * peak
        load2tmp = 0
        for iobw in values:
            if iobw <= low_edge:
                load2tmp += 0.15
            elif iobw <= mid_edge:
                load2tmp += 0.3
            elif iobw <= high_edge:
                load2tmp += 0.6
            else:
                load2tmp += 0.85
        load2 = round(load2tmp / 1800.0, 4)
        load3 = round(min(realpeak / peak, 1.00), 4)
        anadata.append([htime, load1, load2, load3])
    return anadata


if __name__ == '__main__':
    st = sys.argv[1]
    et = sys.argv[2]
    if not os.path.exists(ANAPATH):
        os.mkdir(ANAPATH)
    ana_data(st, et)
