import sys
from utils import *
import datetime
import json
import os
import csv
import pandas as pd
import multiprocessing
from utils import *
JSONPATH = 'fwd_analyse/'
ANAPATH = '/get_together_fwd/'


def ana_data(year):
    for month in range(1, 13):
        print(month)
        month = str(month)
        if len(month) < 2:
            month = '0' + month
        ana_data_everymonth(year, month)


def ana_data_everymonth(year, month):
    m_path = '/'.join([year, month])
    month_path = os.path.join(JSONPATH, m_path)
    if not os.path.exists(month_path):
        print(month_path, 'log missing')
    pool = multiprocessing.Pool(processes=6)
    fwdnodes = []
    for _node in range(18, 145):
        _node = str(_node)
        fwdnodes.append('.'.join(['20.0.2', _node]))
    for fwdnode in fwdnodes:
        fwdcsv_path = os.path.join(month_path, fwdnode, 'all.csv')
        pool.apply_async(ana_data_everyfwd(fwdcsv_path, fwdnode, year, month))

    pool.close()
    pool.join()


def ana_data_everyfwd(fwd_path, fwdnode, year, month):
    try:
        resdict = dict()
        st = '-'.join([year, month, '01'])
        if month != '12':
            tmp = str(int(month) + 1)
            et = '-'.join([year, tmp, '01'])
            day_list = get_day_list(st, et)[:-1]
        else:
            et = '-'.join([year, month, '31'])
            day_list = get_day_list(st, et)
        for day in day_list:
            resdict[day] = [0] * 24
        if os.path.exists(fwd_path):
            with open(fwd_path, 'r') as f:
                data = f.readlines()
            for d in data:
                dlist = d.split(',', maxsplit=5)
                htime = dlist[0].strip(" ['")
                htime_list = htime.split()
                hday = htime_list[0]
                htime = int((htime_list[1]).rsplit(':', maxsplit=2)[0])
                data_sumr_jud = float(dlist[3]) / (1024.0 * 1024.0)
                data_sumw_jud = float(dlist[4]) / (1024.0 * 1024.0)
                data_sum_jud = data_sumr_jud + data_sumw_jud
                high_range = 2.5 * 1024.0
                data_sumsecond = min(round(data_sum_jud / high_range, 4), 1.0)

                if hday not in resdict:
                    print(str(d), hday, 'non-month time')
                    continue
                if resdict[hday][htime] < data_sumsecond:
                    resdict[hday][htime] = data_sumsecond

        resdata = []
        for key, value in resdict.items():
            resdata.append(''.join(list(map(lambda x: str(x) + '\n', value))))
        write_path = os.path.join(year, fwdnode + '.csv')
        if not os.path.exists(year):
            try:
                os.makedirs(year)
            except Exception as e:
                pass
        with open(write_path, 'a') as f:
            f.write(''.join(resdata))
    except Exception as e:
        print('processing error')


if __name__ == '__main__':
    year = sys.argv[1]
    ana_data(year)
