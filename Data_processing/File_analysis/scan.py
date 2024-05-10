import os
import pandas as pd
import pymysql
import csv
import MySQLdb
import numpy as np
HOST = 'localhost'
USER = 'root'
PASSWD = 'xxxxxx'
DATABASE = 'job_info'
PORT = 3306
#from draw_contract import *


def dosql(sql):
    conn = MySQLdb.connect(host='localhost',
                           user=USER,
                           passwd=PASSWD,
                           database=DATABASE,
                           port=PORT)
    cur = conn.cursor()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return result


def list_tables():
    sql = '''show tables'''
    tables = dosql(sql)
    return tables


def list_fields(table):
    sql = '''desc %s''' % table
    return dosql(sql)


def contrast_sum_distr():
    tables = list_tables()
    tmp = ['', '']
    count = 0
    for t in tables:
        for table in t:
            tlist = table.split('_')
            if len(tlist[1]) != 4:
                if table == 'job_total_sw':
                    sql = '''select count(*) from %s''' % table
                    res = int(str((dosql(sql))).strip("(),"))
                    print(table, res)
                continue
            if not (table.endswith('sw') or table == 'job_total_sw'):
                if table == 'job_total_sw':
                    sql = '''select count(*) from %s''' % table
                    res = int(str((dosql(sql))).strip("(),"))
                    print(table, res)
                continue
            sql = '''select count(*) from %s''' % table
            res = int(str((dosql(sql))).strip("(),"))
            print(table, res)
            count += res
    print('各年总数', count)


def contrast_fields():
    tables = list_tables()
    tmp = ['', '']
    count = 0
    res = {}
    for t in tables:
        for table in t:
            print(table)
            tlist = table.split('_')
            if len(tlist[1]) != 4:
                continue
            if not (table.endswith('sw') or table == 'job_total_sw'):
                continue
            sql = 'select corenum,corehour,runtime,state from %s' % table
            yeardata = dosql(sql)
            res[table] = yeardata
            with open(table + '.csv', 'w') as f:
                writer = csv.writer(f)
                for line in yeardata:
                    writer.writerow(line)


def contrast_years():
    res = {}
    #_fps = os.listdir()
    _fps = ['job_' + str(x) + '_sw.csv' for x in range(2017, 2023)]
    print(_fps)
    resultsum = []
    resultavg = []
    for fp in _fps:
        resyearsum = [0, 0, 0]
        resyearavg = [0, 0, 0]
        if not fp.endswith('.csv'):
            continue
        with open(fp, 'r') as f:
            data = f.readlines()
        fkey = fp.split('.')[0]
        fvalue = list(map(lambda x: [float(y) for y in x.split(',')[:3]],
                          data))
        #        + [x.rsplit(',', maxsplit=1)[-1]], data))
        fvalue = np.array(fvalue)
        for i in range(3):
            resyearsum[i] = round(np.sum(fvalue[:, i]), 2)
            resyearavg[i] = round(np.average(fvalue[:, i]), 2)
        jobyearcount = len(data)
        resyearsum.append(jobyearcount)
        resyearavg.append(jobyearcount)
        print(resyearsum)
        print(resyearavg)
        res[fkey] = [resyearsum, resyearavg]
        resultsum.append(resyearsum)
        resultavg.append(resyearavg)
        print(res)

    resultsum = np.array(resultsum).transpose()
    resultavg = np.array(resultavg).transpose()
    headsum = ['CORE_SUM', 'COREHOUR_SUM', 'RUN_TIME_SUM', 'JOB_COUNT']
    headavg = ['CORE_AVG', 'COREHOUR_AVG', 'RUN_TIME_AVG', 'JOB_COUNT']
    with open('yearsum_contrast_drawdata.csv', 'w') as f:
        writer = csv.writer(f)
        for sdata in resultsum:
            writer.writerow(sdata)
    with open('yearavg_contrast_drawdata.csv', 'w') as f:
        writer = csv.writer(f)
        for sdata in resultavg:
            writer.writerow(sdata)

    draw_contrast(resultsum, 'year_contrast_sum', headsum)
    draw_contrast(resultavg, 'year_contrast_avg', headavg)


def find_yearids(year):
    tablename = 'job_' + year + '_sw'
    sql = '''select min(jobid+0),max(jobid+0) from %s''' % tablename
    ids = dosql(sql)
    id7 = []
    id8 = []
    for tup in ids:
        id_low = tup[0]
        id_high = tup[1]
    return [int(year), int(id_low), int(id_high)]


year = '2021'
sql = '''select jobid,starttime,endtime,nodelist from %s where corenum >= 16''' % 'job_2021_sw'
yeardata = dosql(sql)
with open('2021_jobs.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    for line in yeardata:
        writer.writerow(line)
