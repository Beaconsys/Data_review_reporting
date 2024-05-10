import pymysql
import pandas as pd
import pickle
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

global con

con = pymysql.connect(host='localhost',
                      user='root',
                      password='xxxxxx',
                      database='xxxxxx')

NUM_CLUSTER = 20


# Obtain all jobs' I/O performance
def get_all_app():
    sql = 'select avg_iobw_r, avg_iops_r, avg_mdops, avg_iobw_w, avg_iops_w,data_sum_r,data_sum_w from job_io_info'
    app_io = pd.read_sql(sql, con)
    app_io = app_io[app_io['data_sum_r'] + app_io['data_sum_w'] >= 200]
    sql = 'select IOBW_READ_AVERAGE , IOPS_READ_AVERAGE, MDS_OPEN_AVERAGE ,MDS_CLOSE_AVERAGE , IOBW_WRITE_AVERAGE , IOPS_WRITE_AVERAGE,IOBW_READ_SUM,IOBW_WRITE_SUM from job_io_info_1'
    app_io_1 = pd.read_sql(sql, con)
    app_io_1 = app_io_1[app_io_1['IOBW_READ_SUM'] +
                        app_io_1['IOBW_WRITE_SUM'] >= 200]
    app_io_1['avg_mdops'] = app_io_1['MDS_OPEN_AVERAGE'] + app_io_1[
        'MDS_CLOSE_AVERAGE']
    app_io_1 = app_io_1.rename(
        columns={
            'IOBW_READ_AVERAGE': 'avg_iobw_r',
            'IOPS_READ_AVERAGE': 'avg_iops_r',
            'IOBW_WRITE_AVERAGE': 'avg_iobw_w',
            'IOPS_WRITE_AVERAGE': 'avg_iops_w'
        })
    app_io_1 = app_io_1[[
        'avg_iobw_r', 'avg_iops_r', 'avg_mdops', 'avg_iobw_w', 'avg_iops_w'
    ]]
    app_io = app_io[[
        'avg_iobw_r', 'avg_iops_r', 'avg_mdops', 'avg_iobw_w', 'avg_iops_w'
    ]]
    data = pd.concat([app_io, app_io_1])
    mask = data.any(axis=1)
    data = data.loc[mask]
    data.to_csv('io_data_3dim_1.csv', index=False)


#Obtain top applications' I/O performance data
def get_app_perf(program):
    sql = "select jobid,cnc,core,io_time,data_sum_r,data_sum_w,avg_iobw_w,avg_iobw_r, io_node, avg_request_size" \
          "avg_iops_r,avg_iops_w,avg_mdops,file_count,io_mode, from job_io_info where program  LIKE '%s'" %(program)
    app = pd.read_sql(sql, con)
    app = app[app['io_time'] != 0]
    grouped = app.groupby(['core', 'io_mode'])
    app_list = []
    for name, group in grouped:
        app_list.append(pd.DataFrame(group))

    for i in range(len(app_list)):
        app = app_list[i]
        X = app[[
            'cnc', 'avg_iops_r', 'avg_iops_w', 'avg_mdops', 'file_count',
            'avg_iops_w', 'avg_mdops', 'file_count', 'avg_request_size'
        ]]
        #Use for match jobs with similar I/O bahaviors
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        kmeans = KMeans(n_clusters=NUM_CLUSTE20)
        kmeans.fit(X_scaled)
        app['cluster'] = kmeans.labels_
        iobw_r_list = []
        iobw_w_list = []
        for j in range(NUM_CLUSTE20):
            cluster = app[app['cluster'] == j]
            for k in range(cluster):
                iobw_r_list.append(cluster['avg_iobw_r'][k])
                iobw_w_list.append(cluster['avg_iobw_w'][k])
        with open(str(program) + str(j) + 'iobw_r.pickle', 'wb') as f:
            pickle.dump(iobw_r_list, f)
        with open(str(program) + str(j) + 'iobw_w.pickle', 'wb') as f:
            pickle.dump(iobw_w_list, f)


if __name__ == '__main__':
    top_apps = [
        'CESM', 'WRF', 'VASP', 'SWCHECK', 'GRAPES', 'WW3', 'COAWSTM', 'LINGO',
        'NSSOLVER', 'PMCL3D'
    ]
    for program_name in top_apps:
        get_app_perf(program_name)
    get_all_app()
