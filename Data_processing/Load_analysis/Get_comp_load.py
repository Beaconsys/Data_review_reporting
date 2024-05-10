import pymysql
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from datetime import datetime

global con
con = pymysql.connect(host='localhost',
                      user='root',
                      password='xxxxxx',
                      database='hpc_data')


# Get the utilization of compute nodes at hourly granularity
def get_utilization_com(year):
    sql = 'select jobid, nodenum,corenum, starttime,endtime,runtime from %d' % (
        year)
    data = pd.read_sql(sql, con)

    count = [[0] * 24 for _ in range(365)]
    n = len(data)
    for i in range(n):
        start_time = data['starttime'][i]
        end_time = data['endtime'][i]
        nodenum = data['nodenum'][i]
        corenum = data['corenum'][i]

        date_range = pd.date_range(start=start_time.floor('H'),
                                   end=end_time.ceil('H'),
                                   freq='H')
        # Calculate the number of seconds the program has run in each time point
        for j in range(len(date_range) - 1):
            # Calculate the time difference between the current point in time and the next point in time
            duration = date_range[j + 1] - date_range[j]

            # Determine whether the start time and end time have been crossed
            if date_range[j] < start_time:
                duration -= start_time - date_range[j]
            if date_range[j + 1] > end_time:
                duration -= date_range[j + 1] - end_time

            # Determine whether the start time and end time have been crossed
            st = str(year) + "-01-01"
            date1 = datetime.strptime(st, "%Y-%m-%d")
            date2 = date_range[j].strftime('%Y-%m-%d')
            date2 = datetime.strptime(str(date2), '%Y-%m-%d')
            delta = (date2 - date1).days
            hour = int(date_range[j].strftime('%H'))
            seconds = duration.seconds

            count[delta][hour] += seconds * (corenum / (nodenum * 4)) * nodenum

    for i in range(count):
        for j in range(count[i]):
            count[i][j] = count[i][j] / 3600 / 40960 / 4

    file_name = str(year) + "_utilization_comp" + '.pickle'
    with open(file_name, 'wb') as f:
        pickle.dump(count, f)


# Obtain the average hourly load rate of computing nodes from 2017 to 2022
def get_all_utilization_com_per_hour():
    sql = "select jobid, nodenum, corenum, starttime,endtime,runtime from job_2017_sw where starttime > '2017-04-01 " \
          "00:00:00' "
    data_2017 = pd.read_sql(sql, con)
    sql = "select jobid, nodenum, corenum,starttime,endtime,runtime from job_2018_sw"
    data_2018 = pd.read_sql(sql, con)
    sql = "select jobid, nodenum, corenum,starttime,endtime,runtime from job_2019_sw"
    data_2019 = pd.read_sql(sql, con)
    sql = "select jobid, nodenum, corenum,starttime,endtime,runtime from job_2020_sw"
    data_2020 = pd.read_sql(sql, con)
    sql = "select jobid, nodenum, corenum,starttime,endtime,runtime from job_2021_sw"
    data_2021 = pd.read_sql(sql, con)
    ql = "select jobid, nodenum, corenum,starttime,endtime,runtime from job_2022_sw"
    data_2022 = pd.read_sql(sql, con)

    data = pd.concat(
        [data_2017, data_2018, data_2019, data_2020, data_2021, data_2022])

    count = [0] * 24

    n = len(data)
    for i in range(n):
        start_time = data['starttime'][i]
        end_time = data['endtime'][i]
        nodenum = data['nodenum'][i]
        corenum = data['corenum'][i]
        time_range = pd.date_range(start_time.floor('H'),
                                   end_time.ceil('H'),
                                   freq='H')

        for j in range(len(time_range) - 1):
            curr_start = time_range[j]
            curr_end = time_range[j + 1]

            if curr_start <= start_time < curr_end:
                curr_start = start_time
            if curr_start <= end_time < curr_end:
                curr_end = end_time

            time_diff = (curr_end - curr_start).seconds
            count[curr_start.hour] += time_diff * (corenum /
                                                   (nodenum * 4)) * nodenum

    for i in range(count):
        count[i] = count[i] / 6 / 365 / 3600 / 40969 / 4
    with open("ut_comp_hour", 'wb') as f:
        pickle.dump(count, f)


# Statistics of failed jobs in different time zones
def get_abnormal_job_per_hour():
    sql = "select jobid, endtime,runtime, state from job_2017_sw where starttime > '2017-04-01'00:00:00' "
    data_2017 = pd.read_sql(sql, con)
    n = len(data_2017)
    Done_2017 = [0] * 24
    Exit_2017 = [0] * 24
    cnt_2017 = [0] * 24
    for i in range(n):
        try:
            endtime = data_2017['endtime'][i]
            state = data_2017['state'][i]
            runtime = data_2017['runtime'][i]
            hour = data_2017['endtime'][i].hour

            if state == 'Done':
                Done_2017[hour] += runtime
            else:
                Exit_2017[hour] += runtime
                cnt_2017[hour] += 1
        except:
            print('2017', i)

    sql = 'select jobid, endtime,runtime, state from job_2018_sw'
    data_2018 = pd.read_sql(sql, con)
    n = len(data_2018)
    Done_2018 = [0] * 24
    Exit_2018 = [0] * 24
    cnt_2018 = [0] * 24
    for i in range(n):
        try:
            endtime = data_2018['endtime'][i]
            state = data_2018['state'][i]
            runtime = data_2018['runtime'][i]
            hour = data_2018['endtime'][i].hour

            if state == 'Done':
                Done_2018[hour] += runtime
            else:
                Exit_2018[hour] += runtime
                cnt_2018[hour] += 1

        except:
            print('2018', i)

    sql = 'select jobid, endtime,runtime, state from job_2019_sw'
    data_2019 = pd.read_sql(sql, con)
    n = len(data_2019)
    Done_2019 = [0] * 24
    Exit_2019 = [0] * 24
    cnt_2019 = [0] * 24
    for i in range(n):
        try:
            endtime = data_2019['endtime'][i]
            state = data_2019['state'][i]
            runtime = data_2019['runtime'][i]
            hour = data_2019['endtime'][i].hour

            if state == 'Done':
                Done_2019[hour] += runtime
            else:
                Exit_2019[hour] += runtime
                cnt_2019[hour] += 1

        except:
            print('2019', i)

    sql = 'select jobid, endtime,runtime, state from job_2020_sw'
    data_2020 = pd.read_sql(sql, con)
    n = len(data_2020)
    Done_2020 = [0] * 24
    Exit_2020 = [0] * 24
    cnt_2020 = [0] * 24
    for i in range(n):
        try:
            start_time = data_2020['endtime'][i]
            state = data_2020['state'][i]
            runtime = data_2020['runtime'][i]
            hour = data_2020['endtime'][i].hour

            if state == 'Done':
                Done_2020[hour] += runtime
            else:
                Exit_2020[hour] += runtime
                cnt_2020[hour] += 1

        except:
            print('2020', i)

    sql = 'select jobid, endtime,runtime, state from job_2021_sw'
    data_2021 = pd.read_sql(sql, con)
    n = len(data_2021)
    Done_2021 = [0] * 24
    Exit_2021 = [0] * 24
    cnt_2021 = [0] * 24
    for i in range(n):
        try:
            start_time = data_2021['endtime'][i]
            state = data_2021['state'][i]
            runtime = data_2021['runtime'][i]
            hour = data_2021['endtime'][i].hour

            if state == 'Done':
                Done_2021[hour] += runtime
            else:
                Exit_2021[hour] += runtime
                cnt_2021[hour] += 1

        except:
            print('2021', i)

    sql = 'select jobid, endtime,runtime, state from job_2022_sw'
    data_2022 = pd.read_sql(sql, con)
    n = len(data_2022)
    Done_2022 = [0] * 24
    Exit_2022 = [0] * 24
    cnt_2022 = [0] * 24
    for i in range(n):
        try:
            start_time = data_2022['endtime'][i]
            state = data_2022['state'][i]
            runtime = data_2022['runtime'][i]
            hour = data_2022['endtime'][i].hour

            if state == 'Done':
                Done_2022[hour] += runtime
            else:
                Exit_2022[hour] += runtime
                cnt_2022[hour] += 1

        except:
            print('2021', i)

    Done_all = [0] * 24
    Exit_all = [0] * 24
    cnt_all = [0] * 24

    for i in range(24):
        Done_all[i] += Done_2017[i]
        Done_all[i] += Done_2018[i]
        Done_all[i] += Done_2019[i]
        Done_all[i] += Done_2020[i]
        Done_all[i] += Done_2021[i]
        Done_all[i] += Done_2022[i]

        Exit_all[i] += Exit_2017[i]
        Exit_all[i] += Exit_2018[i]
        Exit_all[i] += Exit_2019[i]
        Exit_all[i] += Exit_2020[i]
        Exit_all[i] += Exit_2021[i]
        Exit_all[i] += Exit_2022[i]

        cnt_all[i] += cnt_2017[i]
        cnt_all[i] += cnt_2018[i]
        cnt_all[i] += cnt_2019[i]
        cnt_all[i] += cnt_2020[i]
        cnt_all[i] += cnt_2021[i]
        cnt_all[i] += cnt_2022[i]

    with open('Done_all.pickle', 'wb') as f:
        pickle.dump(Done_all, f)
    with open('Exit_all.pickle', 'wb') as f:
        pickle.dump(Exit_all, f)
    with open('cnt_all.pickle', 'wb') as f:
        pickle.dump(cnt_all, f)


# Cause analysis of load distribution
def get_submitjob_num_nodenum_hour(year):
    sql = 'select jobid, corenum, starttime,runtime,corenum from %d' % (year)
    data = pd.read_sql(sql, con)
    n = len(data)
    count = [0] * 24
    coreenum = [[] for _ in range(24)]
    runtime = [[] for _ in range(24)]
    for i in range(n):
        try:
            start_time = data['starttime'][i]
            nodenum = data['nodenum'][i]
            runtime = data['runtime'][i]
            hour = data['starttime'][i].hour

            count[hour] += 1
            nodenum[hour].append(nodenum)
            runtime[hour].append(runtime)

        except:
            print(i)

    with open(str(year) + 'nodenum.pickle', 'wb') as f:
        pickle.dump(nodenum, f)
    with open(str(year) + 'runtime.pickle', 'wb') as f:
        pickle.dump(runtime, f)
    with open(str(year) + 'count.pickle', 'wb') as f:
        pickle.dump(count, f)


if __name__ == '__main__':
    for i in range(2017, 2023, 1):
        get_utilization_com(i)
        get_submitjob_num_nodenum_hour(i)
    get_all_utilization_com_per_hour()
    get_abnormal_job_per_hour()
