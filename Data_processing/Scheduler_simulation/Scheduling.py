import pandas as pd

def preemptive(data, total_nodes):
    curr_time = data.iloc[0, 2]
    waitingtime_list = []
    waiting_queue = []
    running_queue = []

    while len(data) != 0 or len(waiting_queue) != 0 or len(running_queue) != 0:
        # 1.dataframe
        while len(data) != 0 and curr_time == data.iloc[0][2]:
            id, nodenum, submittime, runtime = data.iloc[0][0], data.iloc[0][1], data.iloc[0][2], data.iloc[0][3]
            if len(waiting_queue) == 0 or nodenum > waiting_queue[-1][1]:
                waiting_queue.append([id, nodenum, submittime, runtime])
            else:
                for i in range(len(waiting_queue)):
                    if nodenum <= waiting_queue[i][1]:
                        waiting_queue.insert(i, [id, nodenum, submittime, runtime])
                        break

            data = data.iloc[1:]

        while len(waiting_queue) != 0 and waiting_queue[0][1] <= total_nodes:
            id, nodenum, submittime, runtime = waiting_queue[0][0], waiting_queue[0][1], waiting_queue[0][2], waiting_queue[0][3]
            endtime = curr_time + runtime
            waitingtime = curr_time - submittime
            total_nodes -= nodenum
            waiting_queue = waiting_queue[1:]
            waitingtime_list.append([id, waitingtime])

            if len(running_queue) == 0 or endtime >= running_queue[-1][0]:
                running_queue.append([endtime, nodenum])
            else:
                for i in range(len(running_queue)):
                    if endtime <= running_queue[i][0]:
                        running_queue.insert(i, [endtime, nodenum])
                        break

        while len(running_queue) != 0 and running_queue[0][0] == curr_time:
            total_nodes += running_queue[0][1]
            running_queue = running_queue[1:]

        curr_time += 1

    return waitingtime_list

def FCFS(data, total_nodes):
    curr_time = data.iloc[0, 2]
    waitingtime_list = []
    waiting_queue = []
    running_queue = []

    while len(data) != 0 or len(waiting_queue) != 0 or len(running_queue) != 0:
        # 1.dataframe
        while len(data) != 0 and curr_time == data.iloc[0, 2]:
            id, nodenum, submittime, runtime = data.iloc[0][0], data.iloc[0][1], data.iloc[0][2], data.iloc[0][3]
            waiting_queue.append([id, nodenum, submittime, runtime])
            data = data.iloc[1:]

        # 2.waiting_queue
        while len(waiting_queue) != 0 and waiting_queue[0][1] <= total_nodes:
            id, nodenum, submittime, runtime = waiting_queue[0][0], waiting_queue[0][1], waiting_queue[0][2], \
                                               waiting_queue[0][3]
            endtime = curr_time + runtime
            waitingtime = curr_time - submittime
            total_nodes -= nodenum
            waiting_queue = waiting_queue[1:]
            waitingtime_list.append([id, waitingtime])

            if len(running_queue) == 0 or endtime >= running_queue[-1][0]:
                running_queue.append([endtime, nodenum])
            else:
                for i in range(len(running_queue)):
                    if endtime <= running_queue[i][0]:
                        running_queue.insert(i, [endtime, nodenum])
                        break

        # 3.running_queue
        while len(running_queue) != 0 and running_queue[0][0] == curr_time:
            total_nodes += running_queue[0][1]
            running_queue = running_queue[1:]

        curr_time += 1
    return waitingtime_list

def Priority_Scheduling(data, total_nodes):
    curr_time = data.iloc[0, 2]
    waitingtime_list = []
    waiting_queue_first = []
    waiting_queue = []
    running_queue = []
    threshold = 20000

    while len(data) != 0 or len(waiting_queue_first) != 0 or len(waiting_queue) != 0 or len(running_queue) != 0:
        # 1.dataframe
        while len(data) != 0 and curr_time == data.iloc[0][2]:
            id, nodenum, submittime, runtime = data.iloc[0][0], data.iloc[0][1], data.iloc[0][2], data.iloc[0][3]
            if len(waiting_queue) == 0 or nodenum > waiting_queue[-1][1]:
                waiting_queue.append([id, nodenum, submittime, runtime])
            else:
                for i in range(len(waiting_queue)):
                    if nodenum <= waiting_queue[i][1]:
                        waiting_queue.insert(i, [id, nodenum, submittime, runtime])
                        break

            data = data.iloc[1:]

        # 2. waiting_queue
        while len(waiting_queue_first) == 0 and len(waiting_queue) != 0:
            if waiting_queue[0][1] <= total_nodes:
                id, nodenum, submittime, runtime = waiting_queue[0][0], waiting_queue[0][1], waiting_queue[0][2], \
                                                   waiting_queue[0][3]
                endtime = curr_time + runtime
                waitingtime = curr_time - submittime
                total_nodes -= nodenum
                waiting_queue = waiting_queue[1:]
                waitingtime_list.append([id, waitingtime])

                if len(running_queue) == 0 or endtime > running_queue[-1][0]:
                    running_queue.append([endtime, nodenum])
                else:
                    for i in range(len(running_queue)):
                        if endtime <= running_queue[i][0]:
                            running_queue.insert(i, [endtime, nodenum])
                            break
            # add waiting_queue_first
            else:
                tmp = []
                for i in range(len(waiting_queue)):
                    id, nodenum, submittime, runtime = waiting_queue[0][0], waiting_queue[0][1], waiting_queue[0][2], \
                                                       waiting_queue[0][3]
                    if (curr_time - submittime) >= threshold:
                        waiting_queue_first.append([id, nodenum, submittime, runtime])
                        tmp.append(i)
                tmp = sorted(tmp, reverse=True)
                if len(tmp) == 0:
                    break
                for index in tmp:
                    del waiting_queue[index]

        # 3. waiting_queue_first
        while len(waiting_queue_first) != 0 and waiting_queue_first[0][1] <= total_nodes:
            id, nodenum, submittime, runtime = waiting_queue_first[0][0], waiting_queue_first[0][1], waiting_queue_first[0][2], \
                                               waiting_queue_first[0][3]
            endtime = curr_time + runtime
            waitingtime = curr_time - submittime
            total_nodes -= nodenum
            waiting_queue_first = waiting_queue_first[1:]
            waitingtime_list.append([id, waitingtime])

            if len(running_queue) == 0 or endtime > running_queue[-1][0]:
                running_queue.append([endtime, nodenum])
            else:
                for i in range(len(running_queue)):
                    if endtime <= running_queue[i][0]:
                        running_queue.insert(i, [endtime, nodenum])
                        break

        # 4. running_queue
        while len(running_queue) != 0 and running_queue[0][0] == curr_time:
            total_nodes += running_queue[0][1]
            running_queue = running_queue[1:]

        curr_time += 1

    return waitingtime_list

import pandas as pd

def easy_backfilling(data, total_nodes):
    total = total_nodes
    curr_time = data.iloc[0, 2]
    waitingtime_list = []
    waiting_queue = []
    running_queue = []

    while len(data) != 0 or len(waiting_queue) != 0 or len(running_queue) != 0:
        # 1.dataframe
        while len(data) != 0 and curr_time == data.iloc[0, 2]:
            id, nodenum, submittime, runtime = data.iloc[0][0], data.iloc[0][1], data.iloc[0][2], data.iloc[0][3]

            # can_schedule_immediately
            flag = False
            if len(waiting_queue) != 0 and nodenum <= total_nodes and total_nodes < waiting_queue[0][1]:
                critical_time = 0
                tmp_node = total - total_nodes
                for i in range(len(running_queue)):
                    tmp_node += running_queue[i][1]
                    if tmp_node >= waiting_queue[0][1]:
                        critical_time = running_queue[i][0] - curr_time
                        if critical_time >= runtime:
                            flag = True
                            endtime = curr_time + runtime
                            waitingtime = 0
                            total_nodes -= nodenum
                            waitingtime_list.append([id, waitingtime])

                            if len(running_queue) == 0 or endtime > running_queue[-1][0]:
                                running_queue.append([endtime, nodenum])
                            else:
                                for i in range(len(running_queue)):
                                    if endtime <= running_queue[i][0]:
                                        running_queue.insert(i, [endtime, nodenum])
                                        break
                            break
            if flag == False:
                waiting_queue.append([id, nodenum, submittime, runtime])
            
            data = data.iloc[1:]

        # 2.waiting_queue
        while len(waiting_queue) != 0 and waiting_queue[0][1] <= total_nodes:
            id, nodenum, submittime, runtime = waiting_queue[0][0], waiting_queue[0][1], waiting_queue[0][2], \
                                               waiting_queue[0][3]
            endtime = curr_time + runtime
            waitingtime = curr_time - submittime
            total_nodes -= nodenum
            waiting_queue = waiting_queue[1:]
            waitingtime_list.append([id, waitingtime])

            if len(running_queue) == 0 or endtime >= running_queue[-1][0]:
                running_queue.append([endtime, nodenum])
            else:
                for i in range(len(running_queue)):
                    if endtime <= running_queue[i][0]:
                        running_queue.insert(i, [endtime, nodenum])
                        break

        # 3.running_queue
        while len(running_queue) != 0 and running_queue[0][0] == curr_time:
            total_nodes += running_queue[0][1]
            running_queue = running_queue[1:]

        curr_time += 1
    return waitingtime_list

track = pd.read_csv('data.csv')


track = track[['jobid', 'nodenum', 'submittime', 'runtime', 'corenum']]
track['submittime'] = pd.to_datetime(track['submittime'], format="%Y-%m-%d %H:%M:%S")
track['submittime'] = (track['submittime'] - track['submittime'].iloc[0]).dt.total_seconds()
track['submittime'] = track['submittime'].astype(int)

total_nodes = 21000
tmp = track.sort_values(by="submittime")
waitingtime_list = preemptive(track, total_nodes)
waitingtime_list = sorted(waitingtime_list)

result = pd.DataFrame(waitingtime_list, columns=['jobid', 'waitingtime_press'])
'''
The first 412 jobs are outside the scope of simulation but will occupy nodes within the simulation range. 
These are jobs that were submitted before March 1, 2018, 00:00:00 and ended after March 1, 2018, 00:00:00.
'''
result = result.iloc[412:]
tmp = track.sort_values(by="submittime")
waitingtime_list = FCFS(track, total_nodes)
waitingtime_list = sorted(waitingtime_list)
tmp = pd.DataFrame(waitingtime_list, columns=['jobid', 'waitingtime'])
tmp = list(tmp['waitingtime'])
tmp = tmp[412:]
result['waitingtime_fcfs'] = tmp

tmp = track.sort_values(by="submittime")
waitingtime_list = Priority_Scheduling(track, total_nodes)
waitingtime_list = sorted(waitingtime_list)
tmp = pd.DataFrame(waitingtime_list, columns=['jobid', 'waitingtime'])
tmp = list(tmp['waitingtime'])
tmp = tmp[412:]
result['waitingtime_prioty'] = tmp

total_nodes = 21000
tmp = track.sort_values(by="submittime")
waitingtime_list = easy_backfilling(track, total_nodes)
waitingtime_list = sorted(waitingtime_list)
tmp = pd.DataFrame(waitingtime_list, columns=['jobid', 'waitingtime'])
tmp = list(tmp['waitingtime'])
tmp = tmp[412:]
result['waitingtime_backfilling'] = tmp

print("-----------------------------------------------------------------------------------------------------------------------")
print("Average Waitingtime：")
print("    DSP:",result['waitingtime_press'].mean())
print("    FCFS:",result['waitingtime_fcfs'].mean())
print("    BcakFilling:",result['waitingtime_backfilling'].mean())
print("    VPS:",result['waitingtime_prioty'].mean())
print("-----------------------------------------------------------------------------------------------------------------------")

t = track[412:]
result['submittime'] = list(t['submittime'])
result['runtime'] = list(t['runtime'])
result['corenum'] = list(t['corenum'])

MBS_fccs = 0
MBS_press = 0
MBS_prioty = 0
MBS_backfilling = 0
#None exceed the threshold of 10, so no replacement is needed
result['MBS_fccs'] = (result['runtime'] + result['waitingtime_fcfs']) / result['runtime']
result['MBS_press'] = (result['runtime'] + result['waitingtime_press']) / result['runtime']
result['MBS_prioty'] = (result['runtime'] + result['waitingtime_prioty']) / result['runtime']
result['MBS_backfilling'] = (result['runtime'] + result['waitingtime_backfilling']) / result['runtime']

corenum_sum = result['corenum'].sum()

for i in range(len(result)):
    MBS_fccs += (result.iloc[i]['MBS_fccs'] * result.iloc[i]['corenum'])
    MBS_press += (result.iloc[i]['MBS_press'] * result.iloc[i]['corenum'])
    MBS_prioty += (result.iloc[i]['MBS_prioty'] * result.iloc[i]['corenum'])
    MBS_backfilling += (result.iloc[i]['MBS_backfilling'] * result.iloc[i]['corenum'])

print("-----------------------------------------------------------------------------------------------------------------------")
print("MWBS: ")
print("    DSP:",MBS_press / corenum_sum)
print("    FCFS:",MBS_fccs / corenum_sum)
print("    BcakFilling:",MBS_backfilling / corenum_sum)
print("    VPS:",MBS_prioty / corenum_sum)
print("-----------------------------------------------------------------------------------------------------------------------")

result['lastendtime_press'] = result['waitingtime_press'] + result['submittime'] + result['runtime']
result['lastendtime_fcfs'] = result['waitingtime_fcfs'] + result['submittime'] + result['runtime']
result['lastendtime_prioty'] = result['waitingtime_prioty'] + result['submittime'] + result['runtime']
result['lastendtime_backfilling'] = result['waitingtime_backfilling'] + result['submittime'] + result['runtime']
print("-----------------------------------------------------------------------------------------------------------------------")
print("Total endtime:")
print("    DSP: ", result['lastendtime_press'].max())
print("    FCFS: ", result['lastendtime_fcfs'].max())
print("    BcakFilling: ", result['lastendtime_backfilling'].max())
print("    VPS: ", result['lastendtime_prioty'].max())
print("-----------------------------------------------------------------------------------------------------------------------")