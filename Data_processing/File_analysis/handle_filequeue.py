import os
import numpy as np
from collections import defaultdict
import math
import sys
from utils import *
inter_distrit = [0 for i in range(15)]
inter_jud = [math.pow(2, i) for i in range(15)]
with open('sc_data/filequeue_csv', 'r') as f:
    data = f.readlines()
count = 0
res = []
jobshare_distrit = [0 for i in range(25)]
job_jud = [math.pow(2, i) for i in range(25)]
jobdict = defaultdict(int)
st = str2time('2021-12-31 00:00:00')
et = str2time('2020-12-31 00:00:00')
for line in data:
    try:
        line = line.strip('\n')
        [_f, fmessage] = line.split('#@', maxsplit=1)
        fqueue = fmessage.split('#@')
        base = str2time(fqueue[0].split(',')[3])
        for i in range(0, len(fqueue)):
            new_ = fqueue[i].split(',')
            '''
            interval = round((str2time(new_[2]) - base).seconds / 3600, 2)
            for _ind in range(15):
                if interval < inter_jud[_ind]:
                    inter_distrit[_ind] += 1
                    break
            base = str2time(new_[3])
            '''
            jobid = new_[0]
            jobdict[jobid] += 1
    except Exception as e:
        pass
_manyfile_jobs = []
for key, val in jobdict.items():
    if val >= 262144:
        _manyfile_jobs.append([key, val])

with open('manyfile_jobs.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(_manyfile_jobs)
sys.exit(0)
'''
    for ind in range(len(jobshare_distrit)):
        if val < job_jud[ind]:     
            jobshare_distrit[ind] += 1
            break
'''
print(inter_distrit)
print(jobshare_distrit)
with open('distrit.txt', 'a') as f:
    #f.write('intervals' + str(inter_distrit) + '\n')
    f.write('\njobshares' + str(jobshare_distrit))
    #st = min(str2time(fqueue[0].split(',')[2]), st)
    #et = max(str2time(fqueue[-1].split(',')[2]), et)
'''
    res.append(line)
with open('no1fqueue.csv', 'w') as f:
    f.write(''.join(res))
'''
'''
    for q in fqueue:
        qlist = q.split(',')
        corelist.append(int(qlist[1]))
    avgcore = np.mean(corelist)
    if avgcore > 16:
        count += 1
        continue
print(count)
'''
