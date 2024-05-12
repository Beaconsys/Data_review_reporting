import sys
from get_ip import *
from utils import *

FILEINFOPATH = 'data/fileinfoscan/fileonly/'


def handlejobs():
    with open('2021_jobs_new.csv', 'r') as f:
        jobs = f.readlines()
    jobcount = 0
    for job in jobs:
        try:
            if jobcount % 100 == 1:
                print('Jobs processed', jobcount)
            [jobid, st, et, nodelist] = job.split(',', maxsplit=3)

            ip_dict = defaultdict(list)
            nodelist_ = nodelist.strip(' "\n').split(',')
            for nodes_ in nodelist_:
                try:
                    if '-' in nodes_:
                        node_ = nodes_.strip('\'"-').split('-')
                        snode = int(node_[0])
                        enode = int(node_[1])
                        for node in range(snode, enode + 1):
                            ip = getip(node)
                            group = 'group_' + ip.split('.', maxsplit=2)[1]
                            ip_dict[group].append(ip)
                    else:
                        ip = getip(nodes_)
                        group = 'group_' + ip.split('.', maxsplit=2)[1]
                        ip_dict[group].append(ip)
                except Exception as e:
                    print(str(e))
            mulprocesses = min(len(ip_dict), 6)
            pool = multiprocessing.Pool(processes=mulprocesses)
            file_dict = {}
            sday = st.split(' ')[0]
            eday = et.split(' ')[0]
            days = get_day_list(sday, eday)
            gres = []
            jobst = str2time(st)
            jobet = str2time(et)
            for group, ip_list in ip_dict.items():
                gres.append(
                    pool.apply_async(
                        get_groupfile,
                        (group, ip_list, days, jobid, jobst, jobet)))
            pool.close()
            pool.join()

            for _async in gres:
                gfile = _async.get()
                if gfile == None or gfile == {}:
                    continue
                for _fn, message in gfile.items():
                    if _fn not in file_dict:
                        file_dict[_fn] = message
                    else:
                        [_proc, _st, _et] = file_dict[_fn]
                        _proc += message[0]
                        if _st > message[1]:
                            _st = message[1]
                        if _et < message[2]:
                            _et = message[2]
                        file_dict[_fn] = [_proc, _st, _et]

            if file_dict != {}:
                updata_redis(file_dict, jobid)

        except Exception as e:
            print(str(e))


def updata_redis(file_dict, jobid):
    try:
        redis_pool = redis.ConnectionPool(host='127.0.0.1',
                                          port='6379',
                                          db=3,
                                          decode_responses=True)
        redis_client = redis.Redis(connection_pool=redis_pool)
        for _f, _message in file_dict.items():
            [_proc, st, et] = _message
            tmpinfo = [jobid, str(_proc), time2str(st), time2str(et)]
            format_tmpinfo = ','.join(tmpinfo)
            fqueue = redis_client.get(_f)
            if fqueue:
                _newvalue = '#@'.join([fqueue, format_tmpinfo])
            else:
                _newvalue = format_tmpinfo
            redis_client.set(_f, _newvalue)
    except Exception as e:
        print('Redis operation error')
        print(str(e))


def get_groupfile(group, ip_list, days, jobid, jobst, jobet):
    file_dict = {}
    try:
        for ip in ip_list:
            ip_file_dict = {}
            for day in days:
                d_path = day.replace('-', '/')
                group_path = os.path.join(FILEINFOPATH, d_path, group)
                ip_path = os.path.join(group_path, ip + '.json')
                if not os.path.exists(ip_path):
                    return

                with open(ip_path, 'r') as f:
                    ip_data = f.readlines()
                for line in ip_data:
                    [_htime, message] = line.split(',', maxsplit=1)
                    htime = str2time(_htime)
                    if htime < jobst or htime > jobet:
                        continue
                    message_list = message.split(',')
                    for finfo in message_list:
                        [fn, fproc] = finfo.split(' ')
                        fproc = int(fproc)
                        if fn == 'None':
                            continue
                        if fn not in ip_file_dict:
                            ip_file_dict[fn] = [fproc, htime, htime]
                        else:
                            [_proc, _st, _et] = ip_file_dict[fn]
                            if _proc < fproc:
                                _proc = fproc
                            if _st > htime:
                                _st = htime
                            if _et < htime:
                                _et = htime
                            ip_file_dict[fn] = [_proc, _st, _et]
            for _fn, _message in ip_file_dict.items():
                if _fn not in file_dict:
                    file_dict[_fn] = _message
                else:
                    [_proc, _st, _et] = file_dict[_fn]
                    _proc += _message[0]
                    if _st > _message[1]:
                        _st = _message[1]
                    if _et < _message[1]:
                        _et = _message[1]
                    file_dict[_fn] = [_proc, _st, _et]

    except Exception as e:
        print(group, 'group processing error')
    return file_dict


handlejobs()
