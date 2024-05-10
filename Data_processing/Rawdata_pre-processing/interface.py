import json
import redis
import gc
#from sharefunc import *
import traceback
from op_re_lwfs_client import *
from lwfs_client_parse import *
from utils import *
from config import *
from lustre_server import *
import json

try:
    import commands as cm
except Exception as e:
    import subprocess as cm


def get_nodes_time_range(st, et):
    days = get_day_list(st, et)
    pool = multiprocessing.Pool(processes=10)
    for day in days:
        print(day)
        d_path = get_day_path(day)
        day_path = os.path.join(RAW_DATA_DIR, d_path)
        if not os.path.exists(day_path):
            continue
        groups = os.listdir(day_path)
        for group in groups:
            path = os.path.join(day_path, group)
            nodes = os.listdir(path)
            for node in nodes:
                pool.apply_async(read_analyse_write, (path, node))
    pool.close()
    pool.join()


def read_analyse_write(path, node):
    try:
        node = str(node)
        if node == '20.0.208.64.json':
            print(path)
        ip = '.'.join(node.split('.')[0:-1])
        json_data = {ip: []}
        path = os.path.join(path, node)
        with open(path, 'r') as f:
            reader = csv.reader(f)
            count = 0
            for line in reader:
                json_data[ip].append(line)
        analyse_data = parse_lwfs_client_data(json_data, ip)[ip]
        tmp = re.split('([/])', path)
        path = ['analyse_json' if i == 'compjson' else i for i in tmp]
        analyse_path = ''.join(path)
        analyse_dir = ''.join(path[:-1])
        if not os.path.exists(analyse_dir):
            try:
                os.makedirs(analyse_dir)
            except Exception as ee:
                print(str(ee))
        with open(analyse_path, 'w', newline='') as f:
            writer = csv.writer(f)
            for row in analyse_data:
                writer.writerow(row)
    except Exception as e:
        error_type, error_value, error_trace = sys.exc_info()
        print(error_type)
        print(error_value)
        for info in traceback.extract_tb(error_trace):
            print(info)


def handle_write_ost(data, day):
    try:
        result = parse_lustre_server_data(data)
        d_path = day.replace('-', '/')
        raw_dir = ANALYSE_PATH_MAP['ost1']
        path = os.path.join(raw_dir, d_path)
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except Exception as e:
                print(str(e))
        for ost, values in result.items():
            ost_path = os.path.join(path, ost + '.json')
            with open(ost_path, 'w') as f:
                writer = csv.writer(f)
                for value in values:
                    writer.writerow(value)
    except Exception as e:
        print(str(e))


def read_analyse_write_ost1(path, ost, day):
    try:
        ost_path = os.path.join(path, ost)
        ost_num = ost.split('.')[0]
        json_data = {ost_num: []}
        with open(ost_path, 'r') as f:
            reader = csv.reader(f)
            for line in reader:
                htime = line[0]
                data = eval(line[1])
                json_data[ost_num].append([htime, data])
        handle_write_ost(json_data, day)

    except Exception as e:
        error_type, error_value, error_trace = sys.exc_info()
        print(error_type)
        print(error_value)
        for info in traceback.extract_tb(error_trace):
            print(info)


def read_ost_json(st, et, ost1_path):
    days = get_day_list(st, et)
    pool = multiprocessing.Pool(processes=6)
    for day in days:
        d_path = get_day_path(day)
        day_path = os.path.join(ost1_path, d_path)
        if not os.path.exists(day_path):
            continue
        osts = os.listdir(day_path)
        for ost in osts:
            pool.apply_async(read_analyse_write_ost1, (day_path, ost, day))
    pool.close()
    pool.join()


def handle_readline(lines):
    res = defaultdict(list)
    for line in lines:
        line = line[:-1]
        line = line.split(' ', maxsplit=1)
        [message, htime] = line[1].rsplit(' ', maxsplit=1)
        ost_num = line[0]
        data = json.loads(message)
        htime = utc2local(htime)
        res[ost_num].append([htime, data])
    return res


def read_day_json(st, et, ost1_path):
    days = get_day_list(st, et)
    for day in days:
        handle_everyday_json(day, ost1_path)


def handle_everyday_json(day, ost1_path):
    try:
        json_data = defaultdict(list)
        tmp_day = day.replace('-', '.')
        d_path = tmp_day + '.json'
        monthpath = '/'.join(day.split('-')[:2])
        day_path = os.path.join(ost1_path, monthpath, d_path)

        if not os.path.exists(day_path):
            return
        with open(day_path, 'r') as f:
            lines = f.readlines()
        mulprocesses = 5
        chunksize = len(lines) // mulprocesses
        if chunksize == 0:
            return
        chunks = [
            lines[i:i + chunksize] for i in range(0, len(lines), chunksize)
        ]
        pool = multiprocessing.Pool(processes=mulprocesses)
        result = pool.map(handle_readline, chunks)
        pool.close()
        pool.join()
        pool.terminate()
        print('The first process pool is executed')
        for d in result:
            for key, value in d.items():
                json_data[key].extend(value)

        print('finish reading')
        pool = multiprocessing.Pool(processes=mulprocesses)
        for ost in json_data:
            ost_data = {}
            ost_data[ost] = json_data[ost]
            pool.apply_async(handle_write_ost, (ost_data, day))
        pool.close()
        pool.join()
        pool.terminate()
    except Exception as e:
        print(str(e))


def handle_lustre_server(st, et, path):
    stime = str2time(st + ' 00:00:00')
    if stime >= str2time('2020-11-01 00:00:00'):
        data = read_ost_json(st, et, path)
    elif stime >= str2time('2018-01-12 00:00:00'):
        data = read_day_json(st, et, path)
    else:
        print('2017 non-paged data')
        return


def handle_oldcomp_readline(lines):
    res = defaultdict(list)
    for line in lines:
        try:
            line = line[1:-2]
            [htime, message] = line.split('T', maxsplit=1)
            [message_info, ip] = message.rsplit(',', maxsplit=1)
            message_info = message_info.strip(" '")
            ip = ip.strip(" '")
            htime = htime.strip(" '[]")
            res[ip].append([htime, message_info])
        except Exception as e:
            print(str(e))
    return res


def handle_write_comp(data, day, time_type='new'):
    ip = ''
    for host in data:
        try:
            result = parse_lwfs_client_data(data, host, format_type=time_type)
            ip = host
            d_path = day.replace('-', '/')
            raw_dir = ANALYSE_PATH_MAP['comp']
            group_path = 'group_' + ip.split('.')[1]
            path = os.path.join(raw_dir, d_path, group_path)
            if not os.path.exists(path):
                try:
                    os.makedirs(path)
                except Exception as e:
                    print(str(e))
            for ip, values in result.items():
                if not ip.endswith('.json'):
                    ip = ip + '.json'
                ip_path = os.path.join(path, ip)
                with open(ip_path, 'w') as f:
                    writer = csv.writer(f)
                    for value in values:
                        writer.writerow(value)
        except Exception as e:
            print(str(e))


def data_generator(jsondict):
    for key, value in jsondict.items():
        yield key, value


def handle_comp_everyday_json(day, path):
    try:
        json_data = defaultdict(list)
        tmp_day = day.replace('-', '.')
        d_path = tmp_day + '.json'
        monthpath = '/'.join(day.split('-')[:2])
        day_path = os.path.join(path, monthpath, d_path)
        if not os.path.exists(day_path):
            return False
        lines = []
        with open(day_path, 'r') as f:
            lines = f.readlines()
        mulprocesses = 10
        chunksize = len(lines) // mulprocesses
        #测试小chuank
        if chunksize == 0:
            return
        chunks = [
            lines[i:i + chunksize] for i in range(0, len(lines), chunksize)
        ]

        with multiprocessing.Pool(processes=mulprocesses) as pool:
            result = pool.map(handle_oldcomp_readline, chunks)
        '''
        pool = multiprocessing.Pool(processes=mulprocesses)
        result = pool.map(handle_oldcomp_readline, chunks)
        pool.close()
        pool.join()
        pool.terminate()
        '''
        print('The first process pool is executed')
        del chunks
        for d in result:
            for key, value in d.items():
                json_data[key].extend(value)

        with open('tmpdict', 'w') as f:
            json.dump(json_data, f)

        del result
        del json_data
        #n = len(json_data) // mulprocesses
        '''
        subdict = [
            dict(list(json_data.items())[i:i + n])
            for i in range(0, len(json_data), n)
        ]
        '''
        print('finish reading')
    except Exception as e:
        print(str(e))
        return True
        '''
        pool = multiprocessing.Pool(processes=mulprocesses)
        for d in subdict:
            pool.apply_async(handle_write_comp, (d, day, 'oldest'))
        pool.close()
        pool.join()
        '''


def handle_lwfs_client(st, et, path):
    stime = str2time(st + ' 00:00:00')
    if stime <= str2time('2020-02-01 00:00:00'):
        handle_comp_day_json(st, et, path)


def handle_dumpjson(st, et, json_type):
    json_path = JSON_PATH_MAP[json_type]
    if json_type == 'ost1':
        handle_lustre_server(st, et, json_path)
    elif json_type == 'comp':
        get_nodes_time_range(st, et)
    return


if __name__ == '__main__':
    st = sys.argv[1]
    et = sys.argv[2]
    json_type = sys.argv[3]
    handle_dumpjson(st, et, json_type)
