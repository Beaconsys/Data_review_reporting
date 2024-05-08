import sys
from utils import *
import traceback
from config import *
from lwfs_client import *


def handle_lwfs_client(st, et):
    days = get_day_list(st, et)
    for day in days:
        print(day)
        handle_oneday_compjson(day)


def handle_oneday_compjson(day):
    pool = multiprocessing.Pool(processes=3)
    d_path = get_day_path(day)
    day_path = os.path.join(RAW_DATA_DIR, d_path)
    if not os.path.exists(day_path):
        return
    groups = os.listdir(day_path)
    for group in groups:
        path = os.path.join(day_path, group)
        nodes = os.listdir(path)
        for node in nodes:
            pool.apply_async(read_analyse_write_comp, (path, node, day))
    pool.close()
    pool.join()


def read_analyse_write_comp(path, node, day):
    try:
        print(path, node)
        path = os.path.join(path, node)
        ip = node.rsplit('.', maxsplit=1)[0]
        json_data = {ip: []}
        with open(path, 'r') as f:
            data = f.readlines()
            '''
            for d in data:
                line = d.strip('[]\n')
                real_line = d.split(',', maxsplit=1)
                json_data[ip].append(real_line)
            '''
        analyse_data = parse_lwfs_client_data(data, ip, format_type='old')[ip]
        del data
        d_path = get_day_path(day)
        day_path = os.path.join(COMP_ANALYSE_PATH, d_path)
        group_path = 'group_' + ip.split('.')[1]
        analyse_dir = os.path.join(day_path, group_path)
        analyse_path = os.path.join(analyse_dir, node)
        if not os.path.exists(analyse_dir):
            try:
                os.makedirs(analyse_dir)
            except Exception:
                pass
        #analyse_str_data = '\n'.join(analyse_data) + '\n'
        write_comp(analyse_data, analyse_path)
    except Exception as e:
        print(path, 'log processing error')
        with open('log.txt', 'a') as logf:
            logf.write('log processing error' + path)
        print(error_type)
        print(error_value)


def write_comp(analyse_data, analyse_path):
    analyse_str_data = list(
        map(lambda x: str(x).strip('[]') + '\n', analyse_data))
    with open(analyse_path, 'w') as f:
        f.write(''.join(analyse_str_data))


if __name__ == '__main__':
    st = sys.argv[1]
    et = sys.argv[2]
    handle_lwfs_client(st, et)
