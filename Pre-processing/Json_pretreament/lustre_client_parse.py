from utils import *
from config import *
from lustre_client import *
import sys


def handle_distrit_days_json(st, et):
    days = get_day_list(st, et)
    for day in days:
        print(day)
        handle_distrit_everyday_json(day)


def handle_distrit_everyday_json(day):
    d_path = day.replace('-', '/')
    day_path = os.path.join(FWD1_JSON_PATH, d_path)
    if not os.path.exists(day_path):
        return
    fwds = os.listdir(day_path)
    fwds_nobak = []
    for fwdnode in fwds:
        if fwdnode.endswith('.json'):
            fwds_nobak.append(fwdnode)
    mulprocs = min(len(fwds_nobak), 8)
    pool = multiprocessing.Pool(processes=mulprocs)
    for node in fwds_nobak:
        pool.apply_async(handle_distri_node_json, (day_path, day, node))
    pool.close()
    pool.join()


def handle_distri_node_json(day_path, day, node):
    ip = node.rsplit('.', maxsplit=1)[0]
    node_jsonpath = os.path.join(day_path, node)
    with open(node_jsonpath, 'r') as f:
        data = f.readlines()
    anadata = parse_lustre_client_data(data, ip, data_type='new')[ip]
    month_list = day.split('-')[:-1]
    monthpath = os.path.join(month_list[0], month_list[1])
    anapath = os.path.join(FWD1_ANALYSE_PATH, monthpath, ip)
    if not os.path.exists(anapath):
        try:
            os.makedirs(anapath)
        except Exception:
            pass
    anawritedata = list(map(lambda x: str(x) + '\n', anadata))
    anafile = os.path.join(anapath, 'all.csv')
    with open(anafile, 'a') as f:
        f.write(''.join(anawritedata))


if __name__ == '__main__':
    st = sys.argv[1]
    et = sys.argv[2]
    handle_distrit_days_json(st, et)
