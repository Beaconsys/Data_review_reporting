#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time     : 2020/7/20 15:52
# @Author   : xhzhang
# @Site     :
# @File     : lustre_server.py
# @Software : PyCharm

import datetime
from collections import defaultdict
# from comm.config import OLD_FLAG
from utils import str2time, division, round2decimal, minus, add

size_order = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, "1K", "2K", "4K", "8K", "16K",
    "32K", "64K", "128K", "256K", "512K", "1M", "2M", "4M", "8M", "16M", "32M",
    "64M", "128M", "256M", "512M", "1024M", "2048M", "4096M", "8192M",
    "16384M", "32768M"
]


def calculate_io(ost, countList):
    """
    calculate IOPs or IOBW
    :param countList: calculate the original information of IOPs
    :param host: the countList belong to the host
    :param ost: the countList belong to the ost
    :return: [{t:xx,r:xx},]
    """
    data_dict = {}
    try:
        base = countList[0]
        realList = countList[1:]
    except Exception:
        pass
    else:
        for index, c in enumerate(realList):
            flag = 0
            c[0] = str2time(c[0]) if isinstance(c[0], str) else c[0]
            base[0] = str2time(base[0]) if isinstance(base[0],
                                                      str) else base[0]
            time_difference = (c[0] - base[0]).total_seconds()
            if time_difference == 0:
                if base[1] > c[1] or base[2] > c[2] or base[3] > c[3] or base[
                        4] > c[4]:
                    flag = 1
                else:
                    flag = 2
            if flag in [0, 2]:
                cr = float(c[1] - base[1])
                cw = float(c[2] - base[2])
                br = float(c[3] - base[3])
                bw = float(c[4] - base[4])
                size_r = minus(c[5], base[5])
                size_w = minus(c[6], base[6])
                acr = round2decimal(cr)
                acw = round2decimal(cw)
                abr = round2decimal(br)
                abw = round2decimal(bw)
                asize_r = size_r
                asize_w = size_w

                if flag == 0:
                    t = c[0]
                    if acr >= 0 and acw >= 0 and abr >= 0 and abw >= 0:
                        _tmp = [
                            acr, acw, abr, abw, asize_r, asize_w, 0.0, 0.0, 0.0
                        ]
                    else:
                        _tmp = [
                            0.0, 0.0, 0.0, 0.0, [0] * len(size_order),
                            [0] * len(size_order), 0.0, 0.0, 0.0
                        ]
                    data_dict[t] = _tmp
                else:
                    r = [cr, cw, br, bw, size_r, size_w, 0.0, 0.0, 0.0]
                    if base[0] in data_dict:
                        old = data_dict[base[0]]
                        data_dict[base[0]] = [
                            add(old[i], r[i]) if isinstance(old[i], list) else
                            round(old[i] + r[i], 2) for i in range(len(r))
                        ]
                    else:
                        data_dict[base[0]] = r
                base = c
    return data_dict


def analyze_lustre_server_data(raw_data):
    """
    analyze lustre server data.
    :param results: results from elastic-search, include message field,host field,@timestamp field.
    :param end : end time
    :return:
    """
    datas = defaultdict(list)
    ios = {}
    for ost, vals in raw_data.items():
        ios[ost] = []
        for val in vals:
            htime = str2time(val[0])
            rpcs = eval(val[1]) if isinstance(val[1], str) else val[1]
            read_count, write_count, read_bw, write_bw, size_r, size_w = 0, 0, 0, 0, [
                0
            ] * len(size_order), [0] * len(size_order)
            for rpc in rpcs:
                try:
                    read_count += int(rpc[1])
                    write_count += int(rpc[2])
                    if isinstance(rpc[0], str) and rpc[0].endswith('K'):
                        rpc_page_size = int(rpc[0].split('K')[0]) * 1024
                        index = size_order.index(rpc[0])
                    elif isinstance(rpc[0], str) and rpc[0].endswith('M'):
                        rpc_page_size = int(rpc[0].split('M')[0]) * 1024 * 1024
                        index = size_order.index(rpc[0])
                    else:
                        rpc_page_size = int(rpc[0])
                        index = size_order.index(rpc_page_size)
                    size_r[index] = rpc[1]
                    size_w[index] = rpc[2]
                except Exception as e:
                    print(str(e))
                else:
                    read_bw += 1024 * 4 * rpc_page_size * rpc[1]
                    write_bw += 1024 * 4 * rpc_page_size * rpc[2]
            ios[ost].append([
                htime, read_count, write_count, read_bw, write_bw, size_r,
                size_w
            ])
    # analyze io data
    for ost, v in ios.items():
        _io_data = {}
        v.sort(key=lambda x: x[0])
        rs = calculate_io(ost, v)
        for ct, cr in rs.items():
            if ct not in _io_data:
                _io_data[ct] = cr
            else:
                br = _io_data[ct]
                _io_data[ct] = [
                    add(br[i], cr[i]) if isinstance(br[i], list) else br[i] +
                    cr[i] for i in range(len(cr))
                ]
        for t, val in _io_data.items():
            if 1:
                datas[ost].append([t] + val)
    return datas


def parse_lustre_server_data(results):
    format_data = results
    ana_datas = analyze_lustre_server_data(format_data)
    return ana_datas
