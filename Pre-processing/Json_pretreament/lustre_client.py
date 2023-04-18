#!/usr/bin/python
# -*- coding: utf-8 -*-
# @Time     : 2020/7/20 15:52
# @Author   : xhzhang
# @Site     :
# @File     : lustre_client.py
# @Software : PyCharm
import json
import datetime
from utils import minus, round2decimal, division, str2time, add, time2str
from collections import defaultdict

# size_order = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, "1K", "2K", "4K", "8K", "16K", "32K", "64K"]
size_order = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384,
    32768, 65536
]


def calculate_io(host, ost, countList):
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
            #Do not count all seconds data, only count changing seconds
            time_difference = min((c[0] - base[0]).total_seconds(), 1)
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
                acr = round2decimal(
                    cr / time_difference if time_difference != 0 else cr)
                acw = round2decimal(
                    cw / time_difference if time_difference != 0 else cw)
                abr = round2decimal(
                    br / time_difference if time_difference != 0 else br)
                abw = round2decimal(
                    bw / time_difference if time_difference != 0 else bw)
                asize_r = division(size_r, time_difference)
                asize_w = division(size_w, time_difference)

                if flag == 0:
                    #for i in range(1, int(time_difference) + 1):
                    #    t = base[0] + datetime.timedelta(seconds=i)
                    for i in range(1):
                        t = c[0]
                        if acr >= 0 and acw >= 0 and abr >= 0 and abw >= 0:
                            _tmp = [
                                acr, acw, abr, abw, asize_r, asize_w, 0.0, 0.0,
                                0.0
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


def calculate_cache(host, countList):
    """
    calculate cache
    :param host:
    :param countList:
    :return:
    """
    data_dic = {}
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
            #Same as rpc, only counts change seconds
            time_difference = min((c[0] - base[0]).total_seconds(), 1)
            if time_difference == 0:
                if base[1] > c[1] or base[2] > c[2] or base[3] > c[3]:
                    flag = 1
                else:
                    flag = 2
            if flag in [0, 2]:
                d_cache_hit = float(int(c[1]) - int(base[1]))
                d_cache_miss = float(int(c[2]) - int(base[2]))
                d_cache_discard = float(int(c[3]) - int(base[3]))
                a_d_cache_hit = round2decimal(
                    d_cache_hit /
                    time_difference if time_difference != 0 else d_cache_hit)
                a_d_cache_miss = round2decimal(
                    d_cache_miss /
                    time_difference if time_difference != 0 else d_cache_miss)
                a_d_cache_discard = round2decimal(
                    d_cache_discard / time_difference
                    if time_difference != 0 else d_cache_discard)
                if flag == 0:
                    #for i in range(1, int(time_difference) + 1):
                    #    t = base[0] + datetime.timedelta(seconds=i)
                    # only counts change seconds
                    for i in range(1):
                        t = c[0]
                        if a_d_cache_hit >= 0 and a_d_cache_miss >= 0 and a_d_cache_discard >= 0:
                            _tmp = [
                                0.0, 0.0, 0.0, 0.0, [0] * len(size_order),
                                [0] * len(size_order), a_d_cache_hit,
                                a_d_cache_miss, a_d_cache_discard
                            ]
                        else:
                            _tmp = [
                                0.0, 0.0, 0.0, 0.0, [0] * len(size_order),
                                [0] * len(size_order), 0.0, 0.0, 0.0
                            ]
                        data_dic[t] = _tmp
                else:
                    r = [
                        0.0, 0.0, 0.0, 0.0, [0] * len(size_order),
                        [0] * len(size_order), d_cache_hit, d_cache_miss,
                        d_cache_discard
                    ]
                    if base[0] in data_dic:
                        old = data_dic[base[0]]
                        data_dic[base[0]] = [
                            add(old[i], r[i]) if isinstance(old[i], list) else
                            round(old[i] + r[i], 2) for i in range(len(r))
                        ]
                    else:
                        data_dic[base[0]] = r

                base = c
    return data_dic


def analyze_lustre_client_data(host_value_map, start, end):
    result = {}
    datas = defaultdict(list)
    for host, values in host_value_map.items():
        ios = {}
        caches = []
        if host not in result:
            result[host] = {}

        for value in values:
            htime = value[0]
            message = value[1]

            if message[0] == "cache":
                caches.append([htime] + message[1])
            else:
                ost_num = message[0]
                rpc_info = message[1]
                read_count, write_count, read_bw, write_bw, size_r, size_w = 0, 0, 0, 0, [
                    0
                ] * len(size_order), [0] * len(size_order)
                for rpc in rpc_info:
                    read_count += int(rpc[1])
                    write_count += int(rpc[2])
                    if isinstance(rpc[0], str) and rpc[0].endswith('K'):
                        rpc_page_size = int(rpc[0].split('K')[0]) * 1024
                    else:
                        rpc_page_size = int(rpc[0])
                    try:
                        index = size_order.index(rpc_page_size)
                        size_r[index] = rpc[1]
                        size_w[index] = rpc[2]
                    except Exception as e:
                        pass

                    read_bw += 1024 * 4 * rpc_page_size * rpc[1]
                    write_bw += 1024 * 4 * rpc_page_size * rpc[2]
                _tmp_io = [
                    htime, read_count, write_count, read_bw, write_bw, size_r,
                    size_w
                ]
                if ost_num not in ios:
                    ios[ost_num] = []
                ios[ost_num].append(_tmp_io)
        if ios:
            for ost, rpcs in ios.items():
                rpcs.sort(key=lambda x: x[0])
                rs = calculate_io(host, ost, rpcs)
                for rt, rr in rs.items():
                    if rt not in result[host]:
                        result[host][rt] = rr
                    else:
                        old = result[host][rt]
                        result[host][rt] = [
                            add(old[i], rr[i]) if isinstance(old[i], list) else
                            round(old[i] + rr[i], 2) for i in range(len(rr))
                        ]
        if caches:
            caches.sort(key=lambda x: x[0])
            cs = calculate_cache(host, caches)
            for ct, cr in cs.items():
                if ct not in result[host]:
                    result[host][ct] = cr
                else:
                    old = result[host][ct]
                    result[host][ct] = [
                        add(old[i], cr[i]) if isinstance(old[i], list) else
                        round(old[i] + cr[i], 2) for i in range(len(cr))
                    ]

    for host, values in result.items():
        for htime, value in values.items():
            if 1:
                htime = htime if isinstance(htime, str) else time2str(htime)
                datas[host].append([htime] + value)
    return datas


def parse_lustre_client_data(results,
                             host,
                             start='00:00:00',
                             end='00:00:00',
                             data_type="new",
                             source='no'):
    format_data = {}
    if data_type == "new":
        for result in results:
            try:
                line = result.strip(' \n')
                listline = line.split(',', maxsplit=2)
                htime = listline[0].strip(" \"'[]")
                ost = listline[1].strip(" \"'[]")
                message = listline[2].strip(" \"'")
                if message.endswith(']]]'):
                    message = message[:-1]
                elif message.endswith("]]']"):
                    message = message[:-2]
                if ost == 'cache':
                    listmessage = message.split(',')
                    tmplist = []
                    for cacheinfo in listmessage:
                        tmpc = cacheinfo.strip(" []'\"")
                        if tmpc.endswith('.0'):
                            tmpc = tmpc[:-2]
                        tmplist.append(int(tmpc))
                    message = tmplist
                else:
                    message = json.loads(message)
                r_data = [ost, message]
                if host not in format_data:
                    format_data[host] = []
                format_data[host].append([htime, r_data])
            except Exception:
                print('Row data exception', result)
    '''
    if source == "es":
        assert isinstance(results, list)
        for result in results:
            try:
                host = result['host']
                message = result['message']
                if source == 'old':
                    htime = result['htime']
                    message_info = message.split(" ", maxsplit=1)
                    ost_num = message_info[0]
                    if ost_num == "cache_hit":
                        cache_info = message.split(" ")
                        cache_hit = int(cache_info[1])
                        cache_miss = int(cache_info[3])
                        cache_discard = int(cache_info[5])
                        data = [cache_hit, cache_miss, cache_discard]
                        r_data = ["cache", data]
                    else:
                        data = eval(message_info[1])
                        r_data = [ost_num, data]
                else:
                    message_info = eval(message)
                    htime = message_info[0]
                    ost_num = message_info[1]
                    if ost_num == "cache":
                        data = message_info[2:]
                    else:
                        data = message_info[2]
                    r_data = [ost_num, data]
                if host not in format_data:
                    format_data[host] = []
                format_data[host].append([htime, r_data])
            except Exception as e:
                pass
    else:
        assert isinstance(results, dict)
        #format_data = results
        for host, values in results.items():
            for raw_data in values:
                try:
                    if host not in format_data:
                        format_data[host] = []
                    format_data[host].append(
                        [raw_data[0], [raw_data[1],
                                       eval(raw_data[2])]])
                except Exception as e:
                    print(str(e))
    '''
    result = analyze_lustre_client_data(format_data, start, end)
    return result
