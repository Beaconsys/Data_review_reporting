#import redis
import os
from collections import defaultdict
import datetime
import threading
from functools import reduce
import re, math
from utils import *


def get_distribution(ov):
    size_order = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    r = [0] * len(size_order)
    for x in ov:
        size_k = x[1] / 1024
        count = x[3]
        index_tmp = int(math.log(size_k, 2))
        if index_tmp < 0:
            index = 0
        else:
            index = index_tmp + 1
        r[index] += count
    return r


def analyse_continuous(read_write_info):
    """
    analyze write is it continuous.
    :param write_info: {host:{fd:{htime:[size,offset,count]}}}
    :return:
    """
    is_continuous = defaultdict(dict)
    for host, values in read_write_info.items():
        is_continuous[host] = defaultdict(dict)
        for fd, value in values.items():
            order_value = sorted(value.items(), key=lambda x: x[0])
            for ov in order_value:
                htime = ov[0]
                finfo = ov[1]
                if len(finfo) == 1:
                    is_continuous[host][htime][fd] = 1
                else:
                    continuous_count = 0
                    not_continuous_count = 0
                    #确保不是倒序读写
                    if finfo[0][1] > finfo[-1][1]:
                        continue
                    else:
                        finfo.sort(key=lambda x: x[1])
                    for index in range(len(finfo) - 1):
                        size = finfo[index][0]
                        offset = finfo[index][1]
                        count = finfo[index][2]
                        next_offset = offset + size * count
                        if index == len(finfo) - 1:
                            pass
                        else:
                            if next_offset == finfo[index + 1][1]:
                                continuous_count += count - 1
                            else:
                                continuous_count += count - 1
                                not_continuous_count += 1
                    if not_continuous_count == 0:
                        is_continuous[host][htime][fd] = 1
                    else:
                        if continuous_count / not_continuous_count > 1:
                            is_continuous[host][htime][fd] = 1
                        else:
                            is_continuous[host][htime][fd] = 0
    return is_continuous


def analyze_data(formatted, is_write_continuous, is_read_continuous, fd_fn_map,
                 start, end):
    ana_data = defaultdict(dict)
    results = {}
    for host, values in formatted.items():
        for htime, value in values.items():
            _fd = []
            _fn = []
            file_info = []
            r_iobw, r_iops, w_iobw, w_iops, open_count, close_count, file_count, r_distr, w_distr = 0, 0, 0, 0, 0, 0, 0,[], []
            _fileness = []
            for op, ov in value.items():
                #ov:[fd,size,offset,count]
                try:
                    if op == "OPEN":
                        open_count = len(ov)
                        _fd += [x[0] for x in ov]
                        _fileness += [(x[0], 0, 0, 1, 0) for x in ov]
                    elif op == "CLOSE":
                        close_count = len(ov)
                        _fd += [x[0] for x in ov]
                        _fileness += [(x[0], 0, 0, 0, 1) for x in ov]
                    elif op == "WRITE":
                        w_iops = reduce(lambda x, y: x + y, [x[3] for x in ov])
                        w_iobw = reduce(lambda x, y: x + y,
                                        [x[3] * x[1] for x in ov])
                        w_distr = get_distribution(ov)
                        _fd += [x[0] for x in ov]
                        _fileness += [(x[0], 0, x[3] * x[1], 0, 0) for x in ov]
                    elif op == "READ":
                        r_iops = reduce(lambda x, y: x + y, [x[3] for x in ov])
                        r_iobw = reduce(lambda x, y: x + y,
                                        [x[3] * x[1] for x in ov])
                        r_distr = get_distribution(ov)
                        _fd += [x[0] for x in ov]
                        _fileness += [(x[0], x[3] * x[1], 0, 0, 0) for x in ov]
                except Exception as e:
                    continue
            for fd in set(_fd):
                try:
                    if host in fd_fn_map and fd in fd_fn_map[host]:
                        fn = fd_fn_map[host][fd]
                    else:
                        fn = None
                    _fn.append(fn)
                    r_size, w_size, f_open, f_close = 0, 0, 0, 0
                    if host in is_write_continuous and htime in is_write_continuous[
                            host] and fd in is_write_continuous[host][htime]:
                        write_continuous = is_write_continuous[host][htime][fd]
                    else:
                        write_continuous = 2

                    if host in is_read_continuous and htime in is_read_continuous[
                            host] and fd in is_read_continuous[host][htime]:
                        read_continuous = is_read_continuous[host][htime][fd]
                    else:
                        read_continuous = 2

                    for f in _fileness:
                        if f[0] == fd:
                            r_size += f[1]
                            w_size += f[2]
                            f_open += f[3]
                            f_close += f[4]
                    file_info.append((fn, fd, r_size, w_size, read_continuous,
                                      write_continuous, f_open, f_close))
                except Exception as e:
                    continue
            # Get the number of files for the operation
            file_count = len(set(_fn))
            cur_val = [
                r_iobw, w_iobw, r_iops, w_iops, open_count, close_count,
                file_count, r_distr, w_distr, file_info
            ]
            if host in ana_data and htime in ana_data[host]:
                old_val = ana_data[host][htime]
                for index, val in enumerate(cur_val):
                    o_val = old_val[index]
                    if index <= 6:
                        cur_val[index] = int(val) + int(o_val)
                    elif 7 <= index <= 8:
                        val = val if val else [0] * 11
                        o_val = o_val if o_val else [0] * 11
                        cur_val[index] = add(val, o_val)
                    elif index == 9:
                        cur_val[index] = val + o_val
                    else:
                        pass
            ana_data[host][htime] = cur_val
    for host, values in ana_data.items():
        if host not in results:
            results[host] = []
        for htime, value in values.items():
            t = htime if isinstance(htime, str) else time2str(htime)
            if 1:
                results[host].append([t] + value)

    return results


def parse_lwfs_client_data(results,
                           host,
                           start='00:00:00',
                           end='23:59:59',
                           format_type='new'):
    #redis_client : redis hash
    if 1:
        try:
            redis_pool = redis.ConnectionPool(host='127.0.0.1',
                                              port='6379',
                                              db=2,
                                              decode_responses=True)
            redis_client = redis.Redis(connection_pool=redis_pool)
            aggregate_data = defaultdict(dict)
            tmp_map = redis_client.hgetall(host)
            if tmp_map != {}:
                fd_fn_map = {host: tmp_map}
            else:
                fd_fn_map = {host: {}}
        except Exception as e:
            print('An error occurred while connecting to redis：', str(e))
            fd_fn_map = {host: {}}
    write_info = {}
    read_info = {}
    rm_fd_fn_map = defaultdict(dict)

    ocount = 0
    rcount = 0
    mcount = 0
    if format_type == 'oldest':
        for host, result in results.items():
            for r in result:
                try:
                    htime = r[0].strip()
                    if htime not in aggregate_data[host]:
                        aggregate_data[host][htime] = defaultdict(list)
                    opinfo = r[1].strip()
                    opinfo = opinfo.split()
                    if 'RELEASE' in opinfo[0]:
                        op = "CLOSE"
                        fd = opinfo[0].split('RELEASE')[1]
                        rm_fd_fn_map[host][fd] = 1
                        if host in fd_fn_map and fd in fd_fn_map[host]:
                            fn = fd_fn_map[host][fd]
                        else:
                            fn = None
                        r = (fd, fn)
                    elif 'OPEN' in opinfo[0]:
                        op = "OPEN"
                        fn = opinfo[1]
                        fd = '00000000'
                        r = (fd, fn)
                        if host not in fd_fn_map:
                            fd_fn_map[host] = {}
                        fd_fn_map[host][fd] = fn
                    elif "maxsize" in opinfo[3]:
                        op = opinfo[0]
                        fd = '00000000'
                        _size = opinfo[5].split("=")[1].strip("'")
                        _size = int(_size.split('.')[0])
                        _offset = 0
                        _count = opinfo[2].split("=")[1].strip("'")
                        _count = int(_count.split('.')[0])
                        r = [fd, _size, _offset, _count]
                        if op == "WRITE":
                            wr = (_size, _offset, _count)
                            if host not in write_info:
                                write_info[host] = {}
                            if fd not in write_info[host]:
                                write_info[host][fd] = {}
                            if htime not in write_info[host][fd]:
                                write_info[host][fd][htime] = []
                            write_info[host][fd][htime].append(wr)
                        elif op == "READ":
                            rr = (_size, _offset, _count)
                            if host not in read_info:
                                read_info[host] = {}
                            if fd not in read_info[host]:
                                read_info[host][fd] = {}
                            if htime not in read_info[host][fd]:
                                read_info[host][fd][htime] = []
                            read_info[host][fd][htime].append(rr)
                    else:
                        print("unknown operation", r)
                        op = None
                        r = None
                    aggregate_data[host][htime][op].append(r)
                except Exception as e:
                    print(str(e))
    elif format_type == 'old':
        for message in results:
            try:
                message = message.strip('[]\n')
                message_info = message.split(",", maxsplit=1)
                htime = message_info[0].strip(" '")
                value = message_info[1].strip(" '")
                # aggregate
                if htime not in aggregate_data[host]:
                    aggregate_data[host][htime] = defaultdict(list)
                opinfo = value.split(' ')
                if 'RELEASE0' in opinfo[0]:
                    op = "CLOSE"
                    fd = opinfo[0][7:]
                    if host in fd_fn_map and fd in fd_fn_map[host]:
                        fn = fd_fn_map[host][fd]
                    else:
                        fn = None
                    r = (fd, fn)
                elif 'OPEN()' in opinfo[0]:
                    op = "OPEN"
                    fd = opinfo[3]
                    #找到fn
                    fn = opinfo[1]
                    fd_fn_map[host][fd] = fn
                    r = (fd, fn)
                elif 'OPEN' in opinfo[0]:
                    op = "OPEN"
                    fd = '00000000'
                    fn = opinfo[1]
                    fd_fn_map[host][fd] = fn
                    r = (fd, fn)
                elif "maxsize" in opinfo[3]:
                    op = opinfo[0]
                    fd = '00000000'
                    float_size = opinfo[5].split("=")[1]
                    _size = int(float_size.split('.')[0])
                    _offset = 0
                    _count = int(opinfo[2].split("=")[1])
                    r = (fd, _size, _offset, _count)
                    if op == "WRITE":
                        wr = (_size, _offset, _count)
                        if host not in write_info:
                            write_info[host] = {}
                        if fd not in write_info[host]:
                            write_info[host][fd] = {}
                        if htime not in write_info[host][fd]:
                            write_info[host][fd][htime] = []
                        write_info[host][fd][htime].append(wr)
                    elif op == "READ":
                        rr = (_size, _offset, _count)
                        if host not in read_info:
                            read_info[host] = {}
                        if fd not in read_info[host]:
                            read_info[host][fd] = {}
                        if htime not in read_info[host][fd]:
                            read_info[host][fd][htime] = []
                        read_info[host][fd][htime].append(rr)
                elif "offset" in opinfo[3]:
                    op = opinfo[0]
                    fd = opinfo[1]
                    _size = int(opinfo[2].split("=")[1])
                    _offset = int(opinfo[3].split("=")[1])
                    _count = int(opinfo[4].split("=")[1])
                    r = (fd, _size, _offset, _count)
                    if op == "WRITE":
                        wr = (_size, _offset, _count)
                        if host not in write_info:
                            write_info[host] = {}
                        if fd not in write_info[host]:
                            write_info[host][fd] = {}
                        if htime not in write_info[host][fd]:
                            write_info[host][fd][htime] = []
                        write_info[host][fd][htime].append(wr)
                    elif op == "READ":
                        rr = (_size, _offset, _count)
                        if host not in read_info:
                            read_info[host] = {}
                        if fd not in read_info[host]:
                            read_info[host][fd] = {}
                        if htime not in read_info[host][fd]:
                            read_info[host][fd][htime] = []
                        read_info[host][fd][htime].append(rr)
                elif "OPEN" in opinfo[3]:
                    op = "OPEN"
                    fn = opinfo[4]
                    fd = opinfo[6]
                    if host not in fd_fn_map:
                        fd_fn_map[host] = {}
                    fd_fn_map[host][fd] = fn
                    r = (fd, fn)
                elif "RELEASE" in opinfo[3]:
                    op = "CLOSE"
                    fd = opinfo[4]
                    if host in fd_fn_map and fd in fd_fn_map[host]:
                        fn = fd_fn_map[host][fd]
                    else:
                        fn = None
                    r = (fd, fn)
                else:
                    op = None
                    r = None
                if op == 'CLOSE':
                    rm_fd_fn_map[host][fd] = 1
                aggregate_data[host][htime][op].append(r)
            except Exception as e:
                #print(str(opinfo))
                pass
    else:
        assert isinstance(results, dict)
        for host, result in results.items():
            for r in result:
                try:
                    htime = r[0]
                    value = r[1]
                    if htime not in aggregate_data[host]:
                        aggregate_data[host][htime] = defaultdict(list)
                    opinfo = value.split()
                    if "offset" in opinfo[3]:
                        op = opinfo[0]
                        fd = opinfo[1]
                        _size = int(opinfo[2].split("=")[1])
                        _offset = int(opinfo[3].split("=")[1])
                        _count = int(opinfo[4].split("=")[1])
                        r = (fd, _size, _offset, _count)
                        if op == "WRITE":
                            wr = (_size, _offset, _count)
                            if host not in write_info:
                                write_info[host] = {}
                            if fd not in write_info[host]:
                                write_info[host][fd] = {}
                            if htime not in write_info[host][fd]:
                                write_info[host][fd][htime] = []
                            write_info[host][fd][htime].append(wr)
                        elif op == "READ":
                            rr = (_size, _offset, _count)
                            if host not in read_info:
                                read_info[host] = {}
                            if fd not in read_info[host]:
                                read_info[host][fd] = {}
                            if htime not in read_info[host][fd]:
                                read_info[host][fd][htime] = []
                            read_info[host][fd][htime].append(rr)
                    elif "OPEN" in opinfo[3]:
                        op = "OPEN"
                        fn = opinfo[4]
                        fd = opinfo[6]
                        if host not in fd_fn_map:
                            fd_fn_map[host] = {}
                        fd_fn_map[host][fd] = fn
                        r = (fd, fn)
                    elif "RELEASE" in opinfo[3]:
                        op = "CLOSE"
                        fd = opinfo[4]
                        rm_fd_fn_map[host][fd] = 1
                        if host in fd_fn_map and fd in fd_fn_map[host]:
                            fn = fd_fn_map[host][fd]
                        else:
                            fn = None
                        r = (fd, fn)
                    else:
                        op = None
                        r = None
                    aggregate_data[host][htime][op].append(r)
                except Exception as e:
                    print(str(e))
    is_write_continuous = analyse_continuous(write_info)
    is_read_continuous = analyse_continuous(read_info)
    results = analyze_data(aggregate_data, is_write_continuous,
                           is_read_continuous, fd_fn_map, start, end)
    for host, value in rm_fd_fn_map.items():
        for fd, rm_flag in value.items():
            if rm_flag == 1:
                if fd in fd_fn_map[host]:
                    fd_fn_map[host].pop(fd)
    del rm_fd_fn_map
    for hosttmp in fd_fn_map:
        try:
            tmp = list(fd_fn_map[hosttmp])
            for key in tmp:
                if fd_fn_map[hosttmp][key] == None or key == '00000000':
                    fd_fn_map[hosttmp].pop(key)
        except Exception:
            pass
    if fd_fn_map[host] != {}:
        try:
            redis_client.hmset(host, fd_fn_map[host])
        except Exception:
            print('redis update error')
    return results
