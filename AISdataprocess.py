# -*- coding:utf-8 -*-
from configparser import ConfigParser
from pymongo import MongoClient
import scipy.signal as signal
from scipy.interpolate import interp1d
import numpy as np
import shapely.geometry
import time
from math import fabs


# 转化地理数据格式
def get_geojson(longitude, latitude):
    wkt_data = shapely.geometry.Point(longitude, latitude)
    geo_data = shapely.geometry.mapping(wkt_data)
    return geo_data


# 角度插值
def interpolatedegrees(start, end, binangle):
    amount = np.arange(0, 1, (1 / binangle))
    dif = fabs(end - start)
    if dif > 180:
        if end > start:
            start += 360
        else:
            end += 360

            # Interpolate it
    value = (start + ((end - start) * amount))

    # Wrap it
    rzero = 360

    arr = np.where((value >= 0) & (value <= 360), (value), (value % rzero))
    dirlist = arr.tolist()
    dirlist.append(end)
    dirlistf = []
    for item in dirlist:
        dirlistf.append('%.2f' % item)
    return dirlistf


def process_data(server, dabport, dbdb, dblogcl, dbshipcl, time_filter):
    client = MongoClient(host=server, port=dabport)
    collection1 = client[dbdb][dbshipcl]
    collection2 = client[dbdb][dblogcl]
    data = collection1.find({}, no_cursor_timeout=True).batch_size(1)

    for item in data:
        # 取出一条船的数据
        print('processing ship No.%s' % item['ship_code'])
        try:
            data_log = collection2.find({'ship': item['ship_code']}, no_cursor_timeout=True)
            data_list = []
            for ij in data_log:
                data_list.append(ij)

            if len(data_list) < 2:
                print('数据不足2条')
                continue
            # 拿出相邻的两条数据进行比较
            i = 0
            while i < (len(data_list) - 1):
                time0 = data_list[i]['timeint']
                time1 = data_list[i + 1]['timeint']
                if abs(time0 - time1) <= 1:
                    i += 1
                    continue
                if abs(time1 - time0) >= time_filter:
                    i += 1
                    continue

                # 拿出速度、时间、经纬度信息
                v_list = [data_list[i]['velocity'], data_list[i + 1]['velocity']]
                long_list = [data_list[i]['longitude'], data_list[i + 1]['longitude']]
                lati_list = [data_list[i]['latitude'], data_list[i + 1]['latitude']]
                time_list = [time0, time1]

                # 插值
                x = np.array(time_list)
                y1 = np.array(v_list)
                y2 = np.array(long_list)
                y3 = np.array(lati_list)
                f1 = interp1d(x, y1, kind='linear')
                f2 = interp1d(x, y2, kind='linear')
                f3 = interp1d(x, y3, kind='linear')
                x_new = np.linspace(time_list[0], time_list[1], num=(abs(time_list[1] - time_list[0]) + 1))
                y1_new = f1(x_new)
                y2_new = f2(x_new)
                y3_new = f3(x_new)

                dire_list = interpolatedegrees(data_list[i]['direction'], data_list[i + 1]['direction'],
                                               abs(time_list[1] - time_list[0]))

                # 更新数据库
                for time_ad, v_ad, long_ad, lati_ad, dire_ad in zip(x_new, y1_new, y2_new, y3_new, dire_list):
                    if time_ad in x:
                        continue
                    else:
                        data_ad = {'time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_ad)),
                                   'timeint': time_ad, 'ship': item['ship_code'],
                                   'longitude': long_ad, 'latitude': lati_ad, 'location': get_geojson(long_ad, lati_ad),
                                   'velocity': ('%.2f' % v_ad), 'direction': dire_ad}
                        collection2.update({'timeint': time_ad, 'ship': item['ship_code']}, data_ad, True)
                i += 1

        except Exception as e:
            print(e)
        data_log.close()
    data.close()


def smooth_data(server, dabport, dbdb, dblogcl, dbshipcl):
    client = MongoClient(host=server, port=dabport)
    collection1 = client[dbdb][dbshipcl]
    collection2 = client[dbdb][dblogcl]
    data = collection1.find({}, no_cursor_timeout=True)
    for i in data:
        # 取出一条船的数据
        print('smoothing ship No.%s' % i['ship_code'])
        try:
            data_log = collection2.find({'ship': i['ship_code']}, no_cursor_timeout=True)
            data_list = []
            for ij in data_log:
                data_list.append(ij)

            if len(data_list) < 2:
                print('数据不足2条')
                continue

            # 拿出速度、时间、经纬度信息
            v_list = []
            long_list = []
            lati_list = []
            time_list = []
            for j in data_list:
                # print(type(j))
                v_list.append(j['velocity'])
                long_list.append(j['longitude'])
                lati_list.append(j['latitude'])
                # 将时间转化为秒数加入列表
                time_list.append(int(time.mktime(time.strptime(j['time'], "%Y-%m-%d %H:%M:%S"))))
            # 滤波
            vlist_new = signal.medfilt(np.array(v_list, dtype='float64'), 5)
            longlist_new = signal.medfilt(np.array(long_list, dtype='float64'), 5)
            latilist_new = signal.medfilt(np.array(lati_list, dtype='float64'), 5)

            # 更新数据库
            flag = 0
            for item_v, item_long, item_lati in zip(vlist_new, longlist_new, latilist_new):
                collection2.update({'_id': data_list[flag]['_id']},
                                   {'$set': {'velocity': item_v, 'longitude': item_long, 'latitude': item_lati,
                                             'location': get_geojson(item_long, item_lati)}})

                flag += 1
            data_log.close()

        except Exception as e:
            print(e)
    data.close()


if __name__ == '__main__':
    cf = ConfigParser()
    cf.read('AISconfig.conf', encoding="utf-8")
    db_db = cf.get('AIS', 'dbdatabase')
    db_ship_cl = cf.get('AIS', 'dbshipcollection')
    db_log_cl = cf.get('AIS', 'dblogcollection')
    db_port = cf.getint('AIS', 'dbport')
    time_split = cf.getint('AIS', 'timesplit')
    db_server = cf.get('AIS', 'dbserver')
    smooth_data(db_server, db_port, db_db, db_log_cl, db_ship_cl)
    process_data(db_server, db_port, db_db, db_log_cl, db_ship_cl, time_split)
