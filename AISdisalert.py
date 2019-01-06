# -*- coding:utf-8 -*-
from configparser import ConfigParser
from pymongo import MongoClient
import time
from geopy.distance import distance
from shapely.geometry import LineString
from os import listdir


def dis_alert(server, dabport, dbdb, dblogcl, dbshipcl, start_time, end_time, dis_dir, leng_times):
    client = MongoClient(host=server, port=dabport)
    collection1 = client[dbdb][dbshipcl]
    collection2 = client[dbdb][dblogcl]
    # 时间换算成秒
    time_s = int(time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S")))
    time_e = int(time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M:%S")))
    time_o = time_s
    file_num = 1
    while time_o <= time_e:
        # 时间转化成可读格式
        time_read = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_o))
        print('%s :' % time_read)
        alert_ships = []
        # 找出在此刻所有的船
        all_ship = collection2.find({'timeint': time_o})
        for one_ship in all_ship:
            print('searching %s' % one_ship['ship'])
            one_ship_info = collection1.find_one({'ship_code': one_ship['ship']})
            if not one_ship_info:
                continue
            # 拿出距离小于3000米的船
            close_ships = collection2.find(
                {'location': {'$near': {'$geometry': one_ship['location'], '$maxDistance': dis_dir}},
                 'timeint': time_o, 'ship': {'$ne': one_ship['ship']}})

            one_ship_length = one_ship_info['length']

            for near_ship in close_ships:
                near_ship_info = collection1.find_one({'ship_code': near_ship['ship']})
                if not near_ship_info:
                    continue
                near_ship_length = near_ship_info['length']
                one_ship_geo = [one_ship['latitude'], one_ship['longitude']]
                near_ship_geo = [near_ship['latitude'], near_ship['longitude']]
                # 计算距离
                dist = distance(one_ship_geo, near_ship_geo).km * 1000

                # 如果距离小于船长之和的leng_times倍
                if dist <= leng_times * (one_ship_length + near_ship_length):

                    # 输出的文本
                    log_con_fi = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (
                        one_ship['time'], one_ship['ship'], one_ship['longitude'], one_ship['latitude'],
                        one_ship['velocity'], one_ship['direction'], one_ship_info['length'], one_ship_info['width'],
                        one_ship_info['type'][:-1], near_ship['ship'], near_ship['longitude'], near_ship['latitude'],
                        near_ship['velocity'], near_ship['direction'], near_ship_info['length'],
                        near_ship_info['width'],
                        near_ship_info['type'][:-1])

                    dire_diff = abs(one_ship['direction'] - near_ship['direction']) if abs(
                        one_ship['direction'] - near_ship['direction']) < 180 else (
                            360 - abs(one_ship['direction'] - near_ship['direction']))

                    cent_poi = LineString([(one_ship['longitude'], one_ship['latitude']),
                                           (near_ship['longitude'], near_ship['latitude'])]).centroid.wkt
                    log_con = (log_con_fi + ',' + '%.2f' + ',' + '%.2f' + ',' + str(cent_poi) + '\n') % (
                        dist, dire_diff)

                    alert_file_name = '%s_%s.txt' % (one_ship['ship'], near_ship['ship'])
                    if (near_ship['ship'], one_ship['ship']) in alert_ships:
                        continue
                    alert_ships.append((one_ship['ship'], near_ship['ship']))

                    with open(alert_file_name, 'a', encoding="utf-8") as alert_log:
                        alert_log.write(log_con)
                        alert_log.close()
                    file_num += 1
        time_o += 1


def dis_alert_1(server, dabport, dbdb, dblogcl, dbshipcl, start_time, end_time, dis_dir, leng_times, time_next):
    client = MongoClient(host=server, port=dabport)
    collection1 = client[dbdb][dbshipcl]
    collection2 = client[dbdb][dblogcl]
    # 时间换算成秒
    time_s = int(time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S")))
    time_e = int(time.mktime(time.strptime(end_time, "%Y-%m-%d %H:%M:%S")))
    time_o = time_s
    file_num = 1
    while time_o <= time_e:
        # 时间转化成可读格式
        time_read = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time_o))
        print('%s :' % time_read)
        # 找出在此刻所有的船
        all_ship = collection2.find({'timeint': time_o}, no_cursor_timeout=True)
        for one_ship in all_ship:
            print('searching %s' % one_ship['ship'])
            one_ship_info = collection1.find_one({'ship_code': one_ship['ship']})
            one_ship_geo = [one_ship['latitude'], one_ship['longitude']]
            if not one_ship_info:
                continue
            # 拿出距离小于3000米的船
            close_ships = collection2.find(
                {'timeint': time_o, 'location': {'$near': {'$geometry': one_ship['location'], '$maxDistance': dis_dir}},
                 'ship': {'$ne': one_ship['ship']}}, no_cursor_timeout=True)

            one_ship_length = one_ship_info['length']

            for near_ship in close_ships:
                near_ship_info = collection1.find_one({'ship_code': near_ship['ship']})
                if not near_ship_info:
                    continue
                near_ship_length = near_ship_info['length']
                near_ship_geo = [near_ship['latitude'], near_ship['longitude']]
                # 计算距离
                dist = distance(one_ship_geo, near_ship_geo).km * 1000

                # 如果距离小于船长之和的leng_times倍
                if dist <= leng_times * (one_ship_length + near_ship_length):
                    # 如果已经计算过了，则跳过
                    alert_file_name_p = '%s_%s.txt' % (near_ship['ship'], one_ship['ship'])
                    if alert_file_name_p in listdir('alertlogs'):
                        continue
                # 输出的文本
                    log_con_fi = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (
                        one_ship['time'], one_ship['ship'], one_ship['longitude'], one_ship['latitude'],
                        one_ship['velocity'], one_ship['direction'], one_ship_info['length'], one_ship_info['width'],
                        one_ship_info['ship_type'], near_ship['ship'], near_ship['longitude'], near_ship['latitude'],
                        near_ship['velocity'], near_ship['direction'], near_ship_info['length'],
                        near_ship_info['width'],
                        near_ship_info['ship_type'])

                    dir_a = abs(float(one_ship['direction']) - float(near_ship['direction']))
                    dire_diff = dir_a if dir_a < 180 else (360 - dir_a)

                    cent_poi = LineString([(one_ship['longitude'], one_ship['latitude']),
                                           (near_ship['longitude'], near_ship['latitude'])]).centroid.wkt
                    log_con = (log_con_fi + ',' + '%.2f' + ',' + '%.2f' + ',' + str(cent_poi) + '\n') % (
                        dist, dire_diff)

                    alert_file_name = 'alertlogs\\%s_%s.txt' % (one_ship['ship'], near_ship['ship'])

                    with open(alert_file_name, 'a', encoding="utf-8") as alert_log:
                        alert_log.write(log_con)
                        alert_log.close()
                    file_num += 1
        collection2.delete_many({'timeint': time_o})
        time_o += time_next


if __name__ == '__main__':
    cf = ConfigParser()
    cf.read('AISconfig.conf', encoding="utf-8")
    db_db = cf.get('AIS', 'dbdatabase')
    db_ship_cl = cf.get('AIS', 'dbshipcollection')
    db_log_cl = cf.get('AIS', 'dblogcollection')
    db_port = cf.getint('AIS', 'dbport')
    db_server = cf.get('AIS', 'dbserver')
    time_start = cf.get('AIS', 'starttime')
    time_end = cf.get('AIS', 'endtime')
    len_times = cf.getint('AIS', 'lentimes')
    time_final = cf.getint('AIS', 'timefinal')
    dis_dic = cf.getint('AIS', 'disare')

    dis_alert_1(db_server, db_port, db_db, db_log_cl, db_ship_cl, time_start, time_end, dis_dic, len_times, time_final)
