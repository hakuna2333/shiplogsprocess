# -*- coding:utf-8 -*-
from configparser import ConfigParser
import re
from pymongo import MongoClient
import shapely.geometry
import time


# 将经纬度转换成geometry格式数据
def get_geojson(longitude, latitude):
    wkt_data = shapely.geometry.Point(longitude, latitude)
    geo_data = shapely.geometry.mapping(wkt_data)
    return geo_data


# 格式化数据
def log_data_format(server, dabport, dbdb, dblogcl, dbshipcl, file, velocity_filter):
    data_dict = {}
    client = MongoClient(host=server, port=dabport)
    log_collection = client[dbdb][dblogcl]
    ship_collection = client[dbdb][dbshipcl]
    indexlist = log_collection.index_information()

    if 'ship_1' not in indexlist:
        log_collection.create_index('ship')
    if 'timeint_1' not in indexlist:
        log_collection.create_index('timeint')
    if 'location_2dsphere' not in indexlist:
        log_collection.create_index([('location', '2dsphere')])

    with open(file, 'r', encoding='utf-8') as f:
        i = 0
        while True:
            data = f.readline().split(',')  # 整行读取数据并分离开
            if data == ['']:
                break
            # 构造数据库文档
            # 船舶类型筛选
            if not ship_collection.find_one({'ship_code': int(data[1])}):
                continue

            # 速度筛选
            if float(data[4]) < velocity_filter:
                continue

            time_int = int(time.mktime(time.strptime(data[0], "%Y-%m-%d %H:%M:%S")))
            data_dict['_id'] = i
            data_dict['time'] = data[0]
            data_dict['timeint'] = time_int
            data_dict['ship'] = int(data[1])
            data_dict['longitude'] = float(data[2])
            data_dict['latitude'] = float(data[3])
            data_dict['location'] = get_geojson(float(data[2]), float(data[3]))
            data_dict['velocity'] = float(data[4])
            data_dict['direction'] = float(data[5])

            # 存入数据库
            log_collection.insert_one(data_dict)
            i += 1
    f.close()


def ship_data_format(server, dabport, dbdb, dbcl, file, ship_filter, length_filter_i, length_filter_a, width_filter_a):
    data_dict = {}
    client = MongoClient(host=server, port=dabport)
    collection = client[dbdb][dbcl]
    collection.create_index('ship_code')
    with open(file, 'r', encoding='utf-8') as f:
        i = 0
        while True:
            data = f.readline().split(',')  # 整行读取数据并分离开
            if data == ['']:
                break
            # 构造数据库文档
            # 用船舶类型筛选
            if data[2][:-1] in ship_filter:
                continue


            data_dict['_id'] = i
            data_dict['ship_code'] = int(data[0])
            anay = data[1].split(' ')
            a = re.search(r'[0-9]+', anay[0]).group()
            b = re.search(r'[0-9]+', anay[1]).group()
            c = re.search(r'[0-9]+', anay[2]).group()
            d = re.search(r'[0-9]+', anay[3]).group()

            data_dict['length'] = int(a) + int(b)
            data_dict['width'] = int(c) + int(d)
            # 用船长筛选
            if data_dict['length'] < length_filter_i or data_dict['length'] > length_filter_a:
                continue
            # 用船宽筛选
            if data_dict['width'] > width_filter_a:
                continue

            data_dict['ship_type'] = data[2][:-1]

            # 存入数据库
            collection.insert_one(data_dict)
            i += 1
        f.close()


if __name__ == '__main__':
    cf = ConfigParser()
    cf.read('AISconfig.conf', encoding="utf-8")
    log_file_name = cf.get('AIS', 'log_filename')
    ship_record = cf.get('AIS', 'ship_file')
    db_db = cf.get('AIS', 'dbdatabase')
    db_log_cl = cf.get('AIS', 'dblogcollection')
    db_ship_cl = cf.get('AIS', 'dbshipcollection')
    db_port = cf.getint('AIS', 'dbport')
    db_server = cf.get('AIS', 'dbserver')
    length_min = cf.getint('AIS', 'lengthmin')
    length_max = cf.getint('AIS', 'lengthmax')
    width_max = cf.getint('AIS', 'widthmax')
    speed_min = cf.getint('AIS', 'speedmin')
    shipless_list = cf.get('AIS', 'shiplesslist').split(',')
    print('importing %s' % ship_record)
    ship_data_format(db_server, db_port, db_db, db_ship_cl, ship_record, shipless_list, length_min, length_max,
                     width_max)
    print('importing %s' % log_file_name)
    log_data_format(db_server, db_port, db_db, db_log_cl, db_ship_cl, log_file_name, speed_min)