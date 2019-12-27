import time
from pymongo import MongoClient

FORMAT_TIME = '%Y%m%d%H%M%S'

def start(*args):
    ip1 = args[0] #mongodb的ip地址
    port1 = args[1] #mongodb的端口号
    databasename_migrated = args[2] #要被迁移的数据库名称
    collection_migrated = args[3] #要被迁移的相应数据库中对应的表（集合）
    databasename_migrate = args[4] #迁移到相应数据库的名称中
    collection_migrate = args[5] #迁移到相应的databasename_migrate下的表中
    id = args[6] #历史数据中的id属性
    timetag = args[7] #历史数据中的timetag属性
    stime = args[8] #历史数据中的stime（开始时间）属性
    etime = args[9]#历史数据中的etime（结束时间）属性

    if ip1:
        ip = ip1
    else:
        ip = '127.0.0.1'

    if port1:
        port = port1
    else:
        port = '27017'

    timetag = int(time.mktime(time.strptime(timetag, FORMAT_TIME))) if timetag is not None else None
    stime = int(time.mktime(time.strptime(stime, FORMAT_TIME))) if stime is not None else None
    etime = int(time.mktime(time.strptime(etime, FORMAT_TIME))) if etime is not None else None
    deal(_ip=ip,_port=port,_databasename_migrated=databasename_migrated,_collection_migrated=collection_migrated,_databasename_migrate=databasename_migrate,_collection_migrate=collection_migrate,_id=id,_timetag=timetag,_stime=stime,_etime=etime)

def conn_mongodb(ip2, port2):
    client = MongoClient(ip2, int(port2))
    return client

def migrated_database(client, databasename1, collection_name1):
    database_object = client[databasename1]
    tabel_object = database_object[collection_name1]
    return tabel_object

def acquire_data(tabel_object1, id, timetag, stime, etime):
    gb = []
    value1 = stime and etime
    value2 = id and timetag
    if value1:
        if value2 and value1:
            items = tabel_object1.find(
                {'id': id, 'timetag': timetag, 'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}},
                {'_id': 0})
        elif value1 and timetag:
            items = tabel_object1.find(
                {'timetag': timetag, 'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}}, {'_id': 0})
        elif value1 and id:
            items = tabel_object1.find({'id': id, 'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}},
                                      {'_id': 0})
        else:
            items = tabel_object1.find({'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}}, {'_id': 0})

        for i in items:
            gb.append(i)
    else:
        if value2:
            items = tabel_object1.find({'id': id, 'timetag': timetag}, {'_id': 0})
        elif id:
            items = tabel_object1.find({'id': id}, {'_id': 0})
            print(items)
        elif timetag:
            items = tabel_object1.find({'timetag': timetag}, {'_id': 0})
        else:
            items = tabel_object1.find({}, {'_id': 0})
        for i in items:
            gb.append(i)

    return gb

def to_migration_database(client, databasename, collection_name, data):
    db = client[databasename]
    table_1 = db[collection_name]
    # 将迁移过来的数据进行操作，当目的数据库的表中的数据进行修改，没有就进行插入操作，相同则不变。

    try:
        for i in data:
            condition = {'id': i.get('id'), 'timetag': i.get('timetag'), 'starttime': i.get('starttime'),
                         'endtime': i.get('endtime')}
            one_data = table_1.find_one(condition)
            if one_data:
                print('删除了数据:%s ' % (str(i.get('id'))))
                table_1.delete_one(condition)
            else:
                pass
        n = 80
        for b in [data[i:i + n] for i in range(0, len(data), n)]:
            table_1.insert_many(b)
    except:
        pass


def deal(_ip,_port,_databasename_migrated,_collection_migrated,_databasename_migrate,_collection_migrate,_id,_timetag,_stime,_etime):
    ip_1 = _ip
    port_2 = _port
    databasename1 =_databasename_migrated
    collection_name1 =_collection_migrated
    id3=_id
    timetag1 = _timetag
    stime1 = _stime
    etime1 = _etime
    databasename2 = _databasename_migrate
    collection_name2 =_collection_migrate

    # 创建连接对象
    # client = conn_mongodb(ip2=ip_1,port2=port_2)
    client = MongoClient(ip_1, int(port_2))
    database_object = client[databasename1]
    tabel_object = database_object[collection_name1]
    print(tabel_object.find({}).count())
    #得到相应的迁移表
    tabel_object = migrated_database(client, databasename1=databasename1,collection_name1=collection_name1)
    # 根据指标来获得相应的数据,列表中嵌套字典的数据
    acquire_datas = acquire_data(tabel_object1=tabel_object,id=id3,timetag=timetag1,stime=stime1,etime=etime1)
    # 将指定的数据迁移到去目的数据库里面（使相应表中的数据保持唯一）
    to_migration_database(client, databasename=databasename2 ,collection_name=collection_name2,data=acquire_datas)

if __name__ == '__main__':
   start('127.0.0.1', '27017', 'history_data', 't_scada_his_3600 ', 'new_history_data', 't_scada_his_3600 ',None,None,None,None)
    # deal('127.0.0.1', '27017', 'history_data', 't_scada_his_3600 ', 'new_history_data', 't_scada_his_3600 ',None,None,None,None)
    # client = conn_mongodb(ip='127.0.0.1',port='27017')
    # #得到相应的迁移表
    # tabel_object = migrated_database(client, databasename='history_data',collection_name='t_scada_his_3600')
    # # 根据指标来获得相应的数据,列表中嵌套字典的数据
    # acquire_datas = acquire_data(tabel_object)
    # # 将指定的数据迁移到去目的数据库里面（使相应表中的数据保持唯一）
    # to_migration_database(client, databasename='new_history_data' ,collection_name='t_scada_his_3600 ',data=acquire_datas)