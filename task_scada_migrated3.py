import time
from pymongo import MongoClient

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args,**kwargs):
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
    ip2 = args[10]
    port2 = args[11]

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
    _deal(ip=ip, port=port, databasename_migrated=databasename_migrated, collection_migrated=collection_migrated, databasename_migrate=databasename_migrate, collection_migrate=collection_migrate, id=id, timetag=timetag, stime=stime, etime=etime,ip2=ip2,port2=port2)


def _deal(ip, port,databasename_migrated, collection_migrated, databasename_migrate, collection_migrate, id, timetag, stime, etime,ip2,port2):

    #连接对象
    client = MongoClient(ip, int(port))
    print(client[databasename_migrated][collection_migrated].find({}).count())
    # 得到相应的迁移表
    database_object = client[databasename_migrated]
    tabel_object1 = database_object[collection_migrated]

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
    client1 = MongoClient(ip2,int(port2))
    db = client1[databasename_migrate]
    table_1 = db[collection_migrate]
    # 将迁移过来的数据进行操作，当目的数据库的表中的数据进行修改，没有就进行插入操作，相同则不变。

    try:
        for i in gb:
            condition = {'id': i.get('id'), 'timetag': i.get('timetag'), 'starttime': i.get('starttime'),
                         'endtime': i.get('endtime')}
            one_data = table_1.find_one(condition)
            if one_data:
                print('删除了数据:%s ' % (str(i.get('id'))))
                table_1.delete_one(condition)
            else:
                pass
        n = 80
        for b in [gb[i:i + n] for i in range(0, len(gb), n)]:
            table_1.insert_many(b)
    except:
        pass


