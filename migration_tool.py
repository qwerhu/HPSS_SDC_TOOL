import sys
from pymongo import MongoClient


def conn_mongodb(ip,port):
    client = MongoClient(ip, int(port))
    return client


def migrated_database(client,databasename,collection_name):
    database_object = client[databasename]
    tabel_object = database_object[collection_name]
    return tabel_object


def acquire_data(tabel_object,id=None,timetag=None,stime=None,etime=None):
    gb = []
    value1 = stime and etime
    value2 = id and timetag
    if value1:
        if value2 and value1:
            items = tabel_object.find({'id': id, 'timetag': timetag,'endtime':{'$lte':etime},'starttime':{'$gte':stime}}, {'_id': 0})
        elif value1 and timetag:
            items = tabel_object.find({'timetag': timetag, 'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}}, {'_id': 0})
        elif value1 and id :
            items = tabel_object.find({'id':id, 'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}}, {'_id': 0})
        else:
            items = tabel_object.find({'endtime': {'$lte': etime}, 'starttime': {'$gte': stime}}, {'_id': 0})
        for i in items:
            gb.append(i)

    else:
        if value2:
            items = tabel_object.find({'id':id,'timetag':timetag},{'_id':0})
        elif id:
            items = tabel_object.find({'id': id},{'_id':0})
        elif timetag:
            items = tabel_object.find({'timetag': timetag},{'_id':0})
        else:
            items = tabel_object.find({}, {'_id': 0})

        for i in items:
            gb.append(i)

    return gb


def to_migration_database(client,databasename,collection_name,data):

    db = client[databasename]
    table_1 = db[collection_name]

    #将迁移过来的数据进行操作，当目的数据库的表中的数据进行修改，没有就进行插入操作，相同则不变。

    try:
        for i in data:
            condition = {'id': i.get('id'), 'timetag': i.get('timetag'),'starttime':i.get('starttime'),'endtime':i.get('endtime')}
            one_data = table_1.find_one(condition)
            if one_data:
                print('删除了数据:%s '%(str(i.get('id'))))
                table_1.delete_one(condition)
            else:
                pass
        n = 50
        for b in [data[i:i + n] for i in range(0, len(data), n)]:
            table_1.insert_many(b)
    except:
        pass


if __name__ == '__main__':
    #手动设置参数
    # ip = str(input('请输入mongodb的ip地址：'))
    # print(ip)
    # port = input('请输入mongodb的端口号：')
    # print(port)
    # databasename_migrated = input('请输入被迁移的数据库名字：')
    # print(databasename_migrated)
    # collection_migrated = input('请输入被迁移的集合名字：')
    # print(collection_migrated)
    # timetag = int(input('请输入被迁移表的timetag属性：'))
    # print(timetag)
    # id = input('请输入被迁移表的id属性：')
    # print(id)
    # databasename_migrate = input('请输入迁移的数据库名字：')
    # print(databasename_migrate)
    # collection_migrate = input('请输入迁移的集合名字：')
    # print(collection_migrate)


    #创建连接对象
    client = conn_mongodb('127.0.0.1','27017')

    #得到相应的迁移表
    tabel_object = migrated_database(client,databasename='history_data',collection_name='t_scada_his_300')

    #根据指标来获得相应的数据,列表中嵌套字典的数据
    # acquire_datas = acquire_data(tabel_object,timetag=1554796800,id='H_ME10004411_2_HBJTYWTYJL')
    acquire_datas = acquire_data(tabel_object)

    #将指定的数据迁移到去目的数据库里面（使相应表中的数据保持唯一）
    to_migration_database(client,databasename='new_history_data',collection_name='t_scada_his_300',data=acquire_datas)


