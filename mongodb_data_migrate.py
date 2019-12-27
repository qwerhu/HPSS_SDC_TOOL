from datetime import datetime
from pymongo import MongoClient

#远程数据库
client = MongoClient('10.10.1.127',27017)
db=client['modeldb']
db1 = client['scadadb_history']

#本地数据库
client1 = MongoClient('127.0.0.1',27017)
db2 = client1['history_data']


def insert_data_local(collection_name,data):
    collection_table = db2[collection_name]
    for i in data:
        condition = {'id':i.get('id'),'timetag':i.get('timetag'),'starttime':i.get('starttime'),'endtime':i.get('endtime')}
        data_one = collection_table.find_one(condition)
        if data_one:
            print('删除了数据:%s ' % (str(i.get('id'))))
            collection_table.delete_one(condition)
        else:
            # print('你好')
            # x = collection_table.insert_one(i)
            # print(x)
            pass
    n =50
    for b in [data[i:i+n] for i in range(0,len(data),n)]:
        collection_table.insert_many(b)


def read_data_remote(collection_name):
    gb = []
    collection_table = db1[collection_name]
    items = collection_table.find({},{'_id':0}).limit(5000)
    for i in items:
        gb.append(i)
    return gb


if __name__ == '__main__':
    # remote_data = read_data_remote('t_scada_his_3600')
    # # print(remote_data[0])
    # # print(len(remote_data))
    # insert_data_local('t_scada_his_3600',data=remote_data)

    x = datetime.fromtimestamp(1554847200)
    y = datetime.fromtimestamp(1554840060)
    print(x,y)
    x= datetime.fromtimestamp(1554840000)
    y = datetime.fromtimestamp(1554847200)
    print(x,y)

    z = datetime.timestamp(x)
    print(z)
    a=[1,2,3,4,5,6,7,89,12,123,13,2,321,3,12,3,21,3,1,'nihao']
    for b in [a[i:i+6] for i in range(0,len(a),6)]:
        print(b)

    x = sum(range(1,100))
    print(x)
