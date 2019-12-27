import time
from data import history, historycl

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args, **kwargs):
    _type = args[0] or 'his'
    _id = args[1]
    s_time = args[2]
    e_time = args[3]
    coffee = args[4]
    s_time = int(time.mktime(time.strptime(s_time, FORMAT_TIME))) if s_time is not None else None
    e_time = int(time.mktime(time.strptime(e_time, FORMAT_TIME))) if e_time is not None else None
    _deal(_type, _id, s_time, e_time, coffee)


def _deal(_type, _id, s_time, e_time, coffee):
    print('开始执行scada历史数据系树转换')
    if s_time is None and e_time is None:
        e_time = int(time.time())
        s_time = e_time - 60*60
    if type(coffee) == str:
        coffee = float(coffee)
    if s_time is None or e_time is None or _id is None or coffee is None:
        print('参数错误')
    if _type == 'cleaning':
        his_raw = historycl.items(s_time, e_time, Ids=[_id])
    else:
        his_raw = history.items(s_time, e_time, Ids=[_id])
    print('修改数据长度：{}'.format(len(his_raw)))
    for item in his_raw:
        item['value'] = float(item['value']) * coffee
        print('id: {}  时间：{}   值：{}'.format(item['id'], item['time'], item['value']))
        if _type == 'cleaning':
            historycl.upsert('r', item['id'], item['value'], item['time'])
        else:
            history.upsert('r', item['id'], item['value'], item['time'])

