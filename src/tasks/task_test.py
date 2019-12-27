import time
from random import random
from data import history_limit

FORMAT_TIME = '%Y%m%d%H%M%S'

def start(*args, **kwargs):
    _id = args[0] #kwargs.get('id')
    s = args[1] #kwargs.get('s')
    e = args[2] # kwargs.get('e')
    m = args[3] #.get('m', 'down')
    s = int(time.mktime(time.strptime(s, FORMAT_TIME))) if s is not None else None
    e = int(time.mktime(time.strptime(e, FORMAT_TIME))) if e is not None else None
    _test_limit(_id, s, e, m)


def _test_limit(_id, s=None, e=None, m='up'):
    print('开始执行异常数据模拟')
    if s is None and e is None:
        e = int(time.time())
        s = e - 60 * 60
    if s is None or e is None or _id is None:
        print('开始时间或者结束时间参数错误')
    his_raw = history_limit.items(s, e, Ids=[_id])
    for item in his_raw:
        item['value'] = (item['min'] - 100 * random()) if m == 'down' else (item['max'] + 100 * random())
        history_limit.upsert('r', _id, item['value'], item['time'], item['min'], item['max'])

