"""
报警模拟
"""
import time
from data import history

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args, **kwargs):
    _type = args[0]
    _id = args[1]
    s_time = args[2]
    e_time = args[3]
    value = args[4]
    s_time = int(time.mktime(time.strptime(s_time, FORMAT_TIME))) if s_time is not None else None
    e_time = int(time.mktime(time.strptime(e_time, FORMAT_TIME))) if e_time is not None else None
    _deal(_type, _id, s_time, e_time, value)


def _deal(_type, _id, s_time, e_time, value):
    print('开始执行scada报警数据模拟')
    if type(value) == str:
        value = float(value)
    if s_time is None or e_time is None or _id is None or value is None:
        print('参数错误')

    his_raw = history.items(s_time, e_time, Ids=[_id])
    for item in his_raw:
        v = float(item['value'])
        if _type == 'zero':
            #模拟零值
            item['value'] = 0
        elif _type == 'lower':
            # 模拟下限报警
            item['value'] = value - (v/10)
        elif _type == 'unchanged':
            # 平值
            item['value'] = value
        history.upsert('r', item['id'], item['value'], item['time'])

