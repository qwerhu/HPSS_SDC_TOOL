"""
将csv中的SCADA数据导入到mongodb中
"""
import time
import pandas as pd
import logging
from os import path
from utils import data_util


def main(csv_path):
    if not path.exists(csv_path):
        logging.error('输入的路径不存在：{}'.format(csv_path))
        return
    # 读取数据
    df = pd.read_csv(csv_path, encoding='gb2312')
    df['time'] = df['DATETIME'].apply(_to_time)
    df = df.sort_values('time')
    if len(df) > 0:
        ids = df['ITEMNAME'].unique()
        for _id in ids:
            temp_df = df[df['ITEMNAME'] == _id]
            data_util.save_history_cleaning(_id, temp_df, 'time', 'DATAVALUE')


def _to_time(v):
    strs = v.split(' ')
    if len(strs) == 1:
        d = v
        t = '00:00:00'
    elif len(strs) == 2:
        d = strs[0]
        t = strs[1]
    else:
        print('无效的时间')
        return None
    if len(t) == 2:
        t += ':00'
    df = '/'.join(['0{}'.format(x) if len(x) < 2 else x for x in d.split('/')])
    tf = ':'.join(['0{}'.format(x) if len(x) < 2 else x for x in t.split(':')])
    r = time.strptime('{} {}'.format(df, tf), '%Y/%m/%d %H:%M:%S')
    return time.mktime(r)
