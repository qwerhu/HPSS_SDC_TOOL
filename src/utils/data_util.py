# coding=utf-8
import ctx
import time
import pandas as pd
import numpy as np
import utils
from data import historycl


def save_history_cleaning(_id, df, field_time='time', field_value='value'):
    """ 将DataFrame里面的数据保存到清洗表 """
    _db = ctx.hmdb[historycl.COLLECTION_NAME]
    _interval = historycl.INTERVAL or 60
    _padding = (historycl.MAX_BUCKET or 120) * _interval
    temp_df = df.sort_values(field_time).copy()
    temp_df.index = temp_df[field_time].apply(lambda x: x+_interval-1 - (x+_interval-1) % _interval)
    temp_df[field_value] = temp_df[field_value].apply(lambda x: float(x))
    temp_df = temp_df.loc[:, [field_time, field_value]]
    temp_df.columns = ['time', 'value']
    temp_df['motime'] = temp_df.index
    temp_df = temp_df[temp_df['value'] != 0]
    # 去除重复
    temp_df = temp_df.drop_duplicates(subset='motime', keep='last')
    if len(temp_df) == 0:
        return
    s_time = t_time = int(temp_df.head(1)[field_time])
    e_time = int(temp_df.tail(1)[field_time])
    p_s, i_s, m_s = utils.pim(t_time, _padding, _interval)
    p_e, i_e, m_e = utils.pim(e_time, _padding, _interval)
    t_index = np.arange(m_s, m_e+_interval, _interval)
    # t_df = pd.DataFrame({'time': 0, 'value': 0, 'motime': 0}, index=t_index)
    t_df = pd.DataFrame({'time': np.NAN, 'value': np.NAN, 'motime': np.NAN}, index=t_index)
    t_df.update(temp_df)
    # t_df = t_df.interpolate()
    t_df = t_df.ffill()
    t_df['time'] = t_df.index
    t_df['motime'] = t_df.index
    while p_s <= p_e:
        s = p_s - _padding + _interval
        e = p_s + _interval
        index_sta = np.arange(s, e, _interval)
        df_sta = pd.DataFrame({'time': 0, 'value': 0, 'motime': 0}, index=index_sta)
        # 检查该时间段是否存在旧数据
        old = get_cleaning_data(_id, s, p_s)
        if old is not None:
            old = old.drop_duplicates(subset='time', keep='last')
            df_sta.update(old)
        df_temp = t_df.loc[s:p_s]
        df_sta.update(df_temp)
        has_data = df_sta[df_sta['time'] != 0]
        if len(has_data) > 0:
            starttime = int(has_data.head(1)['time'])
            endtime = int(has_data.tail(1)['time'])
            data ={
                'id': _id,
                'timetag': p_s,
                'starttime': starttime,
                'endtime': endtime,
                'values': df_sta.to_dict('recodes')
            }
            if old is None:
                _db.insert(data)
            else:
                _db.update({'id': _id, 'timetag': {'$eq': p_s}}, data, upsert=True)
        t_time += _padding
        p_s, i_s, m_s = utils.pim(t_time, _padding, _interval)


def metric_time(v_time, interval=60):
    """ 对齐到刻度上 """
    return v_time + interval - 1 - (v_time + interval - 1) % interval


def get_cleaning_data(_id, s_time, e_time):
    d = historycl.items(s_time, e_time, Ids=[_id])
    if len(d) > 0:
        df = pd.DataFrame(d)
        df.index = df['time'].apply(lambda x: int(x))
        df['motime'] = df['time']
        df = df.drop('id', axis=1)
        return df
    return None


def get_last_cleaning_time(_id):
    """ 获取清洗数据最后的时间 """
    e = int(time.time())
    s = e - 2592000  # 30 days
    r = historycl.last(_id, s, e)
    return r.get('time', 0) if r is not None else 0


def get_first_cleaning_time(_id, s=None):
    """ 获取清洗数据最开始的时间 """
    e = int(time.time())
    if s is None:
        s = e - 259200
    r = historycl.first(_id, s, e)
    return r.get('time', 0) if r is not None else int(time.time())
