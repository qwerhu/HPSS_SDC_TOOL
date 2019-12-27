# coding=utf-8
import ctx
import time
import logging
import traceback
import pandas as pd
from db import oracle
from utils import dma_util
from utils import data_util


def main(zbid, s_time=None, e_time=None):
    if e_time is None:
        e_time = time.localtime()
    if s_time is None:
        s_time = time.localtime(time.mktime(e_time) - 86400)  # 24 hours
    try:
        his_raw = _get_zjdb_data(zbid, s_time, e_time)
        if his_raw is not None and len(his_raw) > 0:
            his_raw['time'] = his_raw['DATETIME'].apply(lambda x: int(time.mktime(x.timetuple())))
            # sid = zbid  # dma_util.zbid2sid(zbid)
            sid = dma_util.zbid2sid(zbid)
            data_util.save_history_cleaning(sid, his_raw, 'time', 'DATAVALUE')
    except Exception as err:
        logging.error('zjdb to mongodb error: %s' % str(err))
        traceback.print_exc()


""" 以下为私有方法 """
FORMAT_TIME = '%Y-%m-%d %H:%M:%S'
FORMAT_ORA_DATE = 'yyyy-mm-dd hh24:mi:ss'


def _get_zjdb_data(s_id, s_time, e_time):
    """ 获取scada数据 """
    s_str = time.strftime(FORMAT_TIME, s_time)
    e_str = time.strftime(FORMAT_TIME, e_time)
    sql = "select itemname, datetime, datavalue from t_scadadata_his_qx where itemname='%s' " \
          "and datetime >= to_date('%s','%s') and datetime <= to_date('%s','%s') " \
          "" % (s_id, s_str, FORMAT_ORA_DATE, e_str, FORMAT_ORA_DATE)
    # print(sql)
    res = oracle.query(ctx.zjdb, sql)
    if len(res) > 0:
        return pd.DataFrame(res)
    return None
