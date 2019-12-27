# coding=utf-8
import time
import logging
from utils import dma_util
from utils import data_util
from data import historycl
from workers import hpss_zjdb_to_mongo

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args):
    """ 任务执行入口 """
    stime = args[0]
    etime = args[1]
    s_time = time.strptime(stime, FORMAT_TIME) if stime is not None else None
    e_time = time.strptime(etime, FORMAT_TIME) if etime is not None else None
    for item in dma_util.get_tagnames():
        _deal(item, s_time, e_time)


def _deal(info, s_time, e_time):
    _id = info['TAGNAME']
    hpss_zjdb_to_mongo.main(_id, s_time, e_time)
