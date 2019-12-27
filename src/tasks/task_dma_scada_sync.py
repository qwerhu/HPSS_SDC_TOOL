# coding=utf-8
import time
import logging
from utils import dma_util
from utils import data_util
from data import historycl
from workers import hpss_zjdb_to_mongo
from gevent import monkey; monkey.patch_all()
import gevent

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args):
    """ 任务执行入口 """
    # 获取dma列表，逐个处理
    for dma_item in dma_util.get_dma_list():
        dma_name = dma_item.get('NAME')
        logging.info(u'开始同步DMA区域：%s' % dma_name)
        _deal(dma_item)
        logging.info(u'DMA区域%s同步结束' % dma_name)


def _deal(dma_info):
    # 确定开始和结束时间
    # formula_r = dma_info.get(dma_util.FIELD_DMA_FORMULA_FLOW)
    formula_n = dma_info.get(dma_util.FIELD_DMA_FORMULA)
    # r_ids = dma_util.decode_formula_ids(formula_r)
    n_ids = dma_util.decode_formula_ids(formula_n)
    # zbid_list = r_ids.union(n_ids)
    w_list = []
    for _id in n_ids:
        e = int(time.time())
        s = data_util.get_last_cleaning_time(_id)
        if s == 0:
            s = e - 86400  # 24 hours
        s_time = time.localtime(s)
        e_time = time.localtime(e)
        w = gevent.spawn(hpss_zjdb_to_mongo.main, _id, s_time, e_time)
        w_list.append(w)
    gevent.joinall(w_list)
