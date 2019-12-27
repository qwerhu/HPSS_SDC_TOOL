# coding=utf-8
"""
与DMA相关的scada数据从zjdb转入mongodb
"""
import logging
import time
from utils import dma_util
from workers import hpss_zjdb_to_mongo
from gevent import monkey; monkey.patch_all()
import gevent

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args):
    """ 任务执行入口 """
    # 解析参数
    arg_stime = arg_etime = arg_gjz = None
    if len(args) > 0:
        arg_stime = args[0]
    if len(args) > 1:
        arg_etime = args[1]
    if len(args) > 2:
        arg_gjz = args[2]
    s_time = time.strptime(arg_stime, FORMAT_TIME) if arg_stime is not None else None
    e_time = time.strptime(arg_etime, FORMAT_TIME) if arg_etime is not None else None
    # 获取dma区域列表，逐个区域进行处理
    dma_list = dma_util.get_dma_list()
    for dma_item in dma_list:
        dma_name = dma_item.get('NAME')
        dma_gjz = dma_item.get('GJZ')
        if arg_gjz is not None and dma_gjz != arg_gjz:
            continue
        logging.info(u'开始处理DMA区域：%s' % dma_name)
        _deal(dma_item, s_time, e_time)
        logging.info(u'DMA区域%s已处理完毕' % dma_name)


def _deal(dma_info, s_time=None, e_time=None):
    # 解析出scada id列表
    formula_r = dma_info.get(dma_util.FIELD_DMA_FORMULA_FLOW)
    formula_n = dma_info.get(dma_util.FIELD_DMA_FORMULA)
    r_ids = dma_util.decode_formula_ids(formula_r)
    n_ids = dma_util.decode_formula_ids(formula_n)
    zbid_list = r_ids.union(n_ids)
    w_list = []
    for _id in zbid_list:
        w = gevent.spawn(hpss_zjdb_to_mongo.main, _id, s_time, e_time)
        w_list.append(w)
        # hpss_zjdb_to_mongo.main(_id, s_time, e_time)
    gevent.joinall(w_list)

