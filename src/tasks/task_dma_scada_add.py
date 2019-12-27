# coding=utf-8
"""
与DMA相关的scada数据从zjdb转入mongodb
"""
import logging
import time
from utils import dma_util
from utils import data_util
from workers import hpss_zjdb_to_mongo

FORMAT_TIME = '%Y%m%d%H%M%S'


def start(*args):
    """ 任务执行入口 """
    # 解析参数
    arg_stime = arg_etime = arg_gjz = None
    if len(args) > 0:
        arg_stime = args[0]
    if len(args) > 2:
        arg_gjz = args[2]
    s_time = time.strptime(arg_stime, FORMAT_TIME) if arg_stime is not None else None

    # 获取dma区域列表，逐个区域进行处理
    while True:
        dma_list = dma_util.get_dma_list()
        for dma_item in dma_list:
            dma_name = dma_item.get('NAME')
            dma_gjz = dma_item.get('GJZ')
            if arg_gjz is not None and dma_gjz != arg_gjz:
                continue
            logging.info(u'开始处理DMA区域：%s' % dma_name)
            _deal(dma_item, s_time)
            logging.info(u'DMA区域%s已处理完毕' % dma_name)
        time.sleep(3600)


def _deal(dma_info, s_time=None):
    # 解析出scada id列表
    formula_r = dma_info.get(dma_util.FIELD_DMA_FORMULA_FLOW)
    formula_n = dma_info.get(dma_util.FIELD_DMA_FORMULA)
    r_ids = dma_util.decode_formula_ids(formula_r)
    n_ids = dma_util.decode_formula_ids(formula_n)
    zbid_list = r_ids.union(n_ids)
    print('ids: ' + str(zbid_list))
    for _id in zbid_list:
        sid = dma_util.zbid2sid(_id)
        e_time = time.localtime(data_util.get_first_cleaning_time(sid, int(time.mktime(s_time))))
        print('id: '+_id+' 时间段：'+time.strftime('%Y-%m-%d %H:%M:%S', s_time) + ' - ' + time.strftime('%Y-%m-%d %H:%M:%S', e_time))
        hpss_zjdb_to_mongo.main(_id, s_time, e_time)

