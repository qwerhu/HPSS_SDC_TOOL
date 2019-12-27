# coding=utf-8
""" dma相关工具函数 """

import re
import ctx
from db import oracle
from config import config_sensor

# 从公式中获取指标id的正则表达式
ID_PATTERN = re.compile(r'\'(.*?)\'')
# tz_dmainfo中的公式字段
FIELD_DMA_FORMULA = 'CHECK_FORMULA_NRW'
FIELD_DMA_FORMULA_FLOW = 'CHECK_FORMULA'


def decode_formula_ids(formula):
    """ 解析公式中的scada id，并返回id数组（set） """
    s = set([])
    # 去掉空格，双引号替换为单引号
    if formula is not None:
        fc = formula.replace(' ', '').replace('"', "'")
        l = ID_PATTERN.findall(fc)
        for i in l:
            s.add(str(i).strip().strip('\''))
    return s


def get_dma_list():
    """ 获取dma区域信息 """
    sql = 'select check_formula_nrw, gjz, name, dmalevel from tz_dmainfo where txyx=0 and %s is not null' % FIELD_DMA_FORMULA
    return oracle.query(ctx.zlsdb, sql)


def get_tagnames():
    sql = 'select tagname from V_MODEL_SCADA'
    return oracle.query(ctx.zlsdb, sql)

def zbid2sid(zbid):
    citem = config_sensor.get_by_zbid(zbid)
    if citem is not None:
        return citem.get('sid')
    return None


def sid2zbid(sid):
    citem = config_sensor.get_by_sid(sid)
    if citem is not None:
        return citem.get('zbid')
    return None


def zbid2sid_list(zbid_list):
    return filter(lambda d: d is not None, [zbid2sid(x) for x in zbid_list])


def sid2zbid_list(sid_list):
    return filter(lambda d: d is not None, [sid2zbid(x) for x in sid_list])

