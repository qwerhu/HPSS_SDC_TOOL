"""
任务：每日dma数据复制（复制最新一份的数据）
"""
import ctx
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
from db import oracle
from data import historycl, history, history_limit


FORMAT_DATE = '%Y-%m-%d'
FORMAT_TIME = '%Y-%m-%d %H:%M:%S'
# 24小时的秒数
ONE_DAY = 86400


def start(*args, **kwargs):
    """
    任务执行入口
    :param args:
    :return:
    """

    # 解析参数
    # date_from = kwargs.get('_from')
    # date_to = kwargs.get('to')
    # just_scada = kwargs.get('just_scada')
    while True:
        # 只有0点才执行，将前一天点数据同步到今日
        if time.localtime()[3:5] == (0, 0):
            date_from = time.strftime(FORMAT_DATE, time.localtime(time.time() - 86400))
            date_to = time.strftime(FORMAT_DATE, time.localtime())
            deal(date_from, date_to)
            time.sleep(61)
        else:
            time.sleep(10)


def deal(date_from=None, date_to=None, just_scada=False):
    print('from {} to {}'.format(date_from, date_to))
    # 默认值
    if date_from is None:
        date_from = time.strftime(FORMAT_DATE, time.localtime(time.time() - ONE_DAY))
    if date_to is None:
        date_to = time.strftime(FORMAT_DATE, time.localtime())
    if not just_scada:
        # 夜间流量数据复制
        print('夜间流量数据复制')
        copy_dma_nmf(date_from, date_to)
        # 产销从数据复制
        print('产销从数据复制')
        copy_dma_nrw(date_from, date_to)
        # 水平衡表数据复制
        print('水平衡表数据复制')
        copy_waterbanlance(date_from, date_to)
        # scada指标每日统计数据复制
        print('scada指标每日统计数据复制')
        copy_dma_monitor(date_from, date_to)
        # return
    # scada历史数据复制
    print('scada历史数据复制')
    t = time.time()
    copy_scada_his(date_from, date_to)
    print('cost time: {}'.format(time.time() - t))
    # scada清洗数据复制
    print('scada清洗数据复制')
    t = time.time()
    copy_scada_cleaning(date_from, date_to)
    print('cost time: {}'.format(time.time() - t))
    # scada上下限数据复制
    print('scada上下限数据复制')
    t = time.time()
    copy_scada_limit(date_from, date_to)
    print('cost time: {}'.format(time.time() - t))
    print('---------- all over ----------')


def copy_dma_nmf(date_from, date_to):
    q = "select * from dma_nmf where to_char(nmf_date, 'yyyy-mm-dd') = '{}'".format(date_from)
    data_from = oracle.query(ctx.zlsdb, q)
    if data_from is not None:
        sql_list = []
        for item in data_from:
            # 修改下日期
            item['NMF_DATE'] = '{} 04:00:00'.format(date_to)
            sql = "insert into dma_nmf (gjz, nmf_date, nmf_value, nmf_trend, inp_date, avg_value, pure_nmf_value) " \
                  "values({GJZ}, to_date('{NMF_DATE}', 'yyyy-mm-dd hh24:mi:ss'), {NMF_VALUE}, {NMF_TREND}, sysdate, " \
                  "{AVG_VALUE}, {PURE_NMF_VALUE})".format(**item)
            oracle.execute(ctx.zlsdb, sql)
        #     sql_list.append(sql)
        # if len(sql_list) > 0:
        #     oracle.execute_many(ctx.zlsdb, sql_list)


def copy_dma_nrw(date_from, date_to):
    q = "select * from dma_nrw where nrw_type = 0 and to_char(cedate, 'yyyy-mm-dd') = '{}'".format(date_from)
    data = oracle.query(ctx.zlsdb, q)
    e = datetime.strptime(date_to, FORMAT_DATE)
    s = e - relativedelta(days=1)
    s = s.strftime(FORMAT_DATE)
    e = e.strftime(FORMAT_DATE)
    if data is not None:
        sql_list = []
        for item in data:
            # 修改下日期
            item['CBDATE'] = s
            item['CEDATE'] = e
            sql = "insert into dma_nrw(id, gjz, cbdate, cedate, nrw_value, nrw_input_value, nrw_sales_value, " \
                  "nrw_trend, inp_date, nrw_type, nfvalue, coefficient) values(sys_guid(), {GJZ}, " \
                  "to_date('{CBDATE}', 'yyyy-mm-dd'), to_date('{CEDATE}','yyyy-mm-dd'), {NRW_VALUE}, " \
                  "{NRW_INPUT_VALUE}, {NRW_SALES_VALUE}, {NRW_TREND}, sysdate, {NRW_TYPE}, {NFVALUE}, " \
                  "{COEFFICIENT})".format(**item)
            oracle.execute(ctx.zlsdb, sql)
        #     sql_list.append(sql)
        # if len(sql_list) > 0:
        #     oracle.execute_many(ctx.zlsdb, sql_list)


def copy_waterbanlance(date_from, date_to):
    q = "select * from dma_waterbalance where to_char(cedate, 'yyyy-mm-dd') = '{}'".format(date_from)
    data = oracle.query(ctx.zlsdb, q)
    e = datetime.strptime(date_to, FORMAT_DATE)
    s = e - relativedelta(months=1)
    s = s.strftime(FORMAT_DATE)
    e = e.strftime(FORMAT_DATE)
    if data is not None:
        sql_list = []
        for item in data:
            # 修改下日期
            item['CBDATE'] = s
            item['CEDATE'] = e
            sql = "insert into dma_waterbalance(id, gjz, cbdate, cedate, suppliedvolume, billedvolume) " \
                  "vlues(sys_guid(), {GJZ}, to_date('{CBDATE}', 'yyyy-mm-dd'), to_date('{CEDATE}', 'yyyy-mm-dd')), " \
                  "{SUPPLIEDVOLUME}, {BILLEDVOLUME})".format(**item)
            oracle.execute(ctx.zlsdb, sql)
        #     sql_list.append(sql)
        # if len(sql_list) > 0:
        #     oracle.execute_many(ctx.zlsdb, sql_list)


def copy_dma_monitor(date_from: str, date_to: str):
    q = "select * from dma_monitor where to_char(endtime, 'yyyy-mm-dd') = '{}'".format(date_from)
    data = oracle.query(ctx.zlsdb, q)
    e = datetime.strptime(date_to, FORMAT_DATE)
    s = e - relativedelta(days=1)
    s = s.strftime(FORMAT_DATE)
    e = e.strftime(FORMAT_DATE)
    if data is not None:
        sql_list = []
        for item in data:
            # 修改下日期
            item['STARTTIME'] = s
            item['ENDTIME'] = e
            sql = "insert into dma_monitor(itemname, starttime, endtime, datavalue) values('{ITEMNAME}', " \
                  "to_date('{STARTTIME}','yyyy-mm-dd'), to_date('{ENDTIME}', 'yyyy-mm-dd'), {DATAVALUE})" \
                  "".format(**item)
            oracle.execute(ctx.zlsdb, sql)
        #     sql_list.append(sql)
        # if len(sql_list) > 0:
        #     oracle.execute_many(ctx.zlsdb, sql_list)


def _query_his_data(_db, s, e):
    ts = s + (7200 - s % 7200)
    te = e + (7200 - e % 7200)
    q = {'timetag': {'$gte': ts, '$lte': te}}
    return _db.find(q).sort([('timetag', 1)])


def copy_scada_his(date_from: str, date_to: str):
    fs = datetime.strptime(date_from, FORMAT_DATE).timestamp()
    ts = datetime.strptime(date_to, FORMAT_DATE).timestamp()
    diff = ts - fs
    fe = fs + 86400 - 1

    _db = ctx.hmdb['t_scada_his']
    data = _query_his_data(_db, fs+1, fe)
    for item in data:
        obj = {}
        obj['id'] = item['id']
        obj['timetag'] = int(float(item['timetag']) + diff)
        obj['starttime'] = int(float(item['starttime']) + diff)
        obj['endtime'] = int(float(item['endtime']) + diff)
        vlist = []
        for x in item['values']:
            v = x['value']
            if isinstance(v, dict):
                v = next(iter(v.items()))[1]
            vlist.append({
                'time': int(x['time'] + diff),
                'motime': int(x['motime'] + diff),
                'value': v
            })
        obj['values'] = vlist
        # _db.update({}, {'$set': obj})
        _db.insert_one(obj)
        # print('[history] insert over time tag: {}'.format(item['timetag']))
    # data = history.items(fs, fe)
    # data = [('m', x['id'], x['value'], x['time'] + diff) for x in data]
    # history.upserts(data)


def copy_scada_cleaning(date_from: str, date_to: str):
    fs = datetime.strptime(date_from, FORMAT_DATE).timestamp()
    ts = datetime.strptime(date_to, FORMAT_DATE).timestamp()
    diff = ts - fs
    fe = fs + 86400 - 1

    _db = ctx.hmdb['t_scada_cleaning']
    data = _query_his_data(_db, fs, fe)
    for item in data:
        item['timetag'] += diff
        item['starttime'] += diff
        item['endtime'] += diff
        for x in item['values']:
            v = x['value']
            if isinstance(v, dict):
                v = next(iter(v.items()))[1]
            x['value'] = v
            x['motime'] += diff
            x['time'] += diff
        item.pop('_id', None)
        _db.insert_one(item)


def copy_scada_limit(date_from: str, date_to: str):
    fs = datetime.strptime(date_from, FORMAT_DATE).timestamp()
    ts = datetime.strptime(date_to, FORMAT_DATE).timestamp()
    diff = ts - fs
    fe = fs + 86400 - 1

    _db = ctx.hmdb['t_scada_limit']
    data = _query_his_data(_db, fs, fe)
    for item in data:
        item['timetag'] += diff
        item['starttime'] += diff
        item['endtime'] += diff
        for x in item['values']:
            v = x['value']
            if isinstance(v, dict):
                v = next(iter(v.items()))[1]
            x['value'] = v
            x['min'] = next(iter(x['min'].items()))[1] if isinstance(x['min'], dict) else x['min']
            x['max'] = next(iter(x['max'].items()))[1] if isinstance(x['max'], dict) else x['max']
            x['motime'] += diff
            x['time'] += diff
        item.pop('_id', None)
        _db.insert_one(item)
