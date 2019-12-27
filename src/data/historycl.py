# coding=utf-8
import ctx
from common.timeseries import TimeSeries

COLLECTION_NAME = 't_scada_cleaning'
MAX_BUCKET = 120
INTERVAL = 60
ASC = 1
DESC = -1

HistoryCl = TimeSeries(ctx.hmdb[COLLECTION_NAME], MAX_BUCKET, INTERVAL, alignment=False)


def group(pipeline):
    return HistoryCl.aggregate(pipeline)


def delete(_id, start, end, loose=False):
    return HistoryCl.delete(_id, start, end, loose=loose)


def upsert(m, _id, v, t, w=1):
    return HistoryCl.upsert(m, _id, v, t, w=w)


def upserts(rcds, w=1):
    return HistoryCl.upserts(rcds, w)


def two(_id, start, end):
    return HistoryCl.two(_id, start, end)


def one(_id, start, end, sort=DESC):
    return HistoryCl.one(_id, start, end, sort=sort)


first = lambda _id, start, end: one(_id, start, end, ASC)
last = lambda _id, start, end: one(_id, start, end, DESC)

def items(s=None, e=None, n=None, Ids=None, sort=[('id', 1), ('time', 1)], loose=False, ext=None):
    return HistoryCl.items(s=s, e=e, n=n, Ids=Ids, sort=sort, loose=loose, ext=ext)


def clear(id):
    return HistoryCl.clear(id)


def migration(old, new, s=None, e=None):
    return HistoryCl.migration(old, new, s, e)
