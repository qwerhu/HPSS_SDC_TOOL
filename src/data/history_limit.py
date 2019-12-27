# coding=utf-8
import ctx
from common.limitseries import LimitSeries

COLLECTION_NAME = 't_scada_limit'
MAX_BUCKET = 120
INTERVAL = 60
ASC = 1
DESC = -1

HistoryLimit = LimitSeries(ctx.hmdb[COLLECTION_NAME], MAX_BUCKET, INTERVAL, alignment=False)


def group(pipeline):
    return HistoryLimit.aggregate(pipeline)


def delete(_id, start, end, loose=False):
    return HistoryLimit.delete(_id, start, end, loose=loose)


def upsert(m, _id, v, t, _min, _max, w=1):
    return HistoryLimit.upsert(m, _id, v, t, _min, _max, w=w)


def upserts(rcds, w=1):
    return HistoryLimit.upserts(rcds, w)


def two(_id, start, end):
    return HistoryLimit.two(_id, start, end)


def one(_id, start, end, sort=DESC):
    return HistoryLimit.one(_id, start, end, sort=sort)


first = lambda _id, start, end: one(_id, start, end, ASC)
last = lambda _id, start, end: one(_id, start, end, DESC)


def items(s=None, e=None, n=None, Ids=None, sort=[('id', 1), ('time', 1)], loose=False, ext=None):
    return HistoryLimit.items(s=s, e=e, n=n, Ids=Ids, sort=sort, loose=loose, ext=ext)


def clear(id):
    return HistoryLimit.clear(id)


def migration(old, new, s=None, e=None):
    return HistoryLimit.migration(old, new, s, e)
