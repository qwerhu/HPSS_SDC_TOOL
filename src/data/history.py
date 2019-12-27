import ctx
from common.timeseries import TimeSeries
MAX_BUCKET = 120
INTERVAL = 60
COLLECTION_RAW = 't_scada_his'
ASC = 1
DESC = -1
History = TimeSeries(ctx.hmdb[COLLECTION_RAW], MAX_BUCKET, INTERVAL, alignment=False)


def group(pipeline):
    return History.aggregate(pipeline)


def delete(_id, start, end, loose=False):
    return History.delete(_id, start, end, loose=loose)


def upsert(m, _id, v, t, w=1):
    return History.upsert(m, _id, v, t, w=w)


def upserts(rcds, w=1):
    return History.upserts(rcds, w)


def two(_id, start, end):
    return History.two(_id, start, end)


def one(_id, start, end, sort=DESC):
    return History.one(_id, start, end, sort=sort)


first = lambda _id, start, end: one(_id, start, end, ASC)
last = lambda _id, start, end: one(_id, start, end, DESC)


def items(s=None, e=None, n=None, Ids=None, sort=[('id', 1), ('time', 1)], loose=False, ext=None):
    return History.items(s=s, e=e, n=n, Ids=Ids, sort=sort, loose=loose, ext=ext)


def clear(id):
    return History.clear(id)


def migration(old, new, s=None, e=None):
    return History.migration(old, new, s, e)
