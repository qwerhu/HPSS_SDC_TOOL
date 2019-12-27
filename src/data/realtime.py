import re
import ctx

__db = ctx.rmdb['t_scada_real']


def _db():
    """@rtype pymongo.Collection"""
    return __db


CURRENT = 'current'
CURRENT_ALL = 'current:all'


def upserts(ss, w=1):
    db = _db().initialize_ordered_bulk_op()
    for _, _id, v, t in ss:
        _upsert(db, _id, v, t)

    db.execute({'w': w})


def upsert(_id, v, t, w=1):
    _db().update({'_id': _id}, {'_id': _id, 'value': v, 'time': t}, upsert=True, w=w)


def _upsert(bulk, _id, v, t):
    bulk.find({'_id': _id}).upsert().update({'$set': {'_id': _id, 'value': v, 'time': t}})


def items(Ids=None, Fields=None, IdPrefix=None, NoId=False):
    q = {}
    if Ids and len(Ids) > 0:
        q['_id'] = {'$in': Ids}
    if IdPrefix is not None:
        q['_id'] = {'$regex': re.compile('^' + re.escape(IdPrefix))}
    r = {} if NoId else {'_id': CURRENT}
    for k in _db().find(q, projection=Fields).batch_size(100000):
        r[k['_id']] = k

    return r


def get(_id):
    return _db().find_one({'_id': _id})


def remove(_id):
    return _db().remove(_id)


def upsert_img(_id, v, t, w=1):
    _db().update({'_id': _id}, {'$set': {'_id': _id, 'b': v, 'bt': t, 'rd': None}}, upsert=True, w=w)
    return


# def read_img(_id, t, read, w=1):
#     _db().update({'_id': _id}, {'$set': {'rd': read, 'state': 0}}, upsert=False, w=w)
#     import picture
#     picture.read(_id, t, read, w)
#
#
# def check_img(_id, t, state, gd, w=1):
#     _db().update({'_id': _id}, {'$set': {'gd': gd, 'state': state}}, upsert=False, w=w)
#     import picture
#     picture.check(_id, t, state, gd, w)