# coding=utf-8
import ctx
import utils
import math, contextlib
ASC = 1
DESC = -1


class LimitSeries:
    def __init__(self, db, bucket, interval, alignment=True):
        self.db = db
        self.bucket = bucket
        self.interval = interval
        self.padding = int(bucket * self.interval)
        self.limits = int(self.padding / self.interval)
        self.alignment = alignment
        self._db = db

    def __db(self):
        return self._db

    def delete(self, _id, start, end, loose=False):
        if ctx.NEW_BUCKET:
            query = {'id': _id, 'timetag': {'$gte': utils.pim(start, self.padding, self.interval)[0],
                       '$lte': utils.pim(end, self.padding, self.interval)[0]}}
        else:
            if loose:
                query = {'id': _id, 'endtime': {'$gte': start, '$lte': end}, 'starttime': {'$lte': end, '$gte': start}}
            else:
                query = {'id': _id, 'endtime': {'$gte': start}, 'starttime': {'$lte': end}}
        for o in self.__db().find(query):
            o['starttime'] = 0
            o['endtime'] = 0
            for k in o['values']:
                if start <= k['time'] <= end:
                    k['time'] = k['motime'] = k['value'] = 0
                else:
                    o['starttime'] = min(k['time'], o['starttime'])
                    o['endtime'] = max(k['time'], o['endtime'])

            if o['starttime'] == 0:
                o['starttime'] = o['endtime']
            if o['endtime'] == 0:
                o['endtime'] = o['starttime']
            if o['starttime'] == 0:
                self.__db().remove(o['_id'])
            else:
                self.__db().save(o)

    def upsert(self, m, _id, v, t, _min, _max, w=1, s=None):
        if m is None or _id is None or v is None or t is None:
            return
        padding, index, metric = utils.pim(t, self.padding, self.interval)
        # o = self.__db().find_one({'id': _id, 'timetag': {'$eq': padding}}, projection={'_id': 1, 'timetag': 1})
        o = self.__db().find_one({'id': _id, 'timetag': {'$eq': padding}})
        tpl = {'time': 0, 'motime': 0, 'value': 0, 'min': 0, 'max': 0}
        node = {'time': metric, 'motime': t, 'value': v, 'min': _min, 'max': _max}
        if s is not None:
            tpl['starttime'], node['starttime'] = s, s
        if not o:
            o = self.__db().update({'id': _id, 'timetag': {'$eq': padding}}, {'$setOnInsert': {'id': _id,
                                'timetag': padding,
                                'starttime': t,
                                'endtime': t,
                                'values': [ tpl for _ in range(self.limits) ]}}, upsert=True, w=w)
        # else:
            # old = o.get('values')[index]
            # if old is not None and old.get('time', 0) != 0:
            #     # 如果某时间点的数据已存在，则不再更新
            #     return old
        query = {'id': _id, 'timetag': {'$eq': padding}}
        if s != 1:
            query['values.%d.motime' % index] = {'$lt': t - t % 60 + 60}
        self.__db().update(query, {'$max': {'endtime': metric}, '$min': {'starttime': metric}, '$set': {'values.%d' % index: node}}, w=w)
        return node

    def one(self, _id, start, end, sort=DESC):
        if ctx.NEW_BUCKET:
            tt = 'time' if self.alignment else 'motime'
            for o in self.__db().find({'id': _id, 'timetag': {'$gte': utils.pim(start, self.padding, self.interval)[0],
                       '$lte': utils.pim(end, self.padding, self.interval)[0]}}).sort([
             (
              'timetag', sort)]):
                if sort == DESC:
                    o['values'].reverse()
                for oo in o['values']:
                    if start <= oo[tt] <= end:
                        return {'id': _id, 'time': oo[tt], 'value': oo['value'], 'min': oo['min'], 'max': oo['max']}

            return
        t = 2147483647 if sort > 0 else -1
        v = float('nan')
        _min = _max = float('nan')
        with contextlib.closing(self.__db().find({'id': _id, 'endtime': {'$gte': start}, 'starttime': {'$lte': end}}).hint([('id', 1), ('endtime', -1), ('starttime', 1), ('_id', 1)]).sort([('_id', 1)])) as (cur):
            for o in cur:
                for oo in o['values']:
                    if oo['time'] >= start and oo['time'] <= end:
                        if sort > 0 and t >= oo['time']:
                            t = oo['time']
                            v = oo['value']
                            _min = oo['min']
                            _max = oo['max']
                        elif sort < 0 and t <= oo['time']:
                            t = oo['time']
                            v = oo['value']
                            _min = oo['min']
                            _max = oo['max']

        if math.isnan(v):
            return
        return {'id': _id, 'time': t, 'value': v, 'min': _min, 'max': _max}
        return

    def two(self, _id, start, end):
        startO = self.first(_id, start, end)
        endO = self.last(_id, start, end)
        if startO and endO and startO.get('time') == endO.get('time'):
            if utils.utils.getYearMonthByTimestamp(start)[1] == utils.utils.getYearMonthByTimestamp(startO['time'])[1]:
                endO = None
            elif utils.utils.getYearMonthByTimestamp(end)[1] == utils.utils.getYearMonthByTimestamp(endO['time'])[1]:
                startO = None
            else:
                startO, endO = (None, None)
        return (startO, endO)

    def first(self, _id, start, end):
        return self.one(_id, start, end, ASC)

    def last(self, _id, start, end):
        return self.one(_id, start, end, DESC)

    def items(self, s=None, e=None, n=None, Ids=None, sort=[('id', 1), ('time', 1)], loose=False, ext=None):
        """
          @param s: 开始时间戳
          @param e: 结束时间戳
          @param Ids: 传感器位号列表
          @param ext: [{'id':'',starttime:'',endtime:''},]
        """
        tt = 'time' if self.alignment else 'motime'
        if not ctx.NEW_BUCKET:
            tt = 'time'
        if ctx.NEW_BUCKET:
            q = {}
            if not ext:
                if len(Ids) > 0:
                    q['id'] = {'$in': Ids}
                if s > 0 or e > 0:
                    q['timetag'] = {}
                if s > 0:
                    q['timetag']['$gte'] = utils.pim(s, self.padding, self.interval)[0]
                if e > 0:
                    q['timetag']['$lte'] = utils.pim(e, self.padding, self.interval)[0]
            else:
                _or = []
                for kv in ext:
                    r = {}
                    if kv.get('starttime') > 0 or kv.get('endtime') > 0:
                        r = {'id': kv.get('id'), 'timetag': {}}
                    if kv.get('starttime') > 0:
                        r['timetag']['$gte'] = utils.pim(kv.get('starttime'), self.padding, self.interval)[0]
                    if kv.get('endtime') > 0:
                        r['timetag']['$lte'] = utils.pim(kv.get('endtime'), self.padding, self.interval)[0]
                    if r:
                        _or.append(r)

                if _or:
                    q['$or'] = _or
                if not q:
                    return []
            p = []
            fext = {}
            if ext:
                for kv in ext:
                    if 'id' in kv:
                        if kv['id'] not in fext:
                            fext[kv['id']] = []
                        fext[kv['id']].append(kv)

            for o in self.__db().find(q).sort([('timetag', 1)]):
                _id = o['id']
                for oo in o['values']:
                    if ext:
                        for kv in fext.get(_id) or []:
                            if oo[tt] >= kv.get('starttime') and oo[tt] <= kv.get('endtime'):
                                p.append({'id': _id, 'time': oo[tt], 'value': oo['value'],
                                          'min': oo['min'], 'max': oo['max']})

                    elif oo[tt] >= s and oo[tt] <= e:
                        p.append({'id': _id, 'time': oo[tt], 'value': oo['value'],
                                  'min': oo['min'], 'max': oo['max']})

            sort = dict(sort)
            p.sort(key=lambda x: (sort['id'] * x['id'], sort['time'] * x['time']))
            if 'value' in sort:
                p.sort(key=lambda x: sort['value'] * x['value'])
        else:
            q = {}
            if s is not None or e is not None:
                if s is not None:
                    q['endtime'] = {}
                    q['endtime']['$gte'] = s
                    if loose:
                        q['endtime']['$lte'] = e
                if e is not None:
                    q['starttime'] = {}
                    q['starttime']['$lte'] = e
                    if loose:
                        q['starttime']['$gte'] = s
            if len(Ids) > 0:
                q['id'] = {'$in': Ids}
            if not q:
                return []
            p = []
            items = self.__db().find(q).hint([('id', 1), ('endtime', -1), ('starttime', 1), ('_id', 1)]).sort([('_id', 1)])
            for o in items:
                _id = o['id']
                for oo in o['values']:
                    if oo[tt] >= s and oo[tt] <= e:
                        p.append({'id': _id, 'time': oo[tt], 'value': oo['value'],
                                  'min': oo['min'], 'max': oo['max']})

            p.sort(key=lambda x: (x['id'], x['time']))
            _p = []
            _lastP = None
            for __p in p:
                if _lastP is not None:
                    if _lastP['id'] != __p['id']:
                        _p.append(_lastP)
                    elif _lastP['time'] != __p['time']:
                        _p.append(_lastP)
                _lastP = __p

            if _lastP:
                _p.append(_lastP)
            p = _p
            sort = dict(sort)
            p.sort(key=lambda x: (sort['id'] * x['id'], sort['time'] * x['time']))
        if n:
            return p[0:n]
        return p
        return

    def clear(self, id):
        self.__db().remove({'id': id}, multi=True)

    def aggregate(self, pipeline, **kwargs):
        return self.__db().aggregate(pipeline, **kwargs)

    def pim(self, t):
        return utils.pim(t, self.padding, self.interval)

    def pad(self, t):
        return self.pim(t)[0]

    def migration(self, old, new, s=None, e=None):
        return self.__db().update({'id': old}, {'$set': {'id': new}}, multi=True)
