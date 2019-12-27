# coding=utf-8
import ctx
import simplejson


class RedisCache:
    """ Redis 缓存 """
    def __init__(self, name, rts={}):
        if not name:
            raise Exception('no RTS name specified')
        self.name = 'RTS:%s:%s' % (ctx.app_name, name)
        for v in rts.values():
            self.set(**v)

    def set(self, _id, v):
        if v:
            return ctx.redis_db.hset(self.name, _id, simplejson.dumps(v))

    def get(self, _id, source=False):
        o = ctx.redis_db.hget(self.name, _id)
        if source:
            return o
        o = None if not o else simplejson.loads(o)
        return o

    def remove(self, _id):
        return ctx.redis_db.hdel(self.name, _id)

    def all(self):
        return ctx.redis_db.hgetall(self.name)

    def empty(self):
        for k in ctx.redis_db.hkeys(self.name) or []:
            ctx.redis_db.hdel(self.name, k)
