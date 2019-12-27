# coding=utf-8
import logging
import simplejson
from redis import StrictRedis


class RedisQueue:

    def __init__(self, url, queue):
        self.redis = StrictRedis.from_url(url)
        self.queue = queue
        self.pc = 0

    def name(self, typo=None):
        if typo:
            queue = self.queue + ':' + typo
        else:
            queue = self.queue
        return queue

    def pipeline(self, transaction=True, shard_hint=None):
        return self.redis.pipeline(transaction, shard_hint)

    def push(self, val, typo=None):
        return self._push(self.redis, val, typo)

    def _push(self, redis, val, typo=None):
        try:
            return redis.rpush(self.name(typo), simplejson.dumps(val))
        except Exception as err:
            logging.exception(err)

    def pop(self, typo=None):
        try:
            _, a = self.redis.blpop([self.name(typo)])
            return simplejson.loads(a or 'null')
        except Exception as err:
            logging.exception(err)

    def dqpop(self, typo=None):
        try:
            if self.pc % 2:
                _, a = self.redis.blpop([self.name(typo)])
            else:
                _, a = self.redis.brpop([self.name(typo)])
            self.pc += 1
        except Exception as err:
            logging.exception(err)

    def len(self, typo=None):
        try:
            return self.redis.llen(self.name(typo)) or 0
        except Exception as err:
            logging.exception(err)
