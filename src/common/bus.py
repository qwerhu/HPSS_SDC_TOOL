# coding=utf-8
import ctx


class RedisBus:

    def __init__(self, *args, **kwargs):
        self.queue_name = ctx.msg_queue.queue
        self.queue = ctx.msg_queue

    def send(self, typo, val):
        self.queue.push(val, typo)

    def sends(self, typo, *vals):
        pipline = self.queue.pipeline()
        for val in vals:
            self.queue._push(pipline, val, typo)
        pipline.execute()

    def recv(self, typo):
        x = self.queue.pop(typo)
        if x:
            ctx.redis_db.incr('%s:count:%s' % (self.queue_name, typo))
        return x

    def llen(self, typo):
        return self.queue.len(typo)

    def dqrecv(self, typo):
        x = self.queue.dqpop(typo)
        if x:
            ctx.redis_db.incr('%s:count:%s' % (self.queue_name, typo))
        return x

    def len(self, typo):
        return self.queue.len(typo), ctx.redis_db.get('%s:count:%s' % (self.queue_name, typo)) or 0


Bus = RedisBus()
