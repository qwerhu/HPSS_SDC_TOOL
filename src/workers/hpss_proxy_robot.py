# coding=utf-8
"""
机器人：通过消息来触发执行指定模块的main方法
"""
import time
import importlib
import threading
from common.bus import Bus
lock = threading.Lock()
inited = set([])


def main(config):
    global lock
    global inited
    basestring = (str, bytes)
    module_name = config['module']
    # id = config.get('id')
    module = importlib.import_module(module_name)
    functions = dir(module)
    if 'init' in functions:
        if '_inited_' not in functions:
            try:
                lock.acquire()
                if module_name not in inited:
                    module.init(**config)
                    inited.add(module_name)
            finally:
                module._inited_ = True
                lock.release()
    msg_in = config.get('in', None)
    msg_out = config.get('out', None)
    if 'main' not in functions:
        time.sleep(10.0)
        return True
    if not msg_in and not msg_out:
        module.main()
        return False
    if msg_out and isinstance(msg_out, basestring):
        msg_out = [msg_out]
    while True:
        data = True
        while data:
            val = Bus.recv(msg_in) or None
            if val is not None:
                r = module.main(*val)
                if msg_out and r is not None:
                    for o in msg_out:
                        Bus.send(o, r)
            else:
                data = False
