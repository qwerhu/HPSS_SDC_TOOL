# coding=utf-8
"""
闹钟：定时执行指定模块的main方法，要求模块中必须要实现is_time和main方法
"""
import time
import importlib
import threading
import logging
import traceback

# 默认运行间隔10分钟
DEFAULT_CHECK_INTERVAL = 600
lock = threading.Lock()
inited = set([])


def main(config):
    global lock
    global inited
    check_interval = config.get('check_interval', DEFAULT_CHECK_INTERVAL)
    module_name = config['module']
    module = importlib.import_module(module_name)
    funcs = dir(module)
    if 'init' in funcs and '_inited_' not in funcs:
        try:
            lock.acquire()
            if module_name not in inited:
                module.init(**config)
                inited.add(module_name)
        finally:
            module._inited_ = True
            lock.release()
    if 'main' not in funcs or 'is_time' not in funcs:
        time.sleep(10.0)
        return True
    while True:
        try:
            if module.is_time():
                module.main()
        except Exception as err:
            logging.error(err)
            traceback.print_exc()
        time.sleep(check_interval)

