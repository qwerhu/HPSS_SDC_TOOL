# coding=utf-8
"""
代码上下文定义
"""
import os
import time
import logging
from utils.config_util import Config
from db import encrypt
from pymongo.mongo_client import MongoClient
from redis import StrictRedis
from common.msgqueue import RedisQueue

# 消息类型定义
MSG_LIMIT_ALARM = 'limit-alarm'

config_path = os.path.dirname(os.path.abspath(__file__))+'\\config\\config.json'
# print 'config path is %s' % config_path
config = Config(config_path)
# print config.cfg_data
config_api_url = config.get('config', 'url')
# mongodb config
app_name = config.get('sys', 'app_name')
_mongoinfo = config.get('sys', 'mongodb')
_redisinfo = config.get('sys', 'redis')
_queue_prefix = 'RMQ' + (':' + app_name if app_name else '')
NEW_BUCKET = config.get('sys', 'nb', True)
# 解密
if _mongoinfo is not None and 'password' in _mongoinfo and _mongoinfo.get('password'):
    _mongoinfo['password'] = encrypt.decode(str(_mongoinfo.get('password')))
mongodb = MongoClient(maxPoolSize=config.get('sys', 'maxPoolSize', 128), connect=False, **_mongoinfo)
# 检查mongodb连接
while True:
    try:
        print('test connect to mongodb...')
        mongodb['test']['__test__'].count()
        print('pass')
        break
    except KeyboardInterrupt:
        print('Terminated!')
        break
    except:
        print('unable to coneect ' + str(config.get('sys', 'mongodb')))
        print('reconnecting')
    time.sleep(2000)
# 实例化消息队列
msg_queue = RedisQueue(_redisinfo, _queue_prefix)
# 实例化Redis
redis_db = StrictRedis.from_url(_redisinfo)

# Oracle db config
from db.oracle import Oracle
# zlsdb
_zlsdbinfo = config.get('sys', 'zlsdb')
zlsdb = Oracle.getDB(**_zlsdbinfo)
# zjdb
_zjdbifno = config.get('sys', 'zjdb')
zjdb = Oracle.getDB(**_zjdbifno)

hmdb = mongodb['scadadb_history']
rmdb = mongodb['scadadb_real']

LOG_FORMAT = '%(asctime)s - %(pathname)s:%(funcName)s:%(lineno)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(**{'level': config.get('sys', 'log_level', 'DEBUG'), 'format': LOG_FORMAT})


