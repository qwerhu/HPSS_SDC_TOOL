# coding=utf-8
"""
主入口
"""
import sys
import getopt
import time
import boss
import workers
from workers import hpss_proxy_robot
from workers import hpss_proxy_clock
from multiprocessing import freeze_support
from workers import hpss_zjdb_to_mongo
import tasks


def add_robot(_id, mode, name, module, _in=None, _out=None, args={}):
    conf = {}
    conf['module'] = module
    conf['id'] = _id
    conf['in'] = _in
    conf['out'] = _out
    for k, v in args:
        conf[k] = v
    boss.spawn(mode, hpss_proxy_robot.main, name, kwArgs={'config': conf})


def add_clock(_id, mode, name, module, interval=None, args={}):
    conf = {}
    conf['module'] = module
    conf['id'] = _id
    conf['check_interval'] = interval if interval is not None else hpss_proxy_clock.DEFAULT_CHECK_INTERVAL
    for k, v in args:
        conf[k] = v
    boss.spawn(mode, hpss_proxy_clock.main, name, kwArgs={'config': conf})


def main():
    tasks.TASK_MANAGER.register_task(
        'dma_scada_load',
        'tasks.task_dma_scada_load',
        ['stime', 'etime', 'dmagjz'],
        u'加载dma相关的历史数据，默认加载全部dma区域的24小时前的数据'
    )
    tasks.TASK_MANAGER.register_task(
        'dma_scada_auto',
        'tasks.task_dma_scada_auto',
        [],
        u'自动加载dma相关的历史数据'
    )

    tasks.TASK_MANAGER.register_task(
        'dma_scada_sync',
        'tasks.task_dma_scada_sync',
        [],
        u'同步dma相关的历史数据，如果未同步过，则同步最近24小时的'
    )

    tasks.TASK_MANAGER.register_task(
        'dma_scada_add',
        'tasks.task_dma_scada_add',
        ['stime'],
        u'补充dma相关的历史数据，从开始时间一直到最后出现已有数据的时间'
    )

    tasks.TASK_MANAGER.register_task(
        'zlsdb_scada',
        'tasks.task_zlsdb_scada',
        ['stime', 'etime'],
        u'从oracle导出数据到mongodb'
    )

    tasks.TASK_MANAGER.register_task(
        'dma_data_copy',
        'tasks.task_dma_data_copy',
        [],
        u'复制前一天到dma数据到今日'
    )

    tasks.TASK_MANAGER.register_task(
        'test',
        'tasks.task_test',
        ['id', 's', 'e', 'm'],
        u'模拟区域上限或者下限报警数据【测试】'
    )

    tasks.TASK_MANAGER.register_task(
        'scada_his_cvt',
        'tasks.task_scada_his_cvt',
        ['type', 'id', 'stime', 'etime', 'coffee'],
        u'scada数据系数转换'
    )

    tasks.TASK_MANAGER.register_task(
        'scada_alarm',
        'tasks.task_scada_alarm',
        ['type', 'id', 'stime', 'etime', 'value'],
        u'scada报警数据模拟'
    )

    tasks.TASK_MANAGER.register_task(
        'scada_migrations',
        'tasks.task_scada_migrated',
        ['ip','ip1','port','database_migrated ','table_migrate','database_migrate','id','timetag','stime','etime',],
        u'scada历史数据的迁移'
    )


def _main():
    freeze_support()
    main()


def exc(argv):
    _id = stime = etime = ''
    try:
        opts, args = getopt.getopt(argv, 'hse:i')
    except getopt.GetoptError as err:
        print('main.py -i <id> -s <start time> -e <end time>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('main.py -i <id> -s <start time> -e <end time>')
            sys.exit()
        elif opt in ('-i',):
            _id = arg
        elif opt in ('-s',):
            stime = arg
        elif opt in -('-e',):
            etime = arg
    s_time = time.strftime('%Y-%m-%d %H:%M:%S', stime) if stime != '' else None
    e_time = time.strftime('%Y-%m-%d %H:%M:%S', etime) if etime != '' else None
    hpss_zjdb_to_mongo.main(_id, s_time, e_time)


if __name__ == '__main__':
    # _main()
    # exc(sys.argv[1:])
    main()
    tasks.TASK_MANAGER.run_task(sys.argv[1:])
    # hpss_zjdb_to_mongo.main('SL7_QSF.PV', e_time=time.strptime('2018-08-28 14:05:00', '%Y-%m-%d %H:%M:%S'))
