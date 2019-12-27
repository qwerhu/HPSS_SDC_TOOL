# coding=utf-8
import sys
import getopt
import logging
import importlib
import threading

lock = threading.Lock()
inited = set([])


class TaskManager(object):

    def __init__(self):
        self.task_dict = {}

    def run_task(self, argv):
        if len(argv) == 0:
            print(u'请输入-r 或者--run，后面接需要运行的任务。查询可执行的任务清单，请输入-h')
            return
        try:
            opts, args = getopt.getopt(argv, 'r:h', ['run=','help'])
        except getopt.GetoptError as err:
            print(u'请输入-r 或者--run，后面接需要运行的任务。查询可执行的任务清单，请输入-h')
            sys.exit(2)
        task_name = ''
        task_params = args or []
        for opt, arg in opts:
            if opt in ('-r', '--run'):
                task_name = arg
            elif opt in ('-h', '--help'):
                self.print_task_list()
                return
        self._run_task(task_name, task_params)

    def register_task(self, name, module, params=None, description=''):
        """ 注册任务 """
        if name in self.task_dict:
            raise u'任务名【%s】已存在' % name
        else:
            self.task_dict[name] = (module, params, description)

    def print_task_list(self):
        i = 1
        for t_name in self.task_dict:
            m, p, d = self.task_dict.get(t_name)
            print('%d. -r %s %s => %s' % (i, t_name, ' '.join(p or []), d))
            i += 1

    def _run_task(self, name, params=[]):
        """ 执行任务 """
        module, param_names, des = self.task_dict.get(name)
        if module is not None:
            logging.info(u'开始执行任务%s，输入参数：%s' % (name, ','.join(params)))
            m = importlib.import_module(module)
            f_list = dir(m)
            if 'start' in f_list:
                m.start(*params)
                logging.info(u'任务%s执行完成' % name)
            else:
                logging.warning(u'模板%s不包含可执行方法' % module)


TASK_MANAGER = TaskManager()
