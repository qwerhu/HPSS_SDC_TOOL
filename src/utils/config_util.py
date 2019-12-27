# coding=utf-8
import os

DEFAULT_PATH = '../config/config.json'


class Config(object):
    def __init__(self, filepath=None):
        self.cfg_data = {}
        f = filepath or DEFAULT_PATH
        if os.path.exists(f):
            with open(f, 'r') as (fd):
                self.cfg_data = eval(fd.read(), {'__builtins__': None}) or {}

    def get(self, ns, key, default_value=None):
        r = self.cfg_data.get(ns, None)
        return r.get(key, default_value) if r is not None else default_value


