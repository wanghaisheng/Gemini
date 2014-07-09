#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 解析和预处理yaml配置文件
 History:
     1. 2014/7/4 15:30 :  is created.
"""



import sys
import os
from argparse import ArgumentParser
import yaml


class Config():
    """"""

    def __init__(self, yaml_file):
        """Constructor for Config"""
        self._obj = yaml.load(open(yaml_file))
        self._valid()

    def __getitem__(self, item):
        return self._obj[item]

    def _valid(self):
        path = self['WORK_BASE_PATH'] + '/log'
        if not os.path.exists(path):
            os.makedirs(path)
        path = self['WORK_BASE_PATH'] + '/data'
        if not os.path.exists(path):
            os.makedirs(path)


if __name__=='__main__':
    raise Exception("do not run it directly.")

