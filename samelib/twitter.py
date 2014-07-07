#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 关于tiwtter相关信息的封装
 History:
     1. 2014/7/4 16:18 : twitter.py is created.
"""



import sys
import os
from utils import flat2list

class TwitterInfo():
    """"""

    def __init__(self, ):
        """Constructor for TwitterInfo"""
        self._length =  -1
        self._data = None
        self._tid_dict = {}

    def load(self, fn, sep="\t"):
        self._data = flat2list(fn, sep)
        self._length = len(self._data)
        self._tid_dict = {}

    def save(self, fn, sep="\t"):

        with open(fn, "w") as fh:
            for l in self._data:
                print >> fh, sep.join(l)
        return

    def merge(self, info_obj):
        """
        将另一个twitter_info数据与当前对象merge， 当前数据的优先级更高。不在当前对象的数据插入。
        @param info_obj:
        @return:
        """
        if not self._tid_dict:
            self.build_tid_dict()
        data = info_obj.get_data()
        tid_dict = {}
        for l in data:
            tid = l[0]
            if tid not in self._tid_dict:
                self._data.append(l)
                tid_dict[tid] = l

        for tid, val in tid_dict.iteritems():
            self._tid_dict[tid] = val

        self._length = len(self._data)

        return

    def build_tid_dict(self):
        self._tid_dict = {}
        for l in self._data:
            tid = l[0]
            self._tid_dict[tid] = l

        return

    def get_data(self):
        return self._data

    def get_length(self):
        return self._length

    def append(self, line):
        self._data.append(line)

if __name__=='__main__':
   raise Exception("do not run it directly.")

