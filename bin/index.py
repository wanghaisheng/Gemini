#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 管理同款服务的索引文件
 History:
     1. 2014/6/30 23:08 : index.py is created.
"""



import sys
import os

import numpy as np
from pyflann import FLANN


class Index():

    def __init__(self):
        """
        _para is from flann.build_index()
        """
        self._data = {}
        self._para = TODO

    def clear(self):
        self._data = {}

    def load(self, categorys, dir):
        """
        @param categorys: list, ['clothes', 'shoes', 'bag']
        @param dir:       path to index files
        """
        for c in categorys:
            data = np.load("%s_data.npy" % c)
            flann = FLANN()
            flann.load_index("%s_index.flann" % c, data)
            tid2img = self._load_tid("%s_tid_with_img.txt" % c)

            self._data[c]['data'] = data
            self._data[c]['flann'] = flann
            self._data[c]['tid2img'] = tid2img

        return True

    def _load_tid(self, fn):
        """
        2881539803 http://imgst.meilishuo.net/pic/_o/db/9c/8bd91fb17f573bf97f3147e1df7c_640_900.c8.jpg
        2880736071 http://imgst.meilishuo.net/pic/_o/e9/f7/41314b0d3bd895a56f0b05be6c65_640_900.c8.jpg
        """
        ret = []
        for line in file(fn):
            fields = line.strip().split()
            ret.append(fields)
        return ret

    def get_index(self, name):
        return self._data[name]

    def search(self, feature, category, num=5):
        flann = self.get_index(category)
        return flann.nn_index(feature, num_neighbors=num, **self._para)

if __name__=='__main__':
    pass

