#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 管理同款服务的索引文件
 History:
     1. 2014/6/30 23:08 : index.py is created.
"""
import json

import sys
import os

import numpy as np
from pyflann import FLANN

from twitter import TwitterInfo
from utils import try_load_npy


class Index():

    def __init__(self):
        """
        _para is from flann.build_index()
        """
        self._flann = FLANN()
        self._flann_para = None
        self._twitter_info = TwitterInfo()
        self._feature_data = None


    def clear(self):
        self._data = {}

    def load(self, dir):
        """
        dir:  path to index files
        """
        self._twitter_info.load(dir+"/twitter_info")
        self._feature_data = try_load_npy(dir+'/feature_data')
        self._flann.load_index(dir+'/flann_index', self._feature_data)
        self._flann_para = json.load(open(dir+'/flann_index_para'))

        return True

    def search(self, feature, neighbors=5):
        return self._flann.nn_index(feature, num_neighbors=neighbors, **self._flann_para)

    def get_tid(self, pos):
        return self._twitter_info[pos][0]

    def get_twitter_info(self, pos=None):
        if pos is None:
            return self._twitter_info
        else:
            return self._twitter_info[pos]

    def insert(self, feature_data, twitter_info):
        """
        注意： 只有liner index支持插入，任何索引一旦插入，则变为linear索引
        返回插入数据在索引中的位置
        """
        offset = self._twitter_info.get_length()

        self._feature_data = np.append(self._feature_data, feature_data)
        self._twitter_info.set_data(self._twitter_info.get_data() + twitter_info.get_data())
        para = self._flann.build_index(self._feature_data, algorithm='linear')
        self._flann_para = para
        return offset

    def shrink(self, max_size, min_size):
        """ 如果索引超过一定规模，则进行瘦身。
        """
        assert(max_size > min_size)

        if self._twitter_info.get_length() <= max_size:
            return -1
        else:
            n = self._twitter_info.get_length()
            feature_data = self._feature_data[-min_size:]
            twitter_data = self._twitter_info.get_data()[-min_size:]
            para = self._flann.build_index(feature_data, algorithm='linear')
            self._flann_para = para
            self._twitter_info.set_data(twitter_data)
            self._feature_data = feature_data
            return n






if __name__=='__main__':
    pass

