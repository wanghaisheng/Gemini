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
from contexttimer import Timer

from twitter import TwitterInfo
from utils import try_load_npy, setup_logger
from config import Config

conf = Config('../build.yaml')
GOODS_CATEGORY = conf['GOODS_CATEGORY']
category_names = map(lambda x:x['name'], GOODS_CATEGORY)

logger = setup_logger('SVR')

class Index():

    def __init__(self):
        """
        _para is from flann.build_index()
        """
        global category_names
        self._flann_categories = {}
        self._flann_para_categories = {}
        self._twitter_info_categories = {}
        self._feature_data_categories = {}
        for c_name in category_names:
            self._feature_data_categories[c_name] = None
            self._twitter_info_categories[c_name] = TwitterInfo()
            self._flann_categories[c_name] = FLANN()
            self._flann_para_categories[c_name] = None


    def clear(self):
        self._data = {}

    def load(self, dir):
        """
        dir:  path to index files
        """
        global category_names
        for c_name in category_names:
            if not os.path.exists(dir + "/%s_twitter_info"):
                continue
            self._twitter_info_categories[c_name].load(dir+"/%s_twitter_info" % c_name)
            self._feature_data_categories[c_name] = try_load_npy(dir+'/%s_feature_data' % c_name)
            self._flann_categories[c_name].load_index(dir+'/%s_flann_index' % c_name, self._feature_data_categories[c_name])
            self._flann_para_categories[c_name] = json.load(open(dir+'/%s_flann_index_para' % c_name))
        return True

    def save(self, dir):
        """
        dir:  path to index files
        """
        global category_names
        for c_name in category_names:
            if c_name not in self._twitter_info_categories:
                continue
            self._twitter_info_categories[c_name].save(dir+"/%s_twitter_info" % c_name)
            self._feature_data_categories[c_name].save(dir+'/%s_feature_data' % c_name)
            self._flann_categories[c_name].save_index(dir+'/%s_flann_index' % c_name)
            json.dump(self._flann_para_categories[c_name], dir + '/%s_flann_index_para' % c_name )
        return True

    def search(self, c_name, feature, neighbors=5, ):
        flann = self._flann_categories[c_name]
        para = self._flann_para_categories[c_name]
        return flann.nn_index(feature, num_neighbors=neighbors, **para)

    def get_tid(self, c_name, pos):
        twitter_info = self._twitter_info_categories[c_name]
        return twitter_info[pos][0]

    def get_twitter_info(self, c_name, pos=None):
        if pos is None:
            return self._twitter_info_categories[c_name]
        else:
            return self._twitter_info_categories[c_name][pos]

    def insert(self, c_name, feature_data, twitter_info):
        """
        注意： 只有liner index支持插入，任何索引一旦插入，则变为linear索引
        返回插入数据在索引中的起始位置
        """
        offset = self._twitter_info_categories[c_name].get_length()

        self._feature_data_categories[c_name] = np.append(self._feature_data_categories[c_name], feature_data)
        self._twitter_info_categories[c_name].set_data(self._twitter_info_categories[c_name].get_data() + twitter_info.get_data())
        para = self._flann_categories[c_name].build_index(self._feature_data_categories[c_name], algorithm='linear')
        self._flann_para_categories = para
        return offset

    def shrink(self, max_size, min_size):
        """ 如果索引超过一定规模，则进行瘦身。
        """
        assert(max_size > min_size)
        global category_names
        for c_name in category_names:
            twitter_info = self._twitter_info_categories[c_name]
            n = twitter_info.get_length()
            if n <= max_size:
                continue
            else:
                with Timer() as t:
                    twitter_data = self._twitter_info_categories[c_name].get_data()[-min_size:]
                    feature_data = self._feature_data_categories[c_name][-min_size:]
                    para = self._flann_categories[c_name].build_index(feature_data, algorithm='linear')
                    self._flann_para_categories[c_name] = para
                    self._twitter_info_categories[c_name].set_data(twitter_data)
                    self._feature_data_categories[c_name] = feature_data
                logger.info("[%s] shrink rt index %s from %s to %s " % (t.elapsed, c_name, n, min_size))

        return






if __name__=='__main__':
    pass

