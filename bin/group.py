#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 同款组的相关类
 History:
     1. 2014/7/1 15:32 : idgroup.py is created.
"""



import sys
import os
import cPickle

import scipy.spatial

from utils import flat2dict, flat2dict_ext, flat2list

class Group():
    """
    同款组
    """

    def __init__(self):
        self._pos2groupid = {}
        self._groupid2pos = {}
        self._tid_and_url = {}
        self._feature_data = None

    def set_feature(self, data):
        self._feature_data = data

    def load(self, fn):
        """
         读取pickle中的同款组映射信息
        """
        with open(fn, 'rb') as fh:
            pos2groupid, groupid2pos = cPickle.load(fh)
            self._pos2groupid = pos2groupid
            self._groupid2pos = groupid2pos
        return True

    def load_tid(self, fn):
        """
        读取 twitter， 更新时间和image url的数据
        """
        self._tid_and_url = flat2list(fn, sep=' ')

    def tid_num(self):
        if self._tid_and_url:
            return len(self._tid_and_url)
        else:
            return 0

    def save(self, fn):
        """
         将tid和group id映射信息存入fn。
        """
        with open(fn, 'wb') as fh:
            ret = (self._pos2groupid, self._groupid2pos)
            # protocal = 2
            cPickle.dump(ret, fh, protocol=2)
        return True

    def from_txt(self, pos_file, group_file):
        """
        解析文本格式的同款组文件 pos2groupid和group2pos文件。
        @param pos_file: pos2groupid文件
        @param group_file:  group2pos文件
        @return: True
        """

        self._pos2groupid = flat2dict(tid_file)
        self._groupid2pos = flat2dict_ext(groupid_file)

        for k, v in self._groupid2pos.iteritems():
            self._groupid2pos[k] = set(v)

        return True


    def to_txt(self, pos_file, group_file):

        with open(pos_file, 'w') as fh:
            for k in sorted(self._pos2groupid):
                print >> fh, "%s\t%s" % (k, self._pos2groupid[k])

        with open(group_file, 'w') as fh:
            for k in sorted(self._groupid2pos):
                print >> fh, "%s\t%s" % (k, "\t".join(self._groupid2pos[k]))


    def get_group(self, pos):
        return self._pos2groupid[pos]

    def get_group_list(self):
        return self._groupid2pos.keys()

    def get_member(self, group_id):
        return self._groupid2pos[group_id]

    def get_url(self, pos):
        if self._tid_and_url:
            return self._tid_and_url[pos][-1]
        else:
            return None

    def get_distance_matrix(self, points):
        vector = self._feature_data[points]
        return scipy.spatial.distance.cdist(vector, vector)

    def insert(self, group_id, neighbor, feature=None):

        # if self._feature_data is None:
        #     raise ValueError("feature data set is not initialized")
        # TODO 各种算法
        if group_id:
            pass


if __name__=='__main__':
    pass
