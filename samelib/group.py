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

    def set_info(self, pos2group, group2pos):

        self._pos2groupid = pos2group
        self._groupid2pos = group2pos

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

    def from_pos_txt(self, pos_file, group_file, sep="\t"):
        """
        解析文本格式的同款组文件 pos2groupid和group2pos文件。
        @param pos_file: pos2groupid文件
        @param group_file:  group2pos文件
        @return: True
        """

        self._pos2groupid = {}
        self._groupid2pos = {}

        for line in open(pos_file):
            k, v = line.strip().split(sep)
            self._pos2groupid[int(k)] = int(v)

        for line in open(group_file):
            fields = line.strip().split(sep)
            self._groupid2pos[int(fields[0])] = set(map(int, fields[1:]))

        return True


    def to_txt(self, pos_file, group_file, twitter_info=None):
        """
        twitter_info保存tid信息，如果为None，直接存pos信息。
        """
        if twitter_info is None:
            with open(pos_file, 'w') as fh:
                for k, v in self._pos2groupid.iteritems():
                    print >> fh, "%s\t%s" % (k, v)
            with open(group_file, 'w') as fh:
                for k, v in self._groupid2pos.iteritems():
                    print >> fh, "%s\t%s" % (k, "\t".join(map(str,v)))
        else:
            with open(pos_file, 'w') as fh:
                for k, v in self._pos2groupid.iteritems():
                    print >> fh, "%s\t%s" % (twitter_info[k][0], twitter_info[v][0])
            with open(group_file, 'w') as fh:
                for k,v in self._groupid2pos.iteritems():
                    print >> fh, "%s\t%s" % (k, "\t".join(map(lambda x : twitter_info[x][0], v)))


    def get_group(self, pos):
        return self._pos2groupid.get(pos)

    def get_group_list(self):
        return self._groupid2pos.keys()

    def get_member(self, group_id):
        return self._groupid2pos.get(group_id)

    def get_url(self, pos):
        if self._tid_and_url:
            return self._tid_and_url[pos][-1]
        else:
            return None

    def get_distance_matrix(self, points):
        vector = self._feature_data[points]
        return scipy.spatial.distance.cdist(vector, vector, 'sqeuclidean')

    def insert(self, group_id, neighbor, feature=None):

        # if self._feature_data is None:
        #     raise ValueError("feature data set is not initialized")
        # TODO 各种算法
        if group_id:
            pass


if __name__=='__main__':
    pass
