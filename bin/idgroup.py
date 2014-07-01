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
import cpickle

from utils import flat2dict, flat2dict_ext

class IDGroup():
    """
    同款组
    """

    def __init__(self):
        self._tid2groupid = {}
        self._groupid2tid = {}
        self._tid2pos = {}
        self._feature_data = None

    def set_feature(self, data):
        self._feature_data = data

    def load(self, fn):
        """
         读取pickle中的映射信息
        """
        with open(fn, 'rb') as fh:
            tid2groupid, groupid2tid = cpickle.load(fh)
            self._tid2groupid = tid2groupid
            self._groupid2tid = groupid2tid

        return True

    def save(self, fn):
        """
         将tid和group id映射信息存入fn。
        """
        with open(fn, 'wb') as fh:
            ret = (self._tid2groupid, self._groupid2tid)
            # protocal = 2
            cpickle.dump(ret, fh, 2)
        return True

    def parse(self, tid_file, groupid_file):

        self._tid2groupid = flat2dict(tid_file)
        self._groupid2tid = flat2dict_ext(groupid_file)

        for k, v in self._groupid2tid.iteritems():
            self._groupid2tid[k] = set(v)

        return True


    def dump(self, tid_file, groupid_file):

        with open(tid_file, 'w') as fh:
            for k in sorted(self._tid2groupid):
                print >> fh, "%s\t%s" % (k, self._tid2groupid[k])

        with open(groupid_file, 'w') as fh:
            for k in sorted(self._groupid2tid):
                print >> fh, "%s\t%s" % (k, "\t".join(self._groupid2tid[k]))


    def get_group(self, tid):

        return self._tid2groupid[tid]

    def get_member(self, group_id):

        return self._groupid2tid[group_id]

    def insert(self, group_id, neighbor, feature=None):

        # if self._feature_data is None:
        #     raise ValueError("feature data set is not initialized")
        if group_id

            TODO

if __name__=='__main__':
    pass
