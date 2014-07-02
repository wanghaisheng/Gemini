#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 对简单数据进行build，构建flann索引文件和组id文件
 History:
     1. 2014/7/1 16:34 : build_simple.py is created.
"""


import sys
import os
import datetime
from time import time
from argparse import ArgumentParser
import logging
import cPickle

from contexttimer import Timer
import numpy as np
from pyflann import FLANN


from utils import setup_logger, try_load_npy, flat2list


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'build_simple.log'

logger = setup_logger('WEK', LOG_FILE, LOG_LEVEL)


def build_flann_index(feature_file, tid_file):
    """

    """
    with Timer() as t:
        feature_data = try_load_npy(feature_file)
    logger.info("[%s] load feature data %s " % (t.elapsed, feature_file))

    with Timer() as t:
        flann = FLANN()
        params = flann.build_index(feature_data, algorithm='autotuned', target_precision=0.9, build_weight=0.01,
                                   memory_weight=0.01, sample_fraction=0.1)
    logger.info("[%s] build index by flann. returned paras are : %s " % (t.elapsed, params))

    return flann, params, feature_data

def build_group_1nn_simple(flann, feature_data, params, threshold):
    """ 最简单的近邻聚类方法： 只要tid的最近邻N（tid）在某同款组，则加入该组。"""

    with Timer() as t:
        neighbors, distances = flann.nn_index(feature_data, num_neighbors=1, **params)
    logger.info("[%s] query all points with nearest neighbors" % t.elapsed)


    # twitter用feature data中的pos表示
    pos2groupid = {}   # {10:5, 20:5}
    groupid2pos = {}   # {5:set(5, 10, 20)}
    n = len(neighbors)
    logger.info("start to cluster %s points into groups" % n)
    for i in xrange(n):
        if i % 1000 == 0:
            logger.debug("  %s points has been processed " % i)

        # 阈值之内才算neighbour， 返回数据按照距离排序
        current_distance = distances[i][0]
        if current_distance >= threshold:
            continue
        current_neighbor = neighbors[i][0]
        groupid = pos2groupid.get(current_neighbor)
        if groupid is not None:
            pos2groupid[i] = groupid
            groupid2pos[groupid].add(i)
        else:
            groupid2pos[i] = set((i, current_neighbor))
            pos2groupid[i] = i
            pos2groupid[current_neighbor] = i

    logger.info("clustering points into group is done")

    return pos2groupid, groupid2pos


def main(args):
    """
    """
    flann, params, feature_data = build_flann_index(args.feature, args.tid)
    index_file = args.output + '/feature_index'
    flann.save_index(index_file)
    pos2groupid, groupid2pos = build_group_1nn_simple(flann, feature_data, params, args.distance)

    group_file = args.output + '/group_pickle'
    cPickle.dump((pos2groupid, groupid2pos), group_file, protocol=2)


def parse_args():
    """
    """
    parser = ArgumentParser(description='输入特征文件和tid映射关系，构建flann索引和组文件.')
    parser.add_argument("--feature", metavar='FEATURE', help="feature文件路径.")
    parser.add_argument("--tid", metavar='TID', help="tid文件路径.")
    parser.add_argument("--distance", metavar='DISTANCE', type=float, help="同款的距离阈值， 欧式距离的平方.")
    parser.add_argument("--output", metavar='OUTPUT', help="输出文件夹.")
    args = parser.parse_args()
    if args.feature is None or not os.path.exists(args.feature):
        print "feature file do not exist"
        parser.print_help()
        sys.exit(1)
    if args.tid is None or not os.path.exists(args.tid):
        print "feature file do not exist"
        parser.print_help()
        sys.exit(1)        
    if args.distance is None:
        print "distance is not specified "
        parser.print_help()
        sys.exit(1)
    if args.output is None or not os.path.exists(args.output):
        os.mkdir(args.output)

    return args


if __name__=='__main__':
    args = parse_args()
    main(args)
