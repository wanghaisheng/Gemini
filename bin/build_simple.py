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

from contexttimer import Timer
import numpy as np
from pyflann import FLANN


from utils import setup_logger, try_load_npy


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

    return flann


def main(args):
    """
    """
    flann = build_flann_index(args.feature, args.tid)
    index_file = args.output + '/feature_index'
    flann.save_index(index_file)


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
