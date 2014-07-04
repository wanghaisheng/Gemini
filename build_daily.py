#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 每天，将本周的数据建成一个小库。供线上查询
 History:
     1. 2014/7/1 0:34 : build_daily.py is created.
"""



import sys
import os
import datetime
from time import time
from argparse import ArgumentParser

from contexttimer import Timer
import numpy as np
from pyflann import FLANN

from idgroup import IDGroup
from utils import setup_logger


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'build.log'

logger = setup_logger('BLD', LOG_FILE, LOG_LEVEL)


def build_flann_index(args):
    """

    """
    with Timer() as t:
        feature_data, tid2url = merge_daily_feature_data()
        feature_data.save(TODO)
    logger.info("[%s] merge_daily_feature_data " % t.elapsed)

    with Timer() as t:
        flann = FLANN()
        params = flann.build_index(feature_data, target_precision=0.95, build_weight=0.01,
                                   memory_weight=0.01, sample_fraction=0.2)
        flann.save_index(TODO)

    logger.info("[%s] build index by flann. returned paras are : %s " % (t.elapsed, params))

    return flann


def build_group_index(args):

    group = IDGroup()
    group.load(TODO)

    for tid in new_tid_list:
        group.insert(tid, )


#----------------------------------------------------------------------
def parse_args():
    """
    """
    parser = ArgumentParser(description='每天定时执行的脚本，将本周数据建立索引.')
    parser.add_argument("--date", metavar='DATE', help="build date like 2014-06-30. default yesterday.")
    args = parser.parse_args()
    if args.date is None:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        args.date = yesterday.strftime("%Y-%m-%d")

    return args


#----------------------------------------------------------------------
def main(args):
    """
    """
    flann = build_flann_index(args)
    build_groupid_index(args)





if __name__=='__main__':
    args = parse_args()
    main(args)

