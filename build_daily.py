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
import time
from argparse import ArgumentParser
import subprocess
import StringIO
import logging
import json

from contexttimer import Timer
import numpy as np
import scipy
from pyflann import FLANN
import leveldb

from samelib.utils import setup_logger
from samelib.config import Config
from samelib.twitter import TwitterInfo
from samelib.utils import try_load_npy
from feature.feature import download_and_compute_feature
from samelib.group import Group
from samelib.build import prepare_twitter_info_with_feature, build_flann_index, build_group_index, stat_shop_group_info


conf = Config('./conf/build.yaml')

LOG_LEVEL = logging.DEBUG

WORK_BASE_PATH = conf['WORK_BASE_PATH']
LOG_FILE = WORK_BASE_PATH + '/log/build_daily.log'
DATA_PATH = WORK_BASE_PATH + '/data/index_day'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']

ITER_RANGE = 1000  # 图片加载计算特征，每100个输出一次状态。
ITER_RANGE2 = 100  # 同款组聚类，每500个完成一次输出。
IMAGE_SERVER = 'http://imgst.meilishuo.net'
GROUPING_DISTANCE = 0.09  # 平方距离（欧式距离平方）阈值。不区分类别时取0.3*0.3
NUM_NEIGHBORS = 10  #

logger = setup_logger('BLD', LOG_FILE, LOG_LEVEL)
db = leveldb.LevelDB(FEATURE_DB_PATH)


def get_twitter_info_by_hive(fn, start):
    """
    根据商品基本信息表(goods_info_new)和 审核表（twitter_verify_operation), 获得需要建库的所有商品信息。
    sqoops定时每天凌晨从stat库导出sell_pool， 但sell_pool每天8点左右数据才ok，所以昨天的数据是有问题的。根据小库补救一下。
    基本信息表中的catalog是商品侧的，与t_dolphin_catalog_goods_map完全不一样。

    @param fn: hive输出文件名
    @param start: datetime对象，时间起点
    @param end 　: datetime对象， 时间终点
    @param pv    : 展现频次阈值
    """

    hive_out = os.path.dirname(fn) + '/hive_out'
    if os.path.exists(hive_out):
        logger.info("remove hive output dir %s" % hive_out)
        os.removedirs(hive_out)
    os.makedirs(hive_out)

    hql = """
     set hive.exec.compress.output=false;
     insert overwrite local directory '%(hive_out)s'
     select A.twitter_id, A.goods_id, A.shop_id, B.catalog_id, A.goods_img from
     ( select twitter_id, goods_id, shop_id, goods_img,
       from ods_brd_shop_goods_info
       where goods_status=1 )
     A join
     ( select twitter_id, catalog_id
       from ods_dolphin_twitter_verify_operation
       where dt > %(dates)s
    )
    B on (A.twitter_id = B.twitter_id) ; """ % {'dates': days_string, 'hive_out': hive_out}
    cmds = [HIVE_PATH + '/hive', '-e', hql.replace("\n", "")]

    logger.info("execute hive with date in (%s) with %s" % (days_string, " ".join(cmds)))

    with Timer() as t:
        ps = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ps.wait()

    logger.info("[%s] hive cmds returns %s" % (t.elapsed, ps.returncode))
    logger.info("hive cmds print to screen:\n %s" % ps.stdout.read())
    if ps.returncode != 0:
        sys.exit(1)

    cmds = ["sed 's/\x01/\x09/g' %s/0* > %s" % (hive_out, fn)]
    with Timer() as t:
        ps = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        ps.wait()
    logger.info("[%s] retcode=%s. run cmd :  %s" % (t.elapsed, ps.returncode, cmds))
    logger.info("sed cmds print to screen:\n %s" % ps.stdout.read())
    if ps.returncode != 0:
        sys.exit(1)

    return True


def prepare_twitter_raw_info(info_file, start, end, pv, base_info_file=None, force=False):
    """ """
    twitter_info = TwitterInfo()

    if force or not os.path.exists(info_file):
        get_twitter_info_by_hive(info_file, start, end)
        with Timer() as t:
            twitter_info.load(info_file)
        logger.info("[%s] twitter info file %s is loaded" % (t.elapsed, info_file))
    else:
        logger.info("twitter_info_file %s is ready" % info_file)
        twitter_info.load(info_file)

    return twitter_info




def main(args):
    """
    """

    data_dir = DATA_PATH + '/daily_%s_%s' % (args.date.strftime("%Y%m%d"))
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    logger.info("==================== start %s daily build %s ======================" % (args.date.strftime("%Y%m%d"), data_dir))

    twitter_info_raw_file = data_dir + '/twitter_info_raw'
    twitter_info_raw = prepare_twitter_raw_info(twitter_info_raw_file, args.date, force=args.force)

    twitter_info_file = data_dir + '/twitter_info'
    feature_file = data_dir + '/feature_data'
    info_data_raw = twitter_info_raw.get_data()
    twitter_info, feature_data = prepare_twitter_info_with_feature(twitter_info_file, feature_file, info_data_raw,
                                                                   force=args.force)

    index_file = data_dir + '/flann_index'
    index_para_file = data_dir + '/flann_index_para'
    flann, params = build_flann_index(index_file, index_para_file, feature_data, force=args.force,
                                      algorithm="autotuned")



def parse_args():
    """
    """
    parser = ArgumentParser(description='建立最近一周（date到最近一个星期天）相似图片索引的脚本，每周定时启动。')
    parser.add_argument("--date", help="build date like 2014-06-30. default yesterday.")
    parser.add_argument("--force", action='store_true', help="force to re-compute every step.")
    args = parser.parse_args()

    if args.date is None:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        args.date = yesterday
    else:
        args.date = datetime.datetime.strptime(args.date, "%Y-%m-%d")

    args.dates = []
    cur = args.date
    while cur.isoweekday() != 'saturday':
        args.dates.append(cur)
        cur -= datetime.timedelta(days=1)

    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)


