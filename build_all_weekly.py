#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 每周对整体数据进行build，构建flann索引文件和组id文件
 History:
     1. 2014/7/1 16:34 : build_weekly.py is created.
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
from samelib.build import build_pipeline_with_twitter_info_raw

conf = Config('./conf/build.yaml')

LOG_LEVEL       = logging.DEBUG

WORK_BASE_PATH  = conf['WORK_BASE_PATH']
LOG_FILE        = WORK_BASE_PATH + '/log/build_weekly.log'
LOG_SCREEN_FILE = WORK_BASE_PATH + '/log/build_weekly.screen.log'
DATA_PATH       = WORK_BASE_PATH + '/data/index_all_weekly'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'

MIN_SHOW_PV     = conf['MIN_SHOW_PV']
HIVE_PATH       = conf['HIVE_PATH']

GOODS_CATEGORY = conf['GOODS_CATEGORY']

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']

ITER_RANGE      = 10000  # 图片加载计算特征，每100个输出一次状态。
ITER_RANGE2      = 10000  # 同款组聚类，每500个完成一次输出。
IMAGE_SERVER    = 'http://imgst.meilishuo.net'
NUM_NEIGHBORS    = 10     #

logger = setup_logger('BLD', LOG_FILE, LOG_LEVEL)


def get_twitter_info_by_hive(fn, start, end, pv):
    """
    根据商品基本信息表(goods_info_new)和展现表（sell_pool), 获得需要建库的所有商品信息。
    sqoops定时每天凌晨从stat库导出sell_pool， 但sell_pool每天8点左右数据才ok，所以昨天的数据是有问题的。根据小库补救一下。

    基本信息表中的catalog是商品侧的，与t_dolphin_catalog_goods_map完全不一样。

    @param fn: hive输出文件名
    @param start: datetime对象，时间起点
    @param end 　: datetime对象， 时间终点
    @param pv    : 展现频次阈值
    """
    days = []
    cur = start
    while cur <= end:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += datetime.timedelta(days=1)
    days_string = ','.join(map(lambda x: "'%s'" % x, days))

    hive_out = os.path.dirname(fn) + '/hive-out'
    if os.path.exists(hive_out):
        logger.info("remove hive output dir %s" % hive_out )
        os.removedirs(hive_out)
    os.makedirs(hive_out)
        

    hql = """
 set hive.exec.compress.output=false;
 insert overwrite local directory '%(hive_out)s'
 row format delimited  FIELDS TERMINATED BY '\t'
 select A.twitter_id, A.goods_id, A.shop_id, C.catalog_id, A.goods_img  from
 ( select twitter_id, goods_id, shop_id, goods_img, goods_first_catalog
   from ods_brd_shop_goods_info
   where goods_status=1 )
 A join
 ( select distinct(twitter_id)
   from ods_dolphin_stat_sell_pool_new 
   where dt in (%(dates)s) and yesterday_shows > %(show)s 
)
B on (A.twitter_id = B.twitter_id) join
( select goods_id, catalog_id
  from ods_dolphin_catalog_goods_map)
C on (A.goods_id = C.goods_id); """ % {'dates': days_string, 'show': pv, 'hive_out': hive_out }

    cmds = [HIVE_PATH +'/hive', '-e', hql.replace("\n", "")]
    logger.info("execute hive with date in (%s) and threshold=%s" % (days_string, pv))
    logger.info("execute hive cmd %s" % " ".join(cmds))

    with Timer() as t:
        ps = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ps.wait()

    logger.info("[%s] hive cmds returns %s. the screen output is :\n %s" % (t.elapsed, ps.returncode, ps.stdout.read()))
    if ps.returncode != 0:
        logger.info("script exit since the hive retcode is not zero")
        sys.exit(1)

    # cmds = ["sed 's/\x01/\x09/g' %s/0* > %s" % (hive_out, fn)]
    cmds = ["cat %s/0* > %s" % (hive_out, fn)]
    logger.info("merge hive output by cmd :  %s" % (cmds))
    with Timer() as t:
        ps = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        ps.wait()
    logger.info("[%s] shell retcode=%s and the screen output is :\n  %s" % (t.elapsed, ps.returncode, ps.stdout.read()))
    if ps.returncode != 0:
        logger.info("script exit since the shell retcode is not zero")
        sys.exit(1)

    return True



def prepare_twitter_raw_info(info_file, start, end, pv, base_info_file=None, force=False):
    """ """
    twitter_info = TwitterInfo()

    if force or not os.path.exists(info_file):
        get_twitter_info_by_hive(info_file, start, end, pv)
        with Timer() as t:
            twitter_info.load(info_file)
        logger.info("[%s] twitter info file %s is loaded" % (t.elapsed, info_file))

        if base_info_file:  # 如果指定了base， 更新twitter_info_raw_file
            twitter_info_base = TwitterInfo()
            with Timer() as t:
                twitter_info_base.load(base_info_file)
            logger.info("[%s] base twitter info file %s is loaded" % (t.elapsed, base_info_file))
            twitter_info.merge(twitter_info_base)
            twitter_info.save(info_file)
    else:
        logger.info("twitter_info_file %s is ready" % info_file)
        twitter_info.load(info_file)

    return twitter_info



def main(args):
    """
    """

    data_dir = DATA_PATH + '/all_%s_%s' % (args.start.strftime("%Y%m%d"), args.end.strftime("%Y%m%d"))
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    logger.info("==================== start to build %s ======================" % data_dir )

    twitter_info_raw_file = data_dir + '/twitter_info_raw'
    twitter_info_raw = prepare_twitter_raw_info(twitter_info_raw_file, args.start, args.end, pv=MIN_SHOW_PV,
                                                base_info_file=args.basefile, force=args.force)

    shop_stat = build_pipeline_with_twitter_info_raw(twitter_info_raw, data_dir, force=args.force)

    path_to_current = DATA_PATH+'/current'
    if os.path.exists(path_to_current):
        if os.path.islink(path_to_current):
            os.unlink(path_to_current)
        else:
            logger.fatal("%s is not a symbolic link!" % path_to_current)
    os.symlink(data_dir, path_to_current)


def parse_args():
    """
    """
    parser = ArgumentParser(description='建立相似图片检测全量索引的脚本，每周定时启动。')
    parser.add_argument("--start", help="build date like 2014-06-24. default 7 days before the end.")
    parser.add_argument("--end", help="build date like 2014-06-30. default yesterday.")
    parser.add_argument("--basefile", help="旧twitter信息文件， 等待融合.")
    parser.add_argument("--force", action='store_true', help="force to re-compute every step.")
    args = parser.parse_args()

    if args.end is None:
        yesterday = datetime.date.today() - datetime.timedelta(days=1)
        args.end = yesterday
    else:
        args.end = datetime.datetime.strptime(args.end, "%Y-%m-%d")

    if args.start is None:
        start_date = args.end - datetime.timedelta(days=6)
        args.start = start_date
    else:
        args.start = datetime.datetime.strptime(args.start, "%Y-%m-%d")

    if args.basefile is not None:
        if not os.path.exists(args.basefile):
            print "%s file do not exist" % args.basefile
            parser.print_help()
            sys.exit(1)

    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)
