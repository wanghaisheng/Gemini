#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 每天，将本周的数据建成一个小库。供线上查询
 History:
     1. 2014/7/14 0:34 : build_label_weekly.py is created.
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
from samelib.build import prepare_twitter_info_with_feature, build_pipeline_with_twitter_info_raw


conf = Config('./conf/build.yaml')

LOG_LEVEL = logging.DEBUG

WORK_BASE_PATH = conf['WORK_BASE_PATH']
LOG_FILE = WORK_BASE_PATH + '/log/build_label_weekly.log'
DATA_PATH = WORK_BASE_PATH + '/data/index_label_weekly'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'
HIVE_PATH       = conf['HIVE_PATH']

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']

ITER_RANGE = 1000  # 图片加载计算特征，每100个输出一次状态。
ITER_RANGE2 = 1000  # 同款组聚类，每500个完成一次输出。
IMAGE_SERVER = 'http://imgst.meilishuo.net'
NUM_NEIGHBORS = 10  #

logger = setup_logger('BLD', LOG_FILE, LOG_LEVEL)


def get_twitter_info_by_hive(fn):
    """
    根据商品基本信息表(goods_info_new)和 审核表（twitter_verify_operation), 获得需要建库的所有商品信息。
    """
    hive_out = os.path.dirname(fn) + '/hive_out'
    if os.path.exists(hive_out):
        logger.info("remove hive output dir %s" % hive_out)
        os.removedirs(hive_out)
    os.makedirs(hive_out)

    # date_string = date.strftime("%Y-%m-%d %H:%M:%S")

    hql = """
     set hive.exec.compress.output=false;
     set mapred.reduce.tasks=1;
     insert overwrite local directory '%(hive_out)s'
     row format delimited  FIELDS TERMINATED BY '\t'
     select A.twitter_id, A.goods_id, A.shop_id, B.catalog_id, A.goods_img from
     ( select twitter_id, goods_id, shop_id, goods_img
       from ods_brd_shop_goods_info)
     A join
     ( select twitter_id, catalog_id
       from ods_dolphin_twitter_verify_operation
       where (op_date >= unix_timestamp('2014-05-01 00:00:00') and business_type=1 and op_uid!=-100) 
    )
    B on (A.twitter_id = B.twitter_id) ; """ % {'hive_out': hive_out}
    cmds = [HIVE_PATH + '/hive', '-e', hql.replace("\n", "")]

    logger.info("execute hive with date 2014-01-01 with %s" % (" ".join(cmds)))

    with Timer() as t:
        ps = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ps.wait()

    logger.info("[%s] hive cmds returns %s" % (t.elapsed, ps.returncode))
    logger.info("hive cmds print to screen:\n %s" % ps.stdout.read())
    if ps.returncode != 0:
        sys.exit(1)

    cmds = ["cat %s/0* > %s" % (hive_out, fn)]
    with Timer() as t:
        ps = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        ps.wait()
    logger.info("[%s] retcode=%s. run cmd :  %s" % (t.elapsed, ps.returncode, cmds))
    logger.info("sed cmds print to screen:\n %s" % ps.stdout.read())
    if ps.returncode != 0:
        sys.exit(1)

    return True


def prepare_twitter_raw_info(info_file, force=False):
    """ """
    twitter_info = TwitterInfo()

    if force or not os.path.exists(info_file):
        get_twitter_info_by_hive(info_file)
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

    data_dir = DATA_PATH + '/label_util_%s' % (args.date.strftime("%Y%m%d"))
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    logger.info("==================== start %s label weekly build %s ======================" % (args.date.strftime("%Y%m%d"), data_dir))

    twitter_info_raw_file = data_dir + '/twitter_info_raw'
    twitter_info_raw = prepare_twitter_raw_info(twitter_info_raw_file, force=args.force)

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
    parser = ArgumentParser(description='建立所有已标注样本的相似图片索引，每周定时启动。')
    parser.add_argument("--date", help="build date like 2014-06-30. default today. 仅在创建文件夹时使用.")
    parser.add_argument("--force", action='store_true', help="force to re-compute every step.")
    args = parser.parse_args()

    if args.date is None:
        args.date = datetime.date.today()
    else:
        args.date = datetime.datetime.strptime(args.date, "%Y-%m-%d")

    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)


