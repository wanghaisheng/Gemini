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
from time import time
from argparse import ArgumentParser
import subprocess
import StringIO
import logging

from contexttimer import Timer
import numpy as np
from pyflann import FLANN



from samelib.utils import setup_logger
from samelib.config import Config
from samelib.twitter import TwitterInfo

conf = Config('./conf/build.yaml')

LOG_LEVEL       = logging.DEBUG
LOG_FILE        = conf['WORK_BASE_PATH'] + '/log/build_weekly.log'
LOG_SCREEN_FILE = conf['WORK_BASE_PATH'] + '/log/build_weekly.screen.log'
DATA_PATH       = conf['WORK_BASE_PATH'] + '/data/index_base'
MIN_SHOW_PV     = conf['MIN_SHOW_PV']
HIVE_PATH       = conf['HIVE_PATH']

logger = setup_logger('WEK', LOG_FILE, LOG_LEVEL)
screen_logger = setup_logger('WEK.SCR', LOG_SCREEN_FILE, LOG_LEVEL)

def build_flann_index(args):
    """

    """
    with Timer() as t:
        feature_data, tid2url = merge_all_feature_data()
        feature_data.save(TODO)
    logger.info("[%s] merge_daily_feature_data " % t.elapsed)

    with Timer() as t:
        flann = FLANN()
        params = flann.build_index(feature_data, target_precision=0.90, build_weight=0.01,
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
 select A.twitter_id, A.goods_id, A.shop_id, A.goods_first_catalog, A.goods_img  from
 ( select twitter_id, goods_id, shop_id, goods_img, goods_first_catalog
   from ods_brd_shop_goods_info
   where goods_status=1 )
 A join
 ( select distinct(twitter_id)
   from ods_dolphin_stat_sell_pool
   where dt in (%(dates)s) and yesterday_shows > %(show)s 
)
B on (A.twitter_id = B.twitter_id) ; """ % {'dates': days_string, 'show': MIN_SHOW_PV, 'hive_out': hive_out }
    cmds = [HIVE_PATH +'/hive', '-e', hql.replace("\n", "")]

    logger.info("execute hive with date in (%s) and threshold=%s" % (days_string, MIN_SHOW_PV))
    screen_logger.info("execute hive cmd %s" % " ".join(cmds))

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





#----------------------------------------------------------------------
def main(args):
    """
    """
    data_dir = DATA_PATH + '/base_%s_%s' % (args.start.strftime("%Y%m%d"), args.end.strftime("%Y%m%d"))
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    twitter_info_file = data_dir + '/twitter_info_file'
    if args.force or not os.path.exists(twitter_info_file):
        get_twitter_info_by_hive(twitter_info_file, args.start, args.end, pv = MIN_SHOW_PV)
        twitter_info = TwitterInfo()
        with Timer() as t:
            twitter_info.load(twitter_info_file)
        logger.info("[%s] twitter info file %s is loaded" % (t.elapsed, twitter_info_file))

        if args.basefile: # 如果指定了base， 更新twitter_info_file
            twitter_info_base = TwitterInfo()
            with Timer() as t:
                twitter_info_base.load(args.basefile)
            logger.info("[%s] base twitter info file %s is loaded" % (t.elapsed, args.basefile))
            twitter_info.merge(twitter_info_base)
            twitter_info.save(twitter_info_file)
    else:
        logger.info("twitter_info_file %s is ready" % twitter_info_file )

    # 只进行第一步。
    sys.exit(0)
    # feature_data, twitter_and_url = prepare_feature_data(feature_leveldb, twitter_info)
    #
    # for category in split_feature_into_category()
    #     build_flann_index()
    #     build_group_index()
    #
    #
    # flann = build_flann_index(args)
    # build = build_groupid_index(args)





if __name__=='__main__':

    args = parse_args()
    main(args)
