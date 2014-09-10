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

import MySQLdb


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
#LOG_FILE = WORK_BASE_PATH + '/log/build_label_daily.log'
LOG_FILE = WORK_BASE_PATH + '/log/build_tid.log'
#DATA_PATH = WORK_BASE_PATH + '/data/index_label_daily'
DATA_PATH = WORK_BASE_PATH + '/data/index_tid'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'
HIVE_PATH       = conf['HIVE_PATH']

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']

ITER_RANGE = 1000  # 图片加载计算特征，每100个输出一次状态。
ITER_RANGE2 = 1000  # 同款组聚类，每500个完成一次输出。
IMAGE_SERVER = 'http://imgst.meilishuo.net'
GROUPING_DISTANCE = 0.09  # 平方距离（欧式距离平方）阈值。不区分类别时取0.3*0.3
NUM_NEIGHBORS = 10  #

logger = setup_logger('BLD', LOG_FILE, LOG_LEVEL)


def get_twitter_info_by_hive(fn, date):
    """
    根据商品基本信息表(goods_info_new)和 审核表（twitter_verify_operation), 获得需要建库的所有商品信息。
    """
    hive_out = os.path.dirname(fn) + '/hive_out'
    if os.path.exists(hive_out):
        logger.info("remove hive output dir %s" % hive_out)
        os.removedirs(hive_out)
    os.makedirs(hive_out)

    date_string = date.strftime("%Y-%m-%d %H:%M:%S")

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
       where op_date >= unix_timestamp('%(dates)s')
    )
    B on (A.twitter_id = B.twitter_id) ; """ % {'dates': date_string, 'hive_out': hive_out}
    cmds = [HIVE_PATH + '/hive', '-e', hql.replace("\n", "")]

    logger.info("execute hive with date in (%s) with %s" % (date_string, " ".join(cmds)))

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

def get_dolphin_db():
    """ get_master_db的配置过时了"""
    db = MySQLdb.connect(host='172.16.0.215', user='meiliwork', passwd='Tqs2nHFn4pvgw',
                         db='dolphin', port=3306)
    return db


def get_brdshop_db():
    """ """
    db = MySQLdb.connect(host='172.16.5.33', user='dbreader', passwd='wearefashions',
                         db='brd_shop', port=3504)
    return db


def load_from_txt_tid(fn, info_file):
        tids=[]
        for line in open(fn):
            tids.append(line)

        db2 = get_brdshop_db()
        cursor2 = db2.cursor()
        sqlt = "select twitter_id, goods_id, shop_id, goods_img from brd_shop_goods_info_new where twitter_id in (%s);"
        tid2url = {}
        sql = sqlt % ",".join(map(str, tids))
        #print sql
        cursor2.execute(sql)
        res = cursor2.fetchall()
        goods_ids=[]
        for r in res:
            twitter_id, goods_id, shop_id, goods_img = map(str, r)
            print "for db2:"+twitter_id+" "+goods_id+" "+shop_id+" "+goods_img

            tid2url[twitter_id] = (goods_id, shop_id, goods_img)
            goods_ids.append(goods_id)
        
        db = get_dolphin_db()
        cursor = db.cursor()
        sqlt = "select goods_id, catalog_id from t_dolphin_catalog_goods_map where goods_id in (%s);"
        sql = sqlt % ",".join(map(str, goods_ids))
        print sql
        cursor.execute(sql)
        res = cursor.fetchall()
        id2catalog={}
        for r in res:
            goods_id,catalog_id = map(str,r)
            id2catalog[goods_id] = catalog_id
            print "from db:"+goods_id+" "+catalog_id       
        tmp=open(info_file, 'wb') 
        for k,v in tid2url.items():
              goods_id, shop_id, goods_img = map(str, v) 
              catalog_id=id2catalog[goods_id]
              #self._data.append(line.strip().split("\t"))
              #print k+" "+goods_id+" "+shop_id+" "+catalog_id+" "+goods_img
              #self._data.append((k,goods_id,shop_id,catalog_id,goods_img)) 
              tmp.write(k+'\t'+goods_id+'\t'+shop_id+'\t'+catalog_id+'\t'+goods_img+'\n')
 

def prepare_twitter_raw_info(info_file, date, force=False):
    """ """
    twitter_info = TwitterInfo()
    if os.path.exists(info_file):
        logger.info("twitter_info_file %s is ready" % info_file)
    if force or not os.path.exists(info_file):
        get_twitter_info_by_hive(info_file, date)
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

    data_dir = DATA_PATH + '/%s' % (args.date.strftime("%Y%m%d"))
    #data_dir ="/home/work/Gemini/data/index_tid/daily_20140824_20140834"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    logger.info("==================== start %s daily build %s ======================" % (args.date.strftime("%Y%m%d"), data_dir))

    twitter_info_raw_file = data_dir + '/twitter_info_raw'
    #input_dir="/home/work/luomingshan/data/%s" % (args.date.strftime("%Y%m%d"))
    #input_file= input_dir + '/all_tids_in_popular'
    input_file = '/home/work/Gemini/tmp.in.tid'
    load_from_txt_tid(input_file ,twitter_info_raw_file)
    #load_from_txt_tid("/home/work/luomingshan/20140829/all_tid",twitter_info_raw_file)
    twitter_info_raw = prepare_twitter_raw_info(twitter_info_raw_file, args.start, 0)#force=args.force)

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
    parser = ArgumentParser(description='建立最近一周（date到最近一个星期天）相似图片索引的脚本，每周定时启动。')
    parser.add_argument("--date", help="build date like 2014-06-30. default yesterday.")
    parser.add_argument("--force", action='store_true', help="force to re-compute every step.")

    parser.add_argument("--input", help="从文件中获取出入，而不是从数据库中查询.")
    #parser.add_argument("--output", help="将同款结果写入文件，而不是数据库.")
 
    args = parser.parse_args()

    if args.date is None:
        args.date = datetime.date.today()
    else:
        args.date = datetime.datetime.strptime(args.date, "%Y-%m-%d")

    cur = args.date
    while cur.isoweekday() != 7: # 最近一个周日
        cur -= datetime.timedelta(days=1)
    args.start = cur 

    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)


