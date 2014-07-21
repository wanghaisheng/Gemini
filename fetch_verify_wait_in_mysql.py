#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 每半个小时（20分钟) 调度一次， 从数据库中取出有catalog_id，无同款id的数据，发给server，检索同款组id信息。
 History:
     1. 2014/7/14 10:04 : fetch_verify_wait_in_mysql.py is created.
"""


import sys
import os
from argparse import ArgumentParser
import logging
import json
from time import sleep

import MySQLdb
import requests
from contexttimer import Timer

from samelib.utils import setup_logger, chunks, catalog_id_to_name
from samelib.config import Config

conf = Config('./conf/build.yaml')

LOG_LEVEL = logging.DEBUG

WORK_BASE_PATH = conf['WORK_BASE_PATH']
LOG_FILE = WORK_BASE_PATH + '/log/fetch_verify_wait_in_mysql.log'

SAME_SERVER = "http://localhost:8773/result"

logger = setup_logger('DB', LOG_FILE, LOG_LEVEL)


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


class Query:
    """ 发送给同款组的twitter_id信息 """

    def __init__(self):
        self._data = []

    def load_from_txt(self, fn):
        self._data = []

        for line in open(fn):
            self._data.append(line.strip().split("\t"))
        

    def load_from_db(self):

        self._data = []
        
        db1 = get_dolphin_db()
        cursor1 = db1.cursor()

        # 选择条件： 一审待审， 有分类id已经计算， 同款id尚未计算。
        sql = "select twitter_id, goods_id, catalog_id from t_dolphin_twitter_verify_wait where verify_stat=0 and same_twitter_id=0 and catalog_id !=0;"
        cursor1.execute(sql)
        result = cursor1.fetchall()
        logger.info("sql returns %s results: %s" % (len(result), sql) )

        tid2data = {}
        for r in result:
            r = map(str, r)
            tid2data[r[0]] = [r[1], r[2]]


        db2 = get_brdshop_db()
        cursor2 = db2.cursor()
        sqlt = "select twitter_id, shop_id, goods_img from brd_shop_goods_info_new where twitter_id in (%s);"
        tid2url = {}
        for chk in chunks(tid2data.keys(), 1000):
            sql = sqlt % ",".join(map(str, chk))
            cursor2.execute(sql)
            res = cursor2.fetchall()
            for r in res:
                twitter_id, shop_id, goods_img = map(str, r)
                tid2url[twitter_id] = (shop_id, goods_img)
                
        for tid in tid2data:
            goods_id, catalog_id = tid2data[tid]
            if tid in tid2url:
                shopid, goods_img = tid2url[tid]
            else:
                shopid, goods_img = "", ""
            val = (tid, goods_id, shopid, catalog_id, goods_img)
            self._data.append(val)

    def save_to_txt(self, fn):
        fh = open(fn, "w")
        for v in self._data:
            print >> fh, "\t".join(map(str, v))
        fh.close()

    def get_data(self):
        return self._data


def post_to_same_server(query):

    data = query.get_data()
    post_data = []
    same_twitter_list = []
    for ele in data:
        if len(ele) != 5 or ele[4].strip() == '':
            # 没有url的数据，比如 select * from brd_shop_goods_info_new where twitter_id=2970750377;
            same_twitter_list.append({'twitter_id':ele[0], 'group_id':-1})
            continue
        r = {'twitter_id': ele[0], 'goods_id': ele[1], 'shop_id': ele[2], 'category': catalog_id_to_name(ele[3]), 'img_url': ele[4]}
        post_data.append(r)
    req = {'data': json.dumps(post_data),
           'method':'group'}
    
    max_tries = 3
    i = 1
    flag = False
    while(i <= max_tries):
        try:
            r = requests.post(SAME_SERVER, data=req)
            flag = True
            break
        except requests.exceptions.ConnectionError:
            # server刚启动, 短时间无法响应，这里等待一下
            logger.info("sleep %s th time for connecting the server" % i )
            sleep(60*i)
        i += 1
        
    if not flag:
        return None

    try:
        # {'status':0, 'message':'abc', 'data':[]}
        remote_data = json.loads(r.content)['data']
    except ValueError:
        logger.fatal("same server return %s: content is %s " % (r, r.content) )
        sys.exit(1)
    return json.loads(r.content)['data'] + same_twitter_list
        


def res_to_txt(res, fn):
    with open(fn, "w") as fh:
        for ele in res:
            line = "\t".join(map(str,[ele.get("twitter_id"), ele.get("group_id")] + ele.get('neighbors', [])))
            print >> fh, line

def res_to_db(res):
    db = get_dolphin_db()
    db.autocommit(True)
    cursor = db.cursor()
    logger.info("start to write same_twitter_id to db")
    for ele in res:
        twitter_id = ele['twitter_id']
        group_id = ele['group_id']
        sql = "update t_dolphin_twitter_verify_wait set same_twitter_id=%s where twitter_id = %s;" %(group_id, twitter_id)
        logger.info(sql)
        cursor.execute(sql)


def main(args):
    """
    """
    query = Query()
    # result = Result()

    if args.input is not None:
        query.load_from_txt(args.input)
    else:
        query.load_from_db()
    if args.save is not None:
        query.save_to_txt(args.save)

    n = len(query.get_data())
    with Timer() as t:
        data = post_to_same_server(query)
        # data = res['data']
    if data is None:
        logger.fatal("could not connect to the server")
        sys.exit(1)

    logger.info("[%s] %s/%s are judged by same server" % (t.elapsed, len(data), n) )


    if args.output is not None:
        res_to_txt(data, args.output)
    else:
        res_to_db(data)


def parse_args():
    """
    """
    parser = ArgumentParser(description='建立所有已标注样本的相似图片索引，每周定时启动。')
    parser.add_argument("--input", help="从文件中获取出入，而不是从数据库中查询.")
    parser.add_argument("--output", help="将同款结果写入文件，而不是数据库.")
    parser.add_argument("--save", help="将待检测数据写入文件，作为下次输入")    
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    args = parse_args()
    main(args)


