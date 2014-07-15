#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose: 
     1. 将两列或者多列twitter数据转化为org文件，再export成html
 History:
     1. 2014/7/15 17:11 : plot_twitter_tuple.py is created.
"""



import sys
import os
from argparse import ArgumentParser
import cPickle
from collections import Counter
import time
import random

import MySQLdb
from samelib.group import Group
from samelib.utils import try_load_npy, distance_matrix
from ptutils.dictools import flat2list

IMAGE_URL = 'http://imgst.meilishuo.net/'

#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='将同款组文件两列或者多列，转化为org（html）格式，分析同款组质量.')
    parser.add_argument("input", metavar='INPUT', help="the group id pickle dump file.")
    parser.add_argument("output", metavar='OUTPUT', help="the output html file.")
    parser.add_argument("type", metavar='TYPE', default='twitter', help="twitter/good id, default twitter.")
    args = parser.parse_args()
    if args.type not in ('twitter', 'good'):
        parser.print_help()
        sys.exit(1)
    return args

def get_brdshop_db():
    """ """
    db = MySQLdb.connect(host='172.16.5.33', user='dbreader', passwd='wearefashions',
                         db='brd_shop', port=3504)
    return db


#----------------------------------------------------------------------
def main(args):
    """
    emacs myorgfile.org --batch -f org-export-as-html --kill
    http://stackoverflow.com/questions/22072773/batch-export-of-org-mode-files-from-the-command-line
    """

    data = flat2list(args.input)
    data_reshape = []
    for line in data:
        data_reshape += line

    if args.type == 'twitter':
        sqlt = "select twitter_id, goods_id, goods_img from brd_shop_goods_info_new where twitter_id in (%s)"
    elif args.type == 'good':
        sqlt = "select twitter_id, goods_id, goods_img from brd_shop_goods_info_new where goods_id in (%s)"

    tid2url = {}
    gid2tid = {}
    db = get_brdshop_db()
    cursor = db.cursor()
    for chk in chunks(data_reshape, 1000):
        sql = sqlt % ",".join(chk)
        print sql
        cursor.execute(sql)
        res = cursor.fetech(all)
        for r in res:
            tid2url[r[0]] = r[2]
            gid2url[r[1]] = r[0]
    

    output_file = args.output
    if output_file.endswith('.html') or output_file.endswith('.htm'):
        temp_file = output_file.rsplit('.', 1)[0] + '.org'
    else:
        temp_file = output_file + '.org'
        
    fh = open(temp_file, 'w')
    print >> fh, "#+ATTR_HTML: target=\"_blank\" "
    for line in  data:
        if args.type =='twitter':
            tids = line
            gids = None
        else:
            gids = line
            tids = map(lambda x: gid2tid.get(x), line)

        if gids is not None:
            print >> fh, "| %s |" % "|".join(map(str, gids))
        print >> fh, "| %s |" % "|".join(map(str, tids))
        info_tuple = map(lambda x: (x, tid2url.get(x)), tids)
        print >> fh, "| %s |" % " | " .join(lambda x: "[[http://www.meilishuo.com/share/item/%s][http://imgst.meilishuo.net/%s]]" % (x[0], x[1]) , info_tuple)
    fh.close()
    cmd = " emacs --kill --batch %s -f org-export-as-html " % temp_file
    os.system(cmd)




if __name__=='__main__':
    args = parse_args()
    main(args)
