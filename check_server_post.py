#! /usr/bin/env python
#coding:utf8

"""
    测试server的post请求
"""
import sys
import re
import requests
import json
import urllib
import time

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


NF = 5

def main2(fn=None):
    if fn is None:
        fn = sys.argv[1]

    tids = []
    all = file(fn).readlines()
    for lines in chunks(all, NF):
        record = []
        for line in lines:
            fields = line.strip().split("\t")
            tid, gid = fields[0], fields[1]
            tids.append(tid)
            tid = int(tid)
            gid = int(gid)
            record.append( {
                'twitter_id':tid,
                'goods_id':gid,
                'wait_source':2
                })
        ret = load_verify(record)
        print "[%s] [insert] [%s] [%s] [%s]" % (time.time(), ret.status_code, ret.text, ",".join(tids), )

def main(fn=None):

    data = [  {'twitter_id': 13345, 'goods_id':1345, 'shop_id': 123, 'category':'shoes', 'img_url':'/pic/afsfdsf.jpg'} ,
              {'twitter_id': 13377, 'goods_id':1347,                 'category':'clothes', 'img_url':'/pic/dsfjsldflads.jpg'}
              ]
    post_data = {'data': json.dumps(data),
                 'method':'group'}

    r = requests.post("http://localhost:8773/result", data=post_data)
    print r


if __name__ == "__main__":
    main()
