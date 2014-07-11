#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose: 
     1. 考察每个店铺内，彼此同款的数量比例。挑选恶意重复上新商家
 History:
     1. 2014/7/10 plot_shop_samegroup.py is created.
"""



import sys
import os
from argparse import ArgumentParser
import cPickle
from collections import Counter
import time
import random

from samelib.group import Group
from samelib.twitter import TwitterInfo
from samelib.utils import try_load_npy, distance_matrix

IMAGE_URL = 'http://imgst.meilishuo.net/'

#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='将考察商店内同款数量，分析恶意上新商家.')
    parser.add_argument("group_file", metavar='GROUP', help="the group id pickle dump file.")
    parser.add_argument("twitter_file", metavar='TWITTER', help="the twitter info file.")
    parser.add_argument("output", metavar='OUTPUT', help="the output html file.")
    parser.add_argument("--num", type=int, default=20, help="the minimum 商品数量. default 20.")
    parser.add_argument("--ratio", type=float, default=0.2, help="最低店内同款率. default 0.2.")    
    parser.add_argument("--feature", help="feature file for drawing distance matrix.")
    parser.add_argument("--shop_ids", help="少量shop的id信息，只展现这些商家的信息. 1,3,5. ")
    parser.add_argument("--maxshow", type=int, default=5, help="同店同款，最大展现数量. default 5 ")
    parser.add_argument("--maxgroup", type=int, default=10, help="同店同款，最大group展现数量. default 10 ")
    args = parser.parse_args()
    return args

def map_shop_to_twitter_pos(twitter_data):
    res = {}
    n = len(twitter_data)
    for i in xrange(n):
        tid, gid, shop_id, category, url = twitter_data[i]
        if shop_id not in res:
            res[shop_id] = set()
        res[shop_id].add(i)
    return res

#----------------------------------------------------------------------
def main(args):
    """
    emacs myorgfile.org --batch -f org-export-as-html --kill
    http://stackoverflow.com/questions/22072773/batch-export-of-org-mode-files-from-the-command-line
    """
    count_total = 0   # 图片总量
    dist_group_size = Counter()   # 同款组size的分布

    group = Group()
    group.load(args.group_file)
    twitter_info = TwitterInfo()
    twitter_info.load(args.twitter_file)

    shop2pos = map_shop_to_twitter_pos(twitter_info.get_data())

    count_total = group.tid_num()
    
    if args.feature is not None:
        feature_data = try_load_npy(args.feature)
        group.set_feature(feature_data)

    shop2pos2 = {}
    if args.shop_ids is not None:
        shopids = map(int, args.ids.split(','))
        for s in shopids:
            if s in shop2pos:
                shop2pos2[s] = shop2pos[s]
        shop2pos = shop2pos2

    result_map = {}
    for shop in shop2pos:
        local_group = {}
        ret = {'total': len(shop2pos[shop]), 'repeat': 0, 'groups': local_group}
        for pos in shop2pos[shop]:
            group_pos = group.get_group(pos)
            if group_pos is None:
                continue
            if group_pos not in local_group:
                local_group[group_pos] = []
            local_group[group_pos].append(pos)
        for group_pos in local_group:
            ret['repeat'] += len(local_group[group_pos]) -1 # 定义repeat为同款组数量减去1

        result_map[shop] = ret

    output_file = args.output
    if output_file.endswith('.html') or output_file.endswith('.htm'):
        temp_file = output_file.rsplit('.', 1)[0] + '.org'
    else:
        temp_file = output_file + '.org'

    fh = open(temp_file, 'w')

    print >> fh, "#+ATTR_HTML: target=\"_blank\" "

    keys_sorted = sorted(result_map.keys(), key=lambda x: result_map[x]['repeat'], reverse=True)
    for shop in keys_sorted:
        info = result_map[shop]
        ratio = float(info['repeat'])/info['total']
        if info['total'] < args.num or ratio < args.ratio:
            continue
        

        print >> fh, "* [[http://www.meilishuo.com/shop/%s][shopid=%s]] 同款率%s 同款量%s 总量%s" % (shop, shop, ratio, info['repeat'], info['total'])
        print >> fh
        
        local_group = info['groups']
        k = 0 
        for group_pos in local_group:
            if k > args.maxgroup:
                break
            k += 1
            positions = local_group[group_pos]
            n = len(positions)
            if n > args.maxshow:
                positions = positions[:args.maxshow]
            if n == 1:
                continue
            twitter_strings = map(lambda x: "%s([[http://www.meilishuo.com/share/item/%s][tid=%s]]) " % (x, twitter_info[x][0], twitter_info[x][0]), positions)
            urls = map(lambda x: "[[%s%s]]" % (IMAGE_URL, twitter_info[x][-1]), positions)
            # print >> fh, "| %s |" % " | ".join(map(str,positions))
            print >> fh, "|*共%s个*| *%s* |" % (n, "* | *".join(twitter_strings))
            print >> fh, "| - |%s |" % " | ".join(urls)
            print >> fh
            
    fh.close()
    cmd = " emacs --kill --batch %s -f org-export-as-html " % temp_file
    os.system(cmd)




if __name__=='__main__':
    args = parse_args()
    main(args)
