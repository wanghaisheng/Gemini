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
    parser.add_argument("--num", type=int, default=2, help="the minimum group size. default 2.")
    parser.add_argument("--feature", help="feature file for drawing distance matrix.")
    parser.add_argument("--shop_ids", help="少量shop的id信息，只展现这些商家的信息. 1,3,5")
    parser.add_argument("--max", type=int, default=10, help="同一商家，同一款式，最大展现数量，避免拖垮浏览器")
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

    for shop in result_map:
        info = result_map[shop]
        print "%s\t%s\t%s" % (float(info['repeat'])/info['total'], info['repeat'], info['total'])

    sys.exit(1)
    
    large_groups = []
    for pos in ids:
        members = group.get_member(pos)
        n = len(members)
        dist_group_size[n] += 1
        if len(members) < args.num:
            continue
        large_groups.append(members)

    bins = dist_group_size.keys()
    for k in bins:
        if count_total:
            print "size %s = %s\t\t[%s]" % (k, dist_group_size[k], dist_group_size[k] / float(count_total))
        else:
            print "size %s = %s" % (k, dist_group_size[k])

    if args.url is not None:
        output_file = args.output
        if output_file.endswith('.html') or output_file.endswith('.htm'):
            temp_file = output_file.rsplit('.', 1)[0] + '.org'
        else:
            temp_file = output_file + '.org'

        fh = open(temp_file, 'w')
        for s in large_groups:
            if random.random() > args.ratio:
                continue
            s = list(s)
            positions = map(lambda x: "%s([[http://www.meilishuo.com/share/item/%s][tid=%s]]) " % (x, group.get_twitter_id(x),group.get_twitter_id(x)), s)
            urls = map(lambda x: "[[%s%s]]" % (IMAGE_URL,group.get_url(x)), s)
            print >> fh, "| %s |" % " | ".join(positions)
            if args.feature is not None:
                n = len(s)
                matrix = group.get_distance_matrix(s)
                for i in range(n):
                    v = matrix[i]
                    print >> fh, "| %s |" % " | ".join(map(str, v))

            print >> fh, "| %s |" % " | ".join(urls)
            print >> fh
        fh.close()
        # wiki to html
        cmd = " emacs --kill --batch %s -f org-export-as-html " % temp_file
        os.system(cmd)




if __name__=='__main__':
    args = parse_args()
    main(args)
