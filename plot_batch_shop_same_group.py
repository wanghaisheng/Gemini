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

GROUP_PICK_LIST = ["clothes_group_pickle", "shoes_group_pickle", "bag_group_pickle", "acc_group_pickle", "other_group_pickle"]

#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='分析恶意上新商家, 批量输出店铺文件')
    parser.add_argument("group_dir", metavar='GROUP', help="the dir of group id pickle dump files.")
    parser.add_argument("twitter_file", metavar='TWITTER', help="the twitter info file.")
    parser.add_argument("shop_list", metavar='SHOP', help="the shop list.")
    parser.add_argument("output", metavar='OUTPUT', help="the output html file.")
    parser.add_argument("--maxshow", type=int, default=5, help="同店同款，最大图片展现数量. default 5 ")
    parser.add_argument("--maxshow2", type=int, default=5, help="他店同款，最大图片展现数量. default 5 ")
    parser.add_argument("--max_col", type=int, default=20, help="全部同款，最大整体展现数量. default 20 ")    
    parser.add_argument("--maxgroup", type=int, default=30, help="最大图片展现group数量. default 30 ")
    parser.add_argument("--maxline", type=int, default=100, help="最大展现group数量. default 100 ")
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

def output_group_image_table(twitter_info, positions, max_show, max_col, flag="店内同款"):
    twitter_strings = map(lambda x: "*%s*" % twitter_info[x][0], positions[:max_col])
    positions1 = positions[:max_show]
    positions2 = positions[max_show:max_col]
    image_urls = map(lambda x: "[[http://www.meilishuo.com/share/item/%s][http://imgst.meilishuo.net/%s]]" % (twitter_info[x][0], twitter_info[x][-1]), positions1)
    text_urls = map(lambda x: "[[http://www.meilishuo.com/share/item/%s][image]]" % (twitter_info[x][0]), positions2)
    x = len(positions)
    sz =  "|*%s共%s个*| %s |" % (flag, x, " | ".join(twitter_strings)) +"\n"
    sz += "| - | %s |" % " | ".join(image_urls+text_urls)
    return sz

def output_group_text_table(twitter_info, positions, max_col, flag="店内同款"):
    x = len(positions)
    positions1 = positions[:max_col]
    text_urls = map(lambda x: "[[http://www.meilishuo.com/share/item/%(tid)s][%(tid)s]]" % {"tid":twitter_info[x][0]}, positions)

    sz =  "|*%s共%s个*| %s |" % (flag, x, " | ".join(text_urls)) +"\n"
    return sz


#----------------------------------------------------------------------
def main(args):
    """
    emacs myorgfile.org --batch -f org-export-as-html --kill
    http://stackoverflow.com/questions/22072773/batch-export-of-org-mode-files-from-the-command-line
    """

    group = Group()
    for f in GROUP_PICK_LIST:
        group.append(args.group_dir + '/' + f, check=True)
        
    twitter_info = TwitterInfo()
    twitter_info.load(args.twitter_file)

    allshop2pos = map_shop_to_twitter_pos(twitter_info.get_data())

    shop2pos = {}
    shopids = map(lambda x: x.strip(), open(args.shop_list).readlines())
    for s in shopids:
        shop2pos[s] = allshop2pos.get(s, set())

    result_map = {}
    for shop in shop2pos:
        local_group = {}
        ret = {'total': len(shop2pos[shop]), 'groups': local_group}
        for pos in shop2pos[shop]:
            group_pos = group.get_group(pos)
            if group_pos is None:
                continue
            if group_pos not in local_group:
                local_group[group_pos] = set()
            local_group[group_pos].add(pos)
        result_map[shop] = ret

    output_file = args.output
    if not os.path.exists(output_file):
        os.makedirs(output_file)

    for shopid, info in result_map.iteritems():
        temp_file = output_file + '/' + shopid + '.org'
        with open(temp_file, "w") as fh:
            print >> fh, "#+ATTR_HTML: target=\"_blank\" "
            print >> fh, "* [[http://www.meilishuo.com/shop/%s][shopid=%s]] 总商品量%s" % (shopid, shopid, info['total'])
            print >> fh

            local_group = info['groups']
            
            #ordered_keys = random.shuffle(local_group.keys())
            ordered_keys = local_group.keys()
            
            n = len(ordered_keys)
            list1 = range(n)[:args.maxgroup]
            list2 = range(n)[args.maxgroup:args.maxline]
            for i in list1:
                group_pos = ordered_keys[i]
                positions = local_group[group_pos]
                sz = output_group_image_table(twitter_info, list(positions), max_show=args.maxshow, max_col=args.max_col, flag="店内同款")
                print >> fh, sz
                total_group_members = group.get_member(group_pos)
                positions2 = total_group_members.difference(positions)
                if positions2:
                    sz = output_group_image_table(twitter_info, list(positions2), max_show=args.maxshow2, max_col=args.max_col, flag="跨店同款")
                    print >> fh, sz

                print >> fh

            if list2:
                print >>fh, "* 避免拖垮浏览器，以下图片省略"
            
            for i in list2:
                group_pos = ordered_keys[i]
                positions = local_group[group_pos]
                sz = output_group_text_table(twitter_info, list(positions), max_col=args.max_col, flag="店内同款")
                print >> fh, sz
                total_group_members = group.get_member(group_pos)
                positions2 = total_group_members.difference(positions)
                sz = output_group_text_table(twitter_info, list(positions2), max_col=args.max_col, flag="跨店同款")
                print >> fh, sz

                print >> fh
        cmd = " emacs --kill --batch %s -f org-export-as-html " % temp_file
        os.system(cmd)
        print "shop %s is done" % shopid


if __name__=='__main__':
    args = parse_args()
    main(args)
