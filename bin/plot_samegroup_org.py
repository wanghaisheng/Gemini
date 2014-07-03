#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose: 
     1. 将group数据转化为org文件,分析同款组的识别质量
 History:
     1. 2014/7/2 16:49 : plot_samegroup_org.py is created.
"""



import sys
import os
from argparse import ArgumentParser
import cPickle
from collections import Counter
import time

from group import Group


#----------------------------------------------------------------------
def parse_args():
    """
    
    """
    parser = ArgumentParser(description='将同款组文件转化为org（html）格式，分析同款组质量.')
    parser.add_argument("input", metavar='INPUT', help="the group id pickle dump file.")
    parser.add_argument("output", metavar='OUTPUT', help="the output html file.")
    parser.add_argument("--num", type=int, default=2, help="the minimum group size. default 2.")
    parser.add_argument("--url", help="the file for twitter id and urls.")
    args = parser.parse_args()
    return args


#----------------------------------------------------------------------
def main(args):
    """
    emacs myorgfile.org --batch -f org-export-as-html --kill
    http://stackoverflow.com/questions/22072773/batch-export-of-org-mode-files-from-the-command-line
    """
    count_total = 0   # 图片总量
    dist_group_size = Counter()   # 同款组size的分布

    group = Group()
    group.load(args.input)
    if args.url is not None:
        group.load_tid(args.url)
        count_total = group.tid_num()

    ids = group.get_group_list()

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
            urls = map(lambda x: "[[%s]]" % group.get_url(x), s)
            print >> fh, "| %s |" % " | ".join(urls)
            print >> fh
        fh.close()
        # wiki to html
        cmd = " emacs --kill --batch %s -f org-export-as-html " % temp_file
        os.system(cmd)




if __name__=='__main__':
    args = parse_args()
    main(args)
