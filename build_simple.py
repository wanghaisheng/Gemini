#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 对简单数据进行build，构建flann索引文件和组id文件
 History:
     1. 2014/7/1 16:34 : build_simple.py is created.
"""


import sys
import os
import datetime
from time import time
from argparse import ArgumentParser
import logging
import cPickle
import json

from contexttimer import Timer
import numpy as np
from pyflann import FLANN
import scipy.spatial


from utils import setup_logger, try_load_npy, flat2list


LOG_LEVEL = logging.DEBUG
LOG_FILE = 'build_simple.log'
# 不同group的聚类算法
CLUSTERING_ALGORITHMS = ['1nn', '1nn_order', '1nn_2step', 'xnn_simple']

logger = None


def build_flann_index(feature_file, index_file, para_file, force=False):
    """
    尝试加载索引文件，如果不存在，就根据feature文件进行build。
    """
    with Timer() as t:
        feature_data = try_load_npy(feature_file)
    logger.info("[%s] load feature data %s " % (t.elapsed, feature_file))

    
    flann = FLANN()
    if os.path.exists(index_file) and not force:
        with Timer() as t:
            flann.load_index(index_file, feature_data)
            params = json.load(open(para_file))
        logger.info("[%s] load index file %s and para file %s" % (t.elapsed, index_file, para_file) )
    else:
        with Timer() as t:
            params = flann.build_index(feature_data, algorithm='autotuned', target_precision=0.9, build_weight=0.01,
                                       memory_weight=0.01, sample_fraction=0.1)
            json.dump(params, open(para_file, 'w'))
            flann.save_index(index_file)
        logger.info("[%s] build index by flann. returned paras are : %s " % (t.elapsed, params))


    return flann, params, feature_data


def build_group(flann, feature_data, params, threshold, algorithm='1nn'):
    if algorithm == '1nn':
        return build_group_1nn_simple(flann, feature_data, params, threshold)
    elif algorithm == '1nn_order':
        return build_group_1nn_order(flann, feature_data, params, threshold)
    elif algorithm == '1nn_2step':
        return build_group_1nn_2step(flann, feature_data, params, threshold)
    elif algorithm == 'xnn_simple':
        return build_group_xnn_simple(flann, feature_data, params, threshold)

def _is_insert_current_group(group, point, feature_data, threshold):
    """
    如果将point插入当前group，满足所有点阈值小于threshold，返回Yes， 否则No
    """
    vec1 = feature_data[[point]]
    vec2 = feature_data[list(group)]
    mat = scipy.spatial.distance.cdist(vec1, vec2, 'sqeuclidean')
    if mat.max() < threshold:
        return True
    else:
        return False


def build_group_xnn_simple(flann, feature_data, params, threshold):
    """ 考察n个近邻。不同点按照最近邻的距离排序，依次处理。
    1. 如果最近邻属于一个分组, 且与分组中所有元素距离小于阈值，则加入该组。
    2. 依次考虑第n个近邻。

    TODO：
    1. 算法可能有风险： 考虑当前元素的第n个次近邻时，其距离可能接近阈值。把后续距离更近的pair（潜在同款组打破）。 把所有的近邻关系全局排序，可能是最优的。
    2. 另外一个思路，是考虑n个近邻中，隶属元素最多的同款组。

    """

    with Timer() as t:
        neighbors, distances = flann.nn_index(feature_data, num_neighbors=10, **params)
    logger.info("[%s] query all %s points with nearest neighbors " % (t.elapsed, len(feature_data)))

    order_positions, order_neighbors, order_distances = _find_and_sort_neighbor(neighbors, distances, threshold)

    n = len(order_positions)
    logger.info(" %s points with neighbors in the threshold are sorted" % n)

    logger.info("start to cluster %s points into groups" % n)
    # twitter用feature data中的pos表示
    pos2groupid = {}   # {10:5, 20:5}
    groupid2pos = {}   # {5:set(5, 10, 20)}
    for i in xrange(n):
        if i % 10000 == 0:
            logger.debug("  %s points has been processed " % i)
        point = order_positions[i]
        
        # if point in (4232,30124,35934,42950,6071):
        #    import pdb
        #    pdb.set_trace()

        # point已经被其他最近邻纳入某个同款组。 风险：当前最近邻不能发挥作用。
        if point in pos2groupid:
            continue
        members = order_neighbors[i]
        m = len(members)
        checked_group = []
        is_grouped = False
        j = 0
        while not is_grouped and j < m :
            nn = members[j]
            j += 1
            groupid = pos2groupid.get(nn)
            if groupid is not None:
                if groupid not in checked_group:
                    cur_group = groupid2pos[groupid]
                    if _is_insert_current_group(cur_group, point, feature_data, threshold ):
                        cur_group.add(point)
                        pos2groupid[point] = groupid
                        is_grouped = True
                    checked_group.append(groupid)
                else:
                    pass # 当前近邻所属同款组，在其他近邻所属同款组时已经检查过了。
            else:
                groupid2pos[point] = set((point, nn))
                pos2groupid[nn] = point
                pos2groupid[point] = point
                is_grouped = True

    logger.info("clustering points into group is done")

    return pos2groupid, groupid2pos


def _find_and_sort_neighbor(neighbors, distances, threshold):
    """
     从nn_index产出的数据中，产生近邻， 按照最近邻距离排序。
    """
    n = len(neighbors)
    threshold_pair = []
    for i in xrange(n):
        members = neighbors[i]
        gaps = distances[i]
        # 阈值之内才算neighbour， 返回数据按照距离排序
        m = len(members)
        j = 0
        while j < m:
            if gaps[j] >= threshold:
                break
            j += 1
        members = members[:j]
        gaps = gaps[:j]

        # 自身不算最近邻
        idx = filter(lambda x: members[x]!=i, range(j))
        members = map(lambda x: members[x], idx)
        gaps = map(lambda x: gaps[x], idx)

        if members:
            threshold_pair.append((i, members, gaps))

    # 按照最近邻的距离排序
    threshold_pair.sort(key=lambda x:x[2][0])
    order_positions = map(lambda x: x[0], threshold_pair)
    order_neighbors = map(lambda x: x[1], threshold_pair)
    order_distances = map(lambda x: x[2], threshold_pair)

    return order_positions, order_neighbors, order_distances

def _split_group(points, feature_data, threshold):
    """
    如果同款组内元素最大距离超过阈值，则对同款组进行分裂。
    以最大距离两个元素为种子： 将最近邻依次加入。

    递归处理剩余元素。
    """
    points = list(points)
    vector = feature_data[points]
    cdist = scipy.spatial.distance.cdist(vector, vector, 'sqeuclidean')
    max_value = cdist.max()
    if max_value < threshold:
        return [points]

    # 最大元素坐标为[i, j]
    idx = cdist.argmax()
    dim1, dim2 = cdist.shape  # dim1 == dim2
    i = idx/dim2
    j = idx % dim2
    idx_list = range(dim1)

    # i和j各自近邻，按照距离排序，不包括自身。
    i_nns = sorted(idx_list[:i]+idx_list[i+1:], key=lambda x: cdist[i, x])
    j_nns = sorted(idx_list[:j]+idx_list[j+1:], key=lambda x: cdist[j, x])

    i_group = [i]
    j_group = [j]
    while i_nns or j_nns:
        while i_nns and (i_nns[0] in i_group or i_nns[0] in j_group):
            i_nns.pop(0)
        while j_nns and (j_nns[0] in i_group or j_nns[0] in j_group):
            j_nns.pop(0)
        if i_nns and j_nns:
            if cdist[i, i_nns[0]] <= cdist[j, j_nns[0]]:
                i_group.append(i_nns.pop(0))
            else:
                j_group.append(j_nns.pop(0))
        elif i_nns:
            i_group.append(i_nns.pop(0))
        elif j_nns:
            j_group.append(j_nns.pop(0))

    i_point_group = map(lambda x: points[x], i_group)
    j_point_group = map(lambda x: points[x], j_group)

    return _split_group(i_point_group, feature_data, threshold) + _split_group(j_point_group, feature_data, threshold)


def build_group_1nn_2step(flann, feature_data, params, threshold):
    """
    两步处理：
    1. 按照阈值，尽量聚类。
    2. 数量超过2同款组，对类别进行分裂。
    """

    with Timer() as t:
        neighbors, distances = flann.nn_index(feature_data, num_neighbors=2, **params)
    logger.info("[%s] query all %s points with nearest neighbors " % (t.elapsed, len(feature_data)))

    order_positions, order_neighbors, order_distances = _find_and_sort_neighbor(neighbors, distances, threshold)
    n = len(order_positions)
    logger.info(" %s points with neighbors in the threshold are sorted" % n)
    
    logger.info("start to cluster %s points into groups" % n)
    # 第一步： 尽量聚类
    # twitter用feature data中的pos表示
    pos2groupid = {}   # {10:5, 20:5}
    groupid2pos = {}   # {5:set(5, 10, 20)}
    for i in xrange(n):
        if i % 10000 == 0:
            logger.debug("  %s points has been processed " % i)
        point = order_positions[i]
        nn = order_neighbors[i][0]
        groupid = pos2groupid.get(nn)
        if groupid is not None:
            pos2groupid[point] = groupid
            groupid2pos[groupid].add(point)
        else:
            groupid2pos[point] = set((point, nn))
            pos2groupid[nn] = point
            pos2groupid[point] = point

    # 第二部： 按照阈值split
    for k in groupid2pos.keys():
        v = list(groupid2pos[k])
        if len(v) > 2:
            del groupid2pos[k]
            vs = _split_group(v, feature_data, threshold)
            for v in vs:
                idx = min(v)
                groupid2pos[idx] = set(v)
                for k in v:
                    pos2groupid[k] = idx
    logger.info("clustering points into group is done")

    return pos2groupid, groupid2pos

def build_group_1nn_order(flann, feature_data, params, threshold):
    """
    最简单的近邻聚类方法： 只要tid的最近邻N（tid）在某同款组，则加入该组。
    按照最近邻绝对距离的顺序操作。
    """

    with Timer() as t:
        neighbors, distances = flann.nn_index(feature_data, num_neighbors=2, **params)
    logger.info("[%s] query all %s points with nearest neighbors " % (t.elapsed, len(feature_data)))


    order_positions, order_neighbors, order_distances = _find_and_sort_neighbor(neighbors, distances, threshold)

    n = len(order_positions)
    logger.info(" % points with neighbors in the threshold are sorted" % n)
    logger.info("start to cluster %s points into groups" % n)

    # twitter用feature data中的pos表示
    pos2groupid = {}   # {10:5, 20:5}
    groupid2pos = {}   # {5:set(5, 10, 20)}
    for i in xrange(n):
        if i % 10000 == 0:
            logger.debug("  %s points has been processed " % i)
        point = order_positions[i]
        nn = order_neighbors[i][0]
        groupid = pos2groupid.get(nn)
        if groupid is not None:
            pos2groupid[point] = groupid
            groupid2pos[groupid].add(point)
        else:
            groupid2pos[point] = set((point, nn))
            pos2groupid[nn] = point
            pos2groupid[point] = point

    logger.info("clustering points into group is done")

    return pos2groupid, groupid2pos

def build_group_1nn_simple(flann, feature_data, params, threshold):
    """ 最简单的近邻聚类方法： 只要tid的最近邻N（tid）在某同款组，则加入该组。"""

    with Timer() as t:
        neighbors, distances = flann.nn_index(feature_data, num_neighbors=2, **params)
    logger.info("[%s] query all points with nearest neighbors" % t.elapsed)


    # twitter用feature data中的pos表示
    pos2groupid = {}   # {10:5, 20:5}
    groupid2pos = {}   # {5:set(5, 10, 20)}
    n = len(neighbors)
    logger.info("start to cluster %s points into groups" % n)
    for i in xrange(n):
        if i % 1000 == 0:
            logger.debug("  %s points has been processed " % i)

        # 阈值之内才算neighbour， 返回数据按照距离排序
        current_neighbor = neighbors[i][0]
        current_distance = distances[i][0]
        if current_neighbor == i: # 自身不算最近邻, 找次近邻
            current_neighbor = neighbors[i][1]
            current_distance = distances[i][1]
            
        if current_distance >= threshold:
            continue

        groupid = pos2groupid.get(current_neighbor)
        if groupid is not None:
            pos2groupid[i] = groupid
            groupid2pos[groupid].add(i)
        else:
            groupid2pos[i] = set((i, current_neighbor))
            pos2groupid[i] = i
            pos2groupid[current_neighbor] = i

    logger.info("clustering points into group is done")

    return pos2groupid, groupid2pos


def main(args):
    """
    """
    global logger
    logger = setup_logger('SPL', args.output+'/'+LOG_FILE, LOG_LEVEL)

    index_file = args.output + '/feature_index'
    index_para_file = args.output + '/feature_index_para'
    group_file = args.output + '/group_pickle'

    flann, params, feature_data = build_flann_index(args.feature, index_file, index_para_file, force=args.force)

        
    if args.force or not os.path.exists(group_file):
        pos2groupid, groupid2pos = build_group(flann, feature_data, params, args.distance, args.algorithm)
        cPickle.dump((pos2groupid, groupid2pos), open(group_file,'w'), protocol=2)


def parse_args():
    """
    """
    parser = ArgumentParser(description='输入特征文件和tid映射关系，构建flann索引和组文件.')
    parser.add_argument("--feature", metavar='FEATURE', help="feature文件路径.")
    parser.add_argument("--tid", metavar='TID', help="tid文件路径.")
    parser.add_argument("--distance", metavar='DISTANCE', type=float, help="同款的距离阈值， 欧式距离的平方.")
    parser.add_argument("--output", metavar='OUTPUT', help="输出文件夹.")
    parser.add_argument("--force", action='store_true', help="删除已有的索引文件.")
    parser.add_argument("--algorithm", default='1nn', help="同款聚类算法.　default= '1nn'")
    args = parser.parse_args()
    if args.feature is None or not os.path.exists(args.feature):
        print "feature file do not exist"
        parser.print_help()
        sys.exit(1)
    if args.tid is None or not os.path.exists(args.tid):
        print "feature file do not exist"
        parser.print_help()
        sys.exit(1)        
    if args.distance is None:
        print "distance is not specified "
        parser.print_help()
        sys.exit(1)
    if args.output is None or not os.path.exists(args.output):
        os.mkdir(args.output)
    if args.algorithm not in CLUSTERING_ALGORITHMS:
        print "未知聚类算法 %s。 合法算法包括%s" % (args.algorithm, CLUSTERING_ALGORITHM)
        parser.print_help()
        sys.exit(1)

    return args


if __name__=='__main__':
    args = parse_args()
    main(args)
