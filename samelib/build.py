#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 建立同款索引相关的工具函数
 History:
     1. 2014/7/9 18:10 : build.py is created.
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
import math

from contexttimer import Timer
import numpy as np
import scipy
from pyflann import FLANN
import leveldb

from samelib.utils import setup_logger, catalog_id_to_name
from samelib.config import Config
from samelib.twitter import TwitterInfo
from samelib.utils import try_load_npy, get_feature_db
from feature.feature import download_and_compute_feature
from samelib.group import Group

conf = Config('./conf/build.yaml')

LOG_LEVEL = logging.DEBUG

WORK_BASE_PATH = conf['WORK_BASE_PATH']
DATA_PATH = WORK_BASE_PATH + '/data/index_day'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']
IS_DUMP_TXT_FEATURE = conf['IS_DUMP_TXT_FEATURE']

FLANN_BUILD_INDEX_PARA = conf['FLANN_BUILD_INDEX_PARA']

GOODS_CATEGORY = conf['GOODS_CATEGORY']

ITER_RANGE = 1000  # 图片加载计算特征，每100个输出一次状态。
ITER_RANGE2 = 1000  # 同款组聚类，每500个完成一次输出。
IMAGE_SERVER = 'http://imgst.meilishuo.net'
NUM_NEIGHBORS = 10  #

# logger = setup_logger('BLD')
# db = leveldb.LevelDB(FEATURE_DB_PATH)



def _pick_flann_para(n, PARA):

    for para_set in PARA:
        if n < para_set['num']:
            return para_set
    logger = setup_logger('BLD')
    logger.fatal("data set of size %s is larger than 100w" % n)


def build_flann_index(index_file, para_file, feature_data, force=False):
    """
    尝试加载索引文件，如果不存在，就根据feature文件进行build。
    """
    logger = setup_logger('BLD')

    flann = FLANN()
    if os.path.exists(index_file) and not force:
        with Timer() as t:
            flann.load_index(index_file, feature_data)
            params = json.load(open(para_file))
        logger.info("[%s] load index file %s and para file %s" % (t.elapsed, index_file, para_file))
    else:
        n = len(feature_data)
        para_input = _pick_flann_para(n, FLANN_BUILD_INDEX_PARA)
        with Timer() as t:
            params = flann.build_index(feature_data, **para_input)
            json.dump(params, open(para_file, 'w'))
            flann.save_index(index_file)
        logger.info("[%s] build index file %s by flann with paras %s " % (t.elapsed, index_file, params))

    return flann, params


def build_group_index(group_pickle_file, group_dump_tid2group, group_dump_group2tid, twitter_info, feature_data, flann,
                      params,
                      threshold, algorithm='xnn_simple', force=False):

    logger = setup_logger('BLD')
    
    group = Group()
    if not force and (os.path.exists(group_pickle_file)):
        with Timer() as t:
            group.load(group_pickle_file)
            if not os.path.exists(group_dump_group2tid) or not os.path.exists(group_dump_tid2group):
                group.to_txt(group_dump_tid2group, group_dump_group2tid, twitter_info=twitter_info)
        logger.info("[%s] directly load group index file %s" % (t.elapsed, group_pickle_file))
        return group

    if algorithm == '1nn':
        pos2group, group2pos = build_group_1nn_simple(flann, feature_data, params, threshold)
    elif algorithm == '1nn_order':
        pos2group, group2pos = build_group_1nn_order(flann, feature_data, params, threshold)
    elif algorithm == '1nn_2step':
        pos2group, group2pos = build_group_1nn_2step(flann, feature_data, params, threshold)
    elif algorithm == 'xnn_simple':
        pos2group, group2pos = build_group_xnn_simple(twitter_info, feature_data, flann, params, threshold)
        group.set_info(pos2group, group2pos)

    with Timer() as t:
        group.save(group_pickle_file)
        group.to_txt(group_dump_tid2group, group_dump_group2tid, twitter_info=twitter_info)
    logger.info("[%s] group index %s and txt version (%s and %s) are saved" % (t.elapsed,
                                                                               group_pickle_file,
                                                                               group_dump_tid2group,
                                                                               group_dump_group2tid))

    return group


def build_group_xnn_simple(twitter_info, feature_data, flann, params, threshold):
    """ 考察n个近邻。不同点按照最近邻的距离排序，依次处理。
    1. 如果最近邻属于一个分组, 且与分组中所有元素距离小于阈值，则加入该组。
    2. 依次考虑第n个近邻。

    TODO：
    1. 算法可能有风险： 考虑当前元素的第n个次近邻时，其距离可能接近阈值。把后续距离更近的pair（潜在同款组打破）。 把所有的近邻关系全局排序，可能是最优的。
    2. 另外一个思路，是考虑n个近邻中，隶属元素最多的同款组。

    """
    logger = setup_logger('BLD')

    # twitter用feature data中的pos表示
    pos2groupid = {}  # {10:5, 20:5}
    groupid2pos = {}  # {5:set(5, 10, 20)}

    # 有很多twitter的图片在物理上是一张图，所以将所有图片完全一样的twi，作为聚类的种子。
    # TODO： 聚类的顺序是否需要和order_positions保持一致？
    with Timer() as t:
        url2pos = {}
        twitter_data = twitter_info.get_data()
        n_t = len(twitter_data)
        n_grouped = 0
        for idx in xrange(n_t):
            url = twitter_data[idx][-1]
            if url not in url2pos:
                url2pos[url] = []
            url2pos[url].append(idx)
        for url, pos_set in url2pos.iteritems():
            k = len(pos_set)
            if k > 1:
                n_grouped += k
                group_id = pos_set[0]
                groupid2pos[group_id] = set(pos_set)
                for pos in pos_set:
                    pos2groupid[pos] = group_id
    logger.info("[%s] %s out of %s points has been grouped by url" % (t.elapsed, n_grouped, n_t))

    with Timer() as t:
        neighbors, distances = flann.nn_index(feature_data, num_neighbors=NUM_NEIGHBORS, **params)
    logger.info("[%s] query all %s points with %s nearest neighbors " % (t.elapsed, len(feature_data), NUM_NEIGHBORS))

    with Timer() as t:
        order_positions, order_neighbors, order_distances = _find_and_sort_neighbor(neighbors, distances, threshold)
        n = len(order_positions)
    logger.info("[%s] %s points with neighbors in the threshold are sorted" % (t.elapsed, n))

    now = time.time()
    s_t, e_t, s_i, e_i = now, now, now, now
    for i in xrange(n):
        if i % ITER_RANGE2 == 0:
            e_i = time.time()
            logger.debug(" [%s] %s points has been clustered" % (e_i - s_i, i))
            s_i = e_i

        point = order_positions[i]

        # if point in (4232,30124,35934,42950,6071):
        # import pdb
        # pdb.set_trace()

        # point已经被其他最近邻纳入某个同款组。 风险：当前最近邻不能发挥作用。
        if point in pos2groupid:
            continue
        members = order_neighbors[i]
        m = len(members)
        checked_group = []
        is_grouped = False
        j = 0
        while not is_grouped and j < m:
            nn = members[j]
            j += 1
            groupid = pos2groupid.get(nn)
            if groupid is not None:
                if groupid not in checked_group:
                    cur_group = groupid2pos[groupid]
                    if _is_insert_current_group(cur_group, point, feature_data, threshold):
                        cur_group.add(point)
                        pos2groupid[point] = groupid
                        is_grouped = True
                    checked_group.append(groupid)
                else:
                    pass  # 当前近邻所属同款组，在其他近邻所属同款组时已经检查过了。
            else:
                groupid2pos[point] = {point, nn}
                pos2groupid[nn] = point
                pos2groupid[point] = point
                is_grouped = True

    e_t = time.time()
    logger.info("[ %s ] clustering all points into group is done" % (e_t - s_t))
    return pos2groupid, groupid2pos


def build_group_xnn_with_reference(twitter_info, feature_data, flann, params, group_ref,
                                   twitter_info_ref, feature_data_ref, flann_ref, params_ref, threshold):
    """
    build_group_xnn_simple算法的扩展。 在聚类的时候，优先考虑加入reference中的已有类别。

    TODO: 算法过于复杂，没有开发完成。

    """
    logger = setup_logger('BLD')

    # twitter用feature data中的pos表示
    pos2groupid = {}  # {10:5, 20:5}
    groupid2pos = {}  # {5:set(5, 10, 20)}

    #   TODO： 有很多twitter的图片在物理上是一张图，所以将所有图片完全一样的twitter，作为聚类的种子。

    with Timer() as t:
        # 目标集合和ref集合的最近邻
        neighbors, distances = flann_ref.nn_index(feature_data, num_neighbors=NUM_NEIGHBORS, **params_ref)
    logger.info("[%s] query all %s points with %s nearest neighbors against reference index" % (t.elapsed, len(feature_data), NUM_NEIGHBORS))

    with Timer() as t:
        order_positions, order_neighbors, order_distances = _find_and_sort_neighbor(neighbors, distances, threshold, exclude_self=False)
        n = len(order_positions)
    logger.info("[%s] %s points with neighbors in the threshold %s are sorted" % (t.elapsed, threshold, n))

    now = time.time()
    s_t, e_t, s_i, e_i = now, now, now, now
    for i in xrange(n):
        if i % ITER_RANGE2 == 0:
            e_i = time.time()
            logger.debug(" [%s] %s points has been clustered" % (e_i - s_i, i))
            s_i = e_i

        point = order_positions[i]

        # if point in (4232,30124,35934,42950,6071):
        # import pdb
        # pdb.set_trace()

        # point已经被其他最近邻纳入某个同款组。 风险：当前最近邻不能发挥作用。
        if point in pos2groupid:
            continue
        members = order_neighbors[i]
        m = len(members)
        checked_group = []
        is_grouped = False
        j = 0
        while not is_grouped and j < m:
            nn = members[j]
            j += 1
            groupid = pos2groupid.get(nn)
            if groupid is not None:
                if groupid not in checked_group:
                    cur_group = groupid2pos[groupid]
                    if _is_insert_current_group(cur_group, point, feature_data, threshold):
                        cur_group.add(point)
                        pos2groupid[point] = groupid
                        is_grouped = True
                    checked_group.append(groupid)
                else:
                    pass  # 当前近邻所属同款组，在其他近邻所属同款组时已经检查过了。
            else:
                groupid2pos[point] = {point, nn}
                pos2groupid[nn] = point
                pos2groupid[point] = point
                is_grouped = True

    e_t = time.time()
    logger.info("[ %s ] clustering all points into group is done" % (e_t - s_t))
    return pos2groupid, groupid2pos


def _find_and_sort_neighbor(neighbors, distances, threshold, exclude_self=True):
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
        if exclude_self:
            idx = filter(lambda x: members[x] != i, range(j))
            members = map(lambda x: members[x], idx)
            gaps = map(lambda x: gaps[x], idx)


        if members:
            threshold_pair.append((i, members, gaps))

    # 按照最近邻的距离排序
    threshold_pair.sort(key=lambda x: x[2][0])
    order_positions = map(lambda x: x[0], threshold_pair)
    order_neighbors = map(lambda x: x[1], threshold_pair)
    order_distances = map(lambda x: x[2], threshold_pair)

    return order_positions, order_neighbors, order_distances


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




# ----------------------------------------------------------------------
def prepare_twitter_info_with_feature(info_file, feature_file, info_data_raw, force=False):
    """
    很多推没有图片的feature数据（gif不能处理，下载不成功etc），放在索引库中有各种问题，暂时忽略非法数据。
    根据raw的info_data， 获取feature数据，将合法数据的info_data存为info_file.
    返回新的info_data和feature_data

    TODO： 下载图片数据计算特征很耗时，比较适合可以利用多线程or多进程进行并行化。
    """

    logger = setup_logger('BLD')

    feature_file_npy = feature_file + '.npy'

    twitter_info = TwitterInfo()

    if not force and (os.path.exists(info_file)
                      and (os.path.exists(feature_file)
                           or os.path.exists(feature_file_npy))):
        logger.info("twitter info file %s and feature file %s is ready" % (info_file, feature_file))
        twitter_info.load(info_file)
        feature_data = try_load_npy(feature_file)
        return twitter_info, feature_data

    n_raw = len(info_data_raw)
    feature_data_raw = np.zeros((n_raw, IMAGE_FEATURE_DIM))
    # db = leveldb.LevelDB(FEATURE_DB_PATH)
    db = get_feature_db(FEATURE_DB_PATH)

    # feature数据可能从db中查询，也可能下载。db_hit表示命中查询， db_miss表示需要下载计算。
    # t表示总量， i表示每个子集（比如每1000）的数量。
    t_db_hit, t_db_miss, i_db_hit, i_db_miss = 0, 0, 0, 0

    now = time.time()
    start_t, end_t, start_i, end_i = now, now, now, now
    i_total, i_valid = 0, 0  # twitter数， 合法twitter数
    for i_total in xrange(n_raw):
        if i_total % ITER_RANGE == 0:
            end_i = time.time()
            logger.debug("[ %s ] download and compute %s images, db_hit=%s, db_miss=%s" % (
                end_i - start_i, i_total, i_db_hit, i_db_miss))
            sys.stdout.flush()
            t_db_hit += i_db_hit
            t_db_miss += i_db_miss
            start_i = end_i
            i_db_hit, i_db_miss = 0, 0

        line = info_data_raw[i_total]
        if len(line) != 5:
            continue
        tid, gid, shop_id, cat, url = line

        feature = None
        try:  # 目标数据库
            val_r = db.Get(url)
            tid_r, gid_r, shop_id_r, cat_r, feature_r = val_r.split("\t", 4)
            if feature_r:
                feature = np.loads(feature_r)
            i_db_hit += 1
        except KeyError:  # 下载图片，计算
            if url.startswith('/'):
                full_url = IMAGE_SERVER + url
            else:  # 0.3%左右缺少开头的/: pic/_o/42/4d/fa0e774f3969972866b23d0de022_311_266.png
                full_url = IMAGE_SERVER + '/' + url
            feature = download_and_compute_feature(full_url)
            if feature is not None:
                val_w = tid + "\t" + gid + "\t" + shop_id + "\t" + cat + "\t" + feature.dumps()
            else:
                val_w = tid + "\t" + gid + "\t" + shop_id + "\t" + cat + "\t" + ""
            i_db_miss += 1
            db.Put(url, val_w)
        #
        if feature is not None:
            twitter_info.append(line)
            feature_data_raw[i_valid] = feature
            i_valid += 1

    end_t = time.time()
    t_db_hit += i_db_hit
    t_db_miss += i_db_miss
    logger.info("[%s] prepare feature data for %s/%s twitters. db_hit=%s, db_miss=%s" \
                % (end_t - start_t, i_valid, n_raw, t_db_hit, t_db_miss))

    with Timer() as t:
        feature_data = feature_data_raw[:i_valid]
        if IS_DUMP_TXT_FEATURE:
            np.savetxt(feature_file, feature_data, fmt="%.4f", delimiter="\t")
        np.save(feature_file_npy, feature_data)
        twitter_info.save(info_file)
    logger.info(
        "[%s] save feature file %s(%s) and info file %s" % (t.elapsed, feature_file, feature_file_npy, info_file))

    return twitter_info, feature_data


def stat_shop_group_info(shop_group_file, category_names, group_categories, twitter_info_categories, force=False):
    """
    统计店家商品中重复的比例
    返回数据结构：
    shop_info = {'12305': [all, same1, same1_score, same2, same2_score]}
    all 表示店内商品数量
    same1表示全局同款的数量
    same2表示店内同款的数量
    same1_score表示全局同款恶劣程度，以同款组的规模来考虑 log(group_size)
    same2_socre表示店内同款的恶劣程度， 以同款组规模考虑 (group_size -1)/group_size
    """

    logger = setup_logger('BLD')
    
    if not force and os.path.exists(shop_group_file):
        logger.info("shop stat file %s is ready" % (shop_group_file))
        return True


    shop_info = {}
    with Timer() as t:
        # 极端条件下，某些类别没有twitter，没有group数据
        for c_name in group_categories:
            twitter_info = twitter_info_categories[c_name]
            twitter_data = twitter_info.get_data()

            # 每个店铺的商品总量 all
            for line in twitter_data:
                tid, goods_id, shop_id, category, url = line
                if shop_id not in shop_info:
                    shop_info[shop_id] = [0, 0, 0, 0, 0]
                shop_info[shop_id][0] += 1

            # 各种同款分值
            group = group_categories[c_name]
            for group_pos in group.get_group_list():
                group_set = group.get_member(group_pos)

                # 统计一个同款组内， 不同店铺的同款数量
                shop_info_local = {}
                for idx in group_set:
                    tid, goods_id, shop_id, category, url = twitter_data[idx]
                    if shop_id not in shop_info_local:
                        shop_info_local[shop_id] = set()
                    shop_info_local[shop_id].add(tid)

                # 将shop_info_local合并到shop_info
                same1_score = math.log(len(group_set))
                for shop_id, shop_group_set in shop_info_local.iteritems():
                    n = len(shop_group_set)
                    shop_info[shop_id][1] += n
                    shop_info[shop_id][2] += n*same1_score

                    if n > 1:
                        shop_info[shop_id][3] += n
                        shop_info[shop_id][4] += (n-1)  # n* ((n-1)/n)

        with open(shop_group_file, 'w') as fh:
            print >> fh, "\t".join(["shop_id", "all", 'same1', 'same1_score', 'same1_ratio', 'same2', 'same2_score', 'same2_ratio'])
            for k, v in shop_info.iteritems():
                print >>fh, "\t".join(map(str, (k, v[0], v[1], v[2], float(v[2]) / v[0], v[3], v[4], float(v[4])/v[0])))

    logger.info("[ %s ] calc the duplicated image for every shop in file %s" % (t.elapsed, shop_group_file))


def build_pipeline_with_twitter_info_raw(twitter_info_raw, data_dir, force=False):
    """
    @param twitter_info_raw:  未经提取特征，未经分类的原始twitter信息
    @param data_dir:  输出文件目录
    @return:
    """

    category_names = map(lambda x: x['name'], GOODS_CATEGORY)
    distance_thresholds = {}
    for category in GOODS_CATEGORY:
        name = category['name']
        threshold = category['threshold']
        distance_thresholds[name] = threshold

    info_data_raw_categories = {}
    for name in category_names:
        info_data_raw_categories[name] = []

    info_data_raw = twitter_info_raw.get_data()
    for line in info_data_raw:
        
        if len(line) == 5:
            # img_url字段不完整，比如 select * from brd_shop_goods_info_new where twitter_id = 2968549481;
            tid, gid, shop_id, catalog_id, img_url = line
        else:
            continue

        name = catalog_id_to_name(catalog_id)
        if name not in info_data_raw_categories:
            info_data_raw_categories[name] = []
        info_data_raw_categories[name].append(line)

    twitter_info_categories = {}
    feature_data_categories = {}
    for c_name in category_names:
        twitter_info_file = data_dir + '/%s_twitter_info' % c_name
        feature_file = data_dir + '/%s_feature_data' % c_name
        twitter_info, feature_data = prepare_twitter_info_with_feature(twitter_info_file, feature_file,
                                                                       info_data_raw_categories[c_name],
                                                                       force=force)
        twitter_info_categories[c_name] = twitter_info
        feature_data_categories[c_name] = feature_data

    flann_categories = {}
    flann_para_categories = {}
    for c_name in category_names:
        index_file = data_dir + '/%s_flann_index' % c_name
        index_para_file = data_dir + '/%s_flann_index_para' % c_name
        feature_data = feature_data_categories[c_name]
        flann, params = build_flann_index(index_file, index_para_file, feature_data, force=force)
        flann_categories[c_name] = flann
        flann_para_categories[c_name] = params

    group_categories = {}
    for c_name in category_names:
        twitter_info = twitter_info_categories[c_name]
        feature_data = feature_data_categories[c_name]
        
        if not twitter_info.get_data() or feature_data.shape[0]==0:
            continue

        group_pickle_file = data_dir + '/%s_group_pickle' % c_name
        group_dump_tid2group = data_dir + '/%s_group_dump_tid2group' % c_name
        group_dump_group2tid = data_dir + '/%s_group_dump_group2tid' % c_name
        twitter_info = twitter_info_categories[c_name]
        feature_data = feature_data_categories[c_name]
        flann = flann_categories[c_name]
        params = flann_para_categories[c_name]

        group = build_group_index(group_pickle_file, group_dump_tid2group, group_dump_group2tid,
                                  twitter_info, feature_data, flann, params, threshold=distance_thresholds[c_name],
                                  algorithm='xnn_simple', force=force)
        group_categories[c_name] = group

    shop_group_file = data_dir + '/shop_group_stat'
    shop_stat = stat_shop_group_info(shop_group_file, category_names,
                                     group_categories, twitter_info_categories, force=force)

    return shop_stat

if __name__=='__main__':
    raise Exception("do not run it directly.")

