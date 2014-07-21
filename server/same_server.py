#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 提供同款检测服务的http server
     2. http://server/result接口
         2.1 输入： {'data':[
                      {'twitter_id': 13345, 'goods_id'：1345, 'shop_id'： 123 'category':'shoes', 'img_url':'/pic/afsfdsf.jpg'} ,
                      {'twitter_id': 13377, 'goods_id'：1347,                 'category':'clothes', 'img_url':'/pic/dsfjsldflads.jpg'}
                      ];
                      'method':'group'}
         2.2 输出： [
                     { 'twitter_id': 13345, 'group_id': 13345, 'neighbors': [ 12346, 12235, 12266] } ,
                     { 'twitter_id': 13345, 'group_id': -1} ,
                   ]

 History:
     1. 2014/6/30 20:38 : index.py is created.
"""

import os
import sys
from argparse import ArgumentParser
import time
import logging
import multiprocessing
import random

import json
from contexttimer import Timer

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import numpy as np

sys.path.append(os.path.dirname(__file__)+'/../')
from samelib.utils import setup_logger, get_feature_db
from samelib.twitter import TwitterInfo
from samelib.config import Config
from feature.feature import download_and_compute_feature


# from tornado.options import define, options
# define("port", default=8081, help="run on the given port", type=int)

http_server = None

conf = Config('./conf/build.yaml')
WORK_BASE_PATH = conf['WORK_BASE_PATH']
LOG_FILE = WORK_BASE_PATH + '/log/server.log'
LOG_LEVEL = logging.DEBUG
logger = setup_logger('SVR', LOG_FILE, LOG_LEVEL)

PATH_INDEX_WEEKLY = WORK_BASE_PATH + '/data/index_label_weekly/current'
PATH_INDEX_DAILY = WORK_BASE_PATH + '/data/index_label_daily/current'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']

GOODS_CATEGORY = conf['GOODS_CATEGORY']
category_names = map(lambda x:x['name'], GOODS_CATEGORY)
category_thresholds = {}
for category in GOODS_CATEGORY:
    name = category['name']
    threshold = category['threshold']
    category_thresholds[name] = threshold



RT_INDEX_DIR = os.path.dirname(__file__) + '/rt_index_dir'
IMAGE_SERVER = 'http://imgst.meilishuo.net'
PROCESS_NUM = 8  # 同时工作的进程数
NEIGHBOR_NUM = 10   # 最多的同款数量
SERVER_PORT = conf['SERVER_PORT']

RT_INDEX_MAX = 50000
RT_INDEX_MIN = 30000

from samelib.index import Index


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')

class PingHandler(tornado.web.RequestHandler):
    def get(self):
        self.write({'status':0, 'message':'server is ok'})


class ExitHandler(tornado.web.RequestHandler):
    def initialize(self):
        self._key = 'UkoJRlAIxsCNlAWO'
        self._flag = False

    def post(self):
        # TODO 优雅关闭
        # 保存实时索引的数据
        vkey = self.get_argument('vkey')
        if vkey != self._key:
            self.write(json.dumps({'status':2, 'message':'invalid vkey, failed to close the server'}))
            return
        
        self.write(json.dumps({'status':0, 'message':'the server is shutingdown'}))
        self._flag = True
        index_rt = self.application.index_rt
        index_rt.save(RT_INDEX_DIR)
        logger.info("save the rt index %s and exit" % RT_INDEX_DIR )

        # feature_db = self.application.feature_db
        # del feature_db # 没找到close方法，保证解锁
        
    def on_finish(self):
        if self._flag:
            global http_server
            http_server.stop()
            tornado.ioloop.IOLoop.instance().stop()



def split_feature_into_categories(feature_data, twitter_info):
    """ 将feature数据按照类目进行查分。
    @param feature_data:
    @param twitter_info:
    @return:
    """

    idx_categories = {}
    n = twitter_info.get_length()
    twitter_data = twitter_info.get_data()
    for i in xrange(n):
        c_name = twitter_data[i][3]
        if c_name not in idx_categories:
            idx_categories[c_name] = []
        idx_categories[c_name].append(i)

    feature_data_categories = {}
    twitter_info_categories = {}
    for c_name, idx in idx_categories.iteritems():
        twitter_info_categories[c_name] = TwitterInfo()
        data = map(lambda x: twitter_data[x], idx)
        twitter_info_categories[c_name].set_data(data)
        feature_data_categories[c_name] = feature_data[idx]

    return feature_data_categories, twitter_info_categories


class ResultPageHandler(tornado.web.RequestHandler):

    def initialize(self):
        self._index_base = self.application.index_base
        self._index_daily = self.application.index_daily
        self._index_rt = self.application.index_rt
        self._feature_db = self.application.feature_db
        self._workers = self.application.workers
        self._queryid = random.random()

    def _get_features(self, twitter_info_raw):
        """ 根据url获取图片特征 """

        n = twitter_info_raw.get_length()
        positions_hit = []
        positions_miss = []
        features_hit = []
        urls_miss = []
        s = time.time()
        for i in xrange(n):
            (tid, gid, shop_id, cat, url) = twitter_info_raw[i]
            try:
                val_r = self._feature_db.Get(url)
                tid_r, gid_r, shop_id_r, cat_r, feature_r = val_r.split("\t", 4)
                positions_hit.append(i)
                if feature_r:
                    features_hit.append(np.loads(feature_r))
                else:
                    features_hit.append(None)
            except KeyError:
                if url.startswith('/'):
                    full_url = IMAGE_SERVER + url
                else:  # 0.3%左右缺少开头的/: pic/_o/42/4d/fa0e774f3969972866b23d0de022_311_266.png
                    full_url = IMAGE_SERVER + '/' + url
                positions_miss.append(i)
                urls_miss.append(full_url)
        e = time.time()
        n_hit, n_miss = len(positions_hit), len(positions_miss)
        logger.info("<%s> [%s] get %s/%s features in leveldb" %(self._queryid, e-s, n_hit, n_hit+n_miss))

        if urls_miss:
            s = e
            features_miss = self._workers.map(download_and_compute_feature, urls_miss)
            e =time.time()
            logger.info("<%s> [%s] get %s features average %s by downloading and computing" % (self._queryid, e-s, n_miss, (e-s)/n_miss))


        # 合并, 有img特征的集合， 无img特征自然没有group id
        s = e
        result_set = []
        feature_data_list = []
        twitter_info = TwitterInfo()

        for i in xrange(n_hit):
            pos = positions_hit[i]
            feature = features_hit[i]
            if feature is not None:
                feature_data_list.append(feature)
                twitter_info.append(twitter_info_raw[pos])
            else:
                twitter_id, goods_id, shop_id, category, url = twitter_info_raw[pos]
                ret = {'twitter_id': twitter_id, 'group_id': -1}
                result_set.append(ret)

        for i in xrange(n_miss):
            pos = positions_miss[i]
            feature = features_miss[i]
            if feature is not None:
                feature_data_list.append(feature)
                twitter_id, goods_id, shop_id, category, url = twitter_info_raw[pos]
                twitter_info.append(twitter_info_raw[pos])
                val_w = twitter_id + "\t" + goods_id + "\t" + shop_id + "\t" + category + "\t" + feature.dumps()
                self._feature_db.Put(url, val_w)
            else:
                twitter_id, goods_id, shop_id, category, url = twitter_info_raw[pos]
                ret = {'twitter_id': twitter_id, 'group_id': -1}
                result_set.append(ret)
                val = "\t".join((twitter_id, goods_id, shop_id, category, ''))
                self._feature_db.Put(url, val)

        n = len(feature_data_list)
        feature_data = np.zeros((n, IMAGE_FEATURE_DIM))
        for i in xrange(n):
            feature_data[i] = feature_data_list[i]

        e = time.time()
        logger.info("<%s> [%s] merge ok and fail feature %s/%s features." % (
            self._queryid, e - s, n, n+len(result_set) ))

        return feature_data, twitter_info, result_set


    def _map_req_to_twitter_info(self, reqs):

        twitter_info = TwitterInfo()
        for req in reqs:
            line = map(lambda x: x.encode('ascii'), (req['twitter_id'], req.get('goods_id', '-1'), req.get('shop_id', '-1'), req['category'], req['img_url']))
            twitter_info.append(line)

        return twitter_info

    def _filter_nn(self, nn, distances, twitter_info, twitter_data_index, threshold):
        """ 符合不同类目近邻标准的提取出来，不符合的返回 feature_data 继续下一轮查询"""
        result_set = []
        remain_pos = []

        n = twitter_info.get_length()
        for i in xrange(n):
            members = nn[i]
            gaps = distances[i]
            tid, gid, shop_id, category, url = twitter_info[i]
            # 阈值之内才算neighbour， 返回数据按照距离排序
            m = len(members)
            j = 0
            while j < m:
                if gaps[j] >= threshold:
                    break
                j += 1
            members = members[:j]

            if len(members) > 0:        # members是np.ndarray
                nn_tids = map(lambda x: twitter_data_index[x][0], members)
                ret = {'twitter_id':tid, 'group_id': nn_tids[0], 'neighbors':nn_tids}
                result_set.append(ret)
            else:
                remain_pos.append(i)

        return result_set, remain_pos


    def _filter_nn_self(self, nn, distances, twitter_info, twitter_data_index, threshold, offset):
        """
        查询包括自身在内的索引库，
        保证返回的聚类不存在较差索引， A的同款是B， B的同款是A，互相等待，死锁。

        """

        n = twitter_info.get_length()
        result_set = []
        remain_pos = []
        for i in xrange(n):
            members = nn[i]
            gaps = distances[i]
            tid, gid, shop_id, category, url = twitter_info[i]
            # 阈值之内才算neighbour， 返回数据按照距离排序
            m = len(members)
            j = 0
            positions = []
            while j < m:
                if gaps[j] >= threshold:
                    break
                # 自身不算，
                if members[j] != offset + i:
                    positions.append(j)
                j += 1

            members = map(lambda x: members[x], positions)
            if len(members)>0:
                nn_tids = map(lambda x: twitter_data_index[x][0], members)
                ret = {'twitter_id': tid, 'group_id': nn_tids[0], 'neighbors': nn_tids}
                result_set.append(ret)
            else:
                remain_pos.append(i)

        return result_set, remain_pos


    def _untie_groupid_cross_ref(self, result_set):
        """
        避免类别id交叉索引，造成上游程序死锁
        123 --> groupid --> 234
        234 --> groupid --> 123
        """
        tid2group = {}
        group2tid = {}

        for t in result_set:
            tid = t['twitter_id']
            groupid = t['group_id']
            if tid in tid2group:        # 已经进入某个类别
                continue
            final_group = tid2group.get(groupid)
            if final_group is not None:
                tid2group[tid] = final_group
                group2tid[final_group].add(tid)
            else:
                tid2group[tid] = tid
                tid2group[groupid] = tid
                group2tid[tid] = {tid, groupid}
                
        new_result_set = []                
        for t in result_set:
            r = {'twitter_id': t['twitter_id'], 'group_id':tid2group[t['twitter_id']], 'neighbors':t['neighbors'] }
            new_result_set.append(r)

        return new_result_set

    def post(self):
        """
        input json for post request:
        """
        method = self.get_argument('method')
        if method != 'group':
            return json.dumps({'status': 1, 'message': 'bad method', 'data':[]})
            
        reqs = json.loads(self.get_argument('data', '[]'))
        
        # import pdb
        # pdb.set_trace()

        twitter_info_raw = self._map_req_to_twitter_info(reqs)
        feature_data, twitter_info, result_set = self._get_features(twitter_info_raw)

        feature_data_categories, twitter_info_categories = split_feature_into_categories(feature_data, twitter_info)

        for c_name in twitter_info_categories:
            feature_data = feature_data_categories[c_name]
            twitter_info = twitter_info_categories[c_name]
            threshold = category_thresholds[c_name]
            # 查询大库
            with Timer() as t:
                nn, distance = self._index_base.search(c_name, feature_data, neighbors=NEIGHBOR_NUM)
                twitter_data_index = self._index_base.get_twitter_info(c_name).get_data()
                res, positions = self._filter_nn(nn, distance, twitter_info, twitter_data_index, threshold)
                result_set += res
            logger.info("<%s> [%s] find %s/%s neighbors in %s base index" % (
                self._queryid, t.elapsed, len(res), twitter_info.get_length(), c_name))

            if positions:
                # 查询天级库
                with Timer() as t:
                    feature_data2 = feature_data[positions]
                    twitter_info2 = TwitterInfo()
                    for pos in positions:
                        twitter_info2.append(twitter_info[pos])
                    nn, distance = self._index_daily.search(c_name, feature_data2, neighbors=NEIGHBOR_NUM)
                    twitter_data_index2 = self._index_daily.get_twitter_info(c_name).get_data()
                    res, positions = self._filter_nn(nn, distance, twitter_info2, twitter_data_index2, threshold)
                    result_set += res
                logger.info("<%s> [%s] find %s/%s neighbors in %s daily index" % (
                    self._queryid, t.elapsed, len(res), twitter_info2.get_length(), c_name))

            if positions:
                # 查询realtime库
                with Timer() as t:
                    feature_data3 = feature_data2[positions]
                    twitter_info3 = TwitterInfo()
                    for pos in positions:
                        twitter_info3.append(twitter_info2[pos])

                    offset = self._index_rt.insert(c_name, feature_data3, twitter_info3)
                    nn, distance = self._index_rt.search(c_name, feature_data3, neighbors=NEIGHBOR_NUM)
                    twitter_data_index3 = self._index_rt.get_twitter_info(c_name).get_data()
                    res, positions = self._filter_nn_self(nn, distance, twitter_info3, twitter_data_index3, threshold, offset)
                    res = self._untie_groupid_cross_ref(res)
                    result_set += res
                logger.info("<%s> [%s] find %s/%s neighbors in %s realtime index" % (
                    self._queryid, t.elapsed, len(res), twitter_info3.get_length(), c_name))


            for pos in positions:
                ret = {'twitter_id': twitter_info3[pos][0], 'group_id': -1}
                result_set.append(ret)

        respones = {'status': 0, 'message': 'successful', 'data':result_set}
        self.write(json.dumps(respones))


    def on_finish(self):
        with Timer() as t:
            self._index_rt.shrink(max_size=RT_INDEX_MAX, min_size=RT_INDEX_MIN)
        logger.info("<%s> [%s] rt index shrinked " % (
                self._queryid, t.elapsed))


class Application(tornado.web.Application):
    """
    TODO: 将重量级初始化（索引加载etc）从这里摘出来，避免ping等服务无响应。
    """
    def __init__(self, base_dir, daily_dir, leveldb=None):
        handles = [
            (r'/ping', PingHandler),
            (r'/result', ResultPageHandler),
            (r'/exit', ExitHandler)
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), 'templates'),
            static_path=os.path.join(os.path.dirname(__file__),'static'),
            debug=True,
        )
        tornado.web.Application.__init__(self,handles,**settings)
    
        # 加载大库
        with Timer() as t:
            index1 = Index()
            index1.load(base_dir)
        logger.info("[%s] loading base index data in %s . " % (t.elapsed, base_dir))

        # 加载天级库
        with Timer() as t:
            index2 = Index()
            index2.load(daily_dir)
        logger.info("[%s] loading daily index data in %s . " % (t.elapsed, daily_dir))
    
    
        # 加载实时库
        with Timer() as t:
            index3 = Index()
            index3.load(RT_INDEX_DIR)
        logger.info("[%s] loading daily index data in %s . " % (t.elapsed, RT_INDEX_DIR))
    
        self.index_base = index1
        self.index_daily = index2
        self.index_rt = index3
    
        self.feature_db = get_feature_db(FEATURE_DB_PATH)
        self.workers = multiprocessing.Pool(PROCESS_NUM)

def parse_args():
    """
    可能需要调整的内容:
    1. 端口
    2. 库文件路径

    TODO: 在config目录提供相应配置，根据tornado.options进行解析
    """
    parser = ArgumentParser(description='图片同款检测服务的http server.')
    parser.add_argument("--basedir", metavar='BASE_DIR', default=PATH_INDEX_WEEKLY, help="path to base index files.")
    parser.add_argument("--daydir", metavar='DAY_DIR', default=PATH_INDEX_DAILY, help="path to daily index files.")
    parser.add_argument("--port", metavar='PORT', type=int, default=SERVER_PORT, help="server port. default %s" % SERVER_PORT )
    args = parser.parse_args()
    return args

#----------------------------------------------------------------------
def main(args):
    global http_server
    
    # tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application(args.basedir, args.daydir))
    # http_server.listen(options.port)
    http_server.listen(args.port)
    tornado.ioloop.IOLoop.instance().start()
    logger.info("same server is exited")


if __name__ == '__main__':
    args = parse_args()
    main(args)
