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
from samelib.index import Index
from samelib.twitter import TwitterInfo
from samelib.utils import setup_logger, get_feature_db
from samelib.config import Config
from feature.feature import download_and_compute_feature

# from tornado.options import define, options
# define("port", default=8081, help="run on the given port", type=int)

conf = Config('./conf/build.yaml')


WORK_BASE_PATH = conf['WORK_BASE_PATH']
LOG_FILE = WORK_BASE_PATH + '/log/server.log'
LOG_LEVEL = logging.DEBUG
DATA_PATH = WORK_BASE_PATH + '/data/index_day'
FEATURE_DB_PATH = WORK_BASE_PATH + '/data/feature_all_leveldb'

IMAGE_FEATURE_DIM = conf['IMAGE_FEATURE_DIM']

GOODS_CATEGORY = ["clothes", "shoes", "bag", "acc", "other"]



RT_INDEX_DIR = os.path.dirname(__file__) + '/rt_index_dir'
IMAGE_SERVER = 'http://imgst.meilishuo.net'
PROCESS_NUM = 8  # 同时工作的进程数
NEIGHBOR_NUM = 10   # 最多的同款数量
SERVER_PORT = conf['SERVER_PORT']
CATEGORY_THRESHOLD = {
    'clothes': 0.1089,
    'shoes': 0.09,
    'bag':   0.1225,
    'acc':   0.04,
    'other': 0.09 }

RT_INDEX_MAX = 1000
RT_INDEX_MIN = 300



logger = setup_logger('SVR', LOG_FILE, LOG_LEVEL)

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')


class ExitHandler(tornado.web.RequestHandler):
    def initialize(self):
        self._key = 'UkoJRlAIxsCNlAWO'

    def post(self):
        # TODO 优雅关闭
        # 保存实时索引的数据
        pass


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
                val = "\t".join((twitter_id, goods_id, shop_id, category, np.dumps(feature)))
                twitter_info.append(twitter_info_raw[pos])
                self._feature_db.Put(url, val)
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
            line = (req['twitter_id'], req.get('goods_id', '-1'), req.get('shop_id', '-1'), req['category'], req['img_url'])
            twitter_info.append(line)

        return twitter_info

    def _filter_nn(self, nn, distances, twitter_info, index):
        """ 符合不同类目近邻标准的提取出来，不符合的返回 feature_data 继续下一轮查询"""

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
            while j < m:
                if gaps[j] >= CATEGORY_THRESHOLD[category]:
                    break
                j += 1
            members = members[:j]


            if members:
                pos = members[0]
                nn_tids = map(lambda x: index.get_tid(x), members)
                ret = {'twitter_id':tid, 'group_id': nn_tid[0], 'neighbors':nn_tids}
                result_set.append(ret)
            else:
                remain_pos.append(i)
        return result_set, remain_pos


    def _filter_nn_self(self, nn, distances, twitter_info, index, offset):
        """ 查询包括自身在内的索引库，
        符合不同类目近邻标准的提取出来，不符合的返回 feature_data 继续下一轮查询"""

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
                if gaps[j] >= CATEGORY_THRESHOLD[category]:
                    break
                # 自身不算，
                if members[j] != offset + i:
                    positions.append(j)
                j += 1

            members = map(lambda x: members[x], positions)
            if members:
                nn_tids = map(lambda x: index.get_tid(x), members)
                ret = {'twitter_id': tid, 'group_id': nn_tid[0], 'neighbors': nn_tids}
                result_set.append(ret)
            else:
                remain_pos.append(i)

        return result_set, remain_pos



    def post(self):
        """
        input json for post request:
        """

        import pdb
        pdb.set_trace()
        method = self.get_argument('method')
        if method != 'group':
            return json.dumps({'status': 1, 'message': 'bad method', 'data':[]})
            
        reqs = json.loads(self.get_argument('data', '[]'))

        twitter_info_raw = self._map_req_to_twitter_info(reqs)
        feature_data, twitter_info, result_set = self._get_features(twitter_info_raw)

        # 查询大库
        with Timer() as t:
            nn, distance = self._index_base.search(feature_data, neighbors=NEIGHBOR_NUM)
            res, positions = self._filter_nn(nn, distance, twitter_info, self._index_base)
            result_set += res
        logger.info("<%s> [%s] find %s/%s neighbors in base index" % (
            self._queryid, t.elapsed, len(res), twitter_info.get_length()))

        # 查询天级库
        with Timer() as t:
            feature_data2 = feature_data[positions]
            twitter_info2 = TwitterInfo()
            for pos in positions:
                twitter_info2.append(twitter_info[pos])
            nn, distance = self._index_daily.search(feature_data2, neighbors=NEIGHBOR_NUM)
            res, positions = self._filter_nn(nn, distance, twitter_info2, self._index_daily)
            result_set += res
        logger.info("<%s> [%s] find %s/%s neighbors in daily index" % (
            self._queryid, t.elapsed, len(res), twitter_info2.get_length()))

        # 查询realtime库
        with Timer() as t:
            feature_data3 = feature_data2[positions]
            twitter_info3 = TwitterInfo()
            for pos in positions:
                twitter_info3.append(twitter_info2[pos])
            offset = self._index_rt.append(feature_data3, twitter_info3)
            nn, distance = self._index_rt.search(feature_data3, neighbors=NEIGHBOR_NUM)
            res, positions = self._filter_nn_self(nn, distance, twitter_info3, self._index_rt, offset)
            result_set += res
        logger.info("<%s> [%s] find %s/%s neighbors in realtime index" % (
            self._queryid, t.elapsed, len(res), twitter_info3.get_length()))


        for pos in positions:
            ret = {'twitter_id': twitter_info3[pos][0], 'group_id': -1}
            result_set.append(ret)

        return json.dumps(result_set)


    def on_finish(self):
        with Timer() as t:
            old_size = self._index_rt.shrink(max_size=RT_INDEX_MAX, min_size=RT_INDEX_MIN)
        if old_size != -1:
            logger.info("<%s> [%s] rt index shrinked from %s to %s" % (
                self._queryid, t.elapsed, old_size, RT_INDEX_MIN))


class Application(tornado.web.Application):
    """
    TODO: 将server改为多进程，提高并发的响应。
    """
    def __init__(self, base_dir, daily_dir, leveldb=None):
        handles = [
            (r'/', IndexHandler),
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
    parser.add_argument("basedir", metavar='BASE_DIR', help="path to base index files.")
    parser.add_argument("daydir", metavar='DAY_DIR', help="path to daily index files.")
    parser.add_argument("--port", metavar='PORT', type=int, default=SERVER_PORT, help="server port. default %s" % SERVER_PORT )
    args = parser.parse_args()
    return args

#----------------------------------------------------------------------
def main(args):
    # tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application(args.basedir, args.daydir))
    # http_server.listen(options.port)
    http_server.listen(args.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    args = parse_args()
    main(args)
