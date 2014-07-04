#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 提供同款检测服务的http server
 History:
     1. 2014/6/30 20:38 : index.py is created.
"""

import os
import os.path
from argparse import ArgumentParser
from time import time
import logging
try:
    import cjson as json
except ImportError:
    import json

import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import numpy as np

from index import Index
from utils import extract_img_feature

# from tornado.options import define, options
# define("port", default=8081, help="run on the given port", type=int)

GOODS_CATEGORY = ["clothes", "shoes", "bag", "acc", "other"]
LOG_LEVEL = logging.DEBUG
LOG_FILE = 'server.log'



def setup_logger():

    logger = logging.getLogger('SVR')
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter("%(asctime)s-[%(name)s]-[%(levelname)s]: %(message)s")

    handler = logging.FileHandler(LOG_FILE)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

logger = setup_logger()

class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        self.render('index.html')


class ResultPageHandler(tornado.web.RequestHandler):
    def initialize(self):
        self._search_index = self.application._search_index

    def post(self):
        """
        input json for post request:
        """
        s = time()
        query_tid = self.get_argument('query_tid', default="010").encode('utf-8')
        img_url = self.get_argument('query_imgurl').encode('utf-8')
        category = self.get_argument('category', default="clothes").encode("utf-8")
        num = self.get_argument('num', default=3)

        if img_url:
            feature = extract_img_feature(img_url)
            results = self._search_index.search(feature, category, num)
            tid_list = self._search_index.get_index(category)['tid2img']
            ret = self._to_json(results, tid_list)
            self.render('result.tpl', text=ret)
        else:
            self.render('result.tpl', text='{}')

    def _to_json(self, results, tid_list):
        ret = []
        neighbor_list, distance_list = results
        for i in range(len(neighbor_list)):
            neighbor = neighbor_list[i]
            distance = distance_list[i]
            for j in range(len(neighbor)):
                pos = neighbor[j]
                tid, url = tid_list[pos]
                score = distance[j]
                pair = {'tid':tid, "sameScore":score}
                ret.append(pair)
        # 默认只有一个query向量
        return json.dumps(ret)








class Application(tornado.web.Application):
    """
    TODO: 将server改为多进程，提高并发的响应。
    """
    def __init__(self, index_dir):
        handles = [
            (r'/', IndexHandler),
            (r'/result', ResultPageHandler),
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), 'templates'),
            static_path=os.path.join(os.path.dirname(__file__),'static'),
            debug=True,
        )
        tornado.web.Application.__init__(self,handles,**settings)

        s = time()
        index = Index()
        index.load(GOODS_CATEGORY, index_dir)
        self._search_index = index
        e = time()
        logger.info("loading index data in %s seconds. " % (e-s))


def parse_args():
    """
    可能需要调整的内容:
    1. 端口
    2. 库文件路径

    TODO: 在config目录提供相应配置，根据tornado.options进行解析
    """
    parser = ArgumentParser(description='图片同款检测服务的http server.')
    parser.add_argument("--port", metavar='PORT', type=int, default=8081, help="server port.")
    parser.add_argument("--dir", metavar='DIR', default="index_data", help="path to index files.")
    args = parser.parse_args()
    return args

#----------------------------------------------------------------------
def main(args):
    # tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application(args.dir))
    # http_server.listen(options.port)
    http_server.listen(args.port)
    tornado.ioloop.IOLoop.instance().start()



if __name__ == '__main__':
    args = parse_args()
    main(args)
