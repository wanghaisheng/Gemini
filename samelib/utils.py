#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 同款检测的工具函数。
 History:
     1. 2014/6/30 23:56 : utils.py is created.
"""


import os
import sys
import logging

import numpy as np
import scipy.spatial

import leveldb

from config import Config
conf = Config('./conf/build.yaml')

GOODS_CATEGORY = conf['GOODS_CATEGORY']

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]

def distance_matrix(points, feature_data):
    vector = feature_data[points]
    return scipy.spatial.distance.cdist(vector, vector)


def np_iter_loadtxt(filename, delimiter=" ", skiprows=0, dtype=float):
    """
     np.genfromtxt很耗内存， 68.9w数据不能执行。
     np.loadtxt略好， 占用接近80%的内存后完成load
     这里是一个更高效的实现。
     http://stackoverflow.com/questions/8956832/python-out-of-memory-on-large-csv-file-numpy
    """
    def iter_func():
        with open(filename, 'r') as infile:
            for _ in range(skiprows):
                next(infile)
            for line in infile:
                line = line.rstrip().split(delimiter)
                for item in line:
                    yield dtype(item)
        np_iter_loadtxt.rowlength = len(line)

    data = np.fromiter(iter_func(), dtype=dtype)
    data = data.reshape((-1, np_iter_loadtxt.rowlength))
    return data



def try_load_npy(fn):

    ret = None
    if fn.endswith('.npy'):
        ret = np.load(fn)
    elif os.path.exists(fn+'.npy'):
        ret = np.load(fn+'.npy')
    else:
        # ret = np.genfromtxt(fn)
        ret = np_iter_loadtxt(fn)
        np.save(fn+'.npy', ret)
    return ret


def setup_logger(name, fn=None, level=logging.DEBUG):

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s-[%(name)s]-[%(levelname)s]: %(message)s")

        handler = logging.FileHandler(fn)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        if 1 : # if conf['IS_PRINT_LOG_TO_SCREEN']:
            handler = logging.StreamHandler(sys.stderr)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    return logger

dbs = {}
def get_feature_db(path):
    if path not in dbs:
        dbs[path] = leveldb.LevelDB(path)
        return dbs[path]
    else:
        return dbs[path]
    

def flat2list(fn, sep="\t", header=0):
    res = []
    if type(fn) == type(""):
        fn = [fn]
    for f in fn:
        fh = file(f)
        for i in range(header):
            fh.next()
        for line in fh:
            fs = line.strip().split(sep)
            res.append(fs)
        fh.close()

    return res

def flat2dict_ext(fn, kpos=0, vpos=[], sep="\t", override=True, header=0):
    """Create a memory dict structure from flat file.
    From : key val ==> d[key] = val

    @type fn: list
    @param fn: list of flat file names
    @type kpos: int
    @param kpos: the fields position of the key. default to 0
    @type vpos: list
    @param vpos: The postion list of the values. default to empty.
    @type override: bool
    @param override: Whether to override the values if the keys are duplicated.
    @type header: int
    @param header: the number of header lines (to be skipped). default to 0
    @rtype: dict
    @return: the desired dict

    """
    res = {}
    if type(fn) == type(""):
        fn = [fn]
    for f in fn:
        fh = file(f)
        for i in range(header):
            fh.next()
        for line in fh:
            fs = line.strip().split(sep)
            k = fs[kpos]
            if vpos:
                v = map(lambda x:fs[x], vpos)
            else:
                v = fs[1:]
            if k not in res:
                res[k] = v
            else:
                if override:
                    res[k] = v
        fh.close()

    return res

def flat2dict(fn, sep="\t", override=True, header=0):
    """
    Create a memory dict structure from flat file.
    From : key val ==> d[key] = val(None)

    The parameters are less but with the same meaning with function
    create_dict_from_flat, which, with nest-structured value, will consume a
    large amount of memory when key number increase.

    """
    res = {}
    if type(fn) == type(""):
        fn = [fn]
    for f in fn:
        fh = file(f)
        for i in range(header):
            fh.next()
        for line in fh:
            fs = line.strip().split(sep, 1)
            k, v = fs[0], None
            if len(fs) == 2:
                v = fs[1]
            if k not in res:
                res[k] = v
            else:
                if override:
                    res[k] = v
        fh.close()

    return res

def extract_img_feature(url):

    ExtractGistFeature = "./ExtractGist"
    output_file = "./tmp.image.url.txt"
    cmd = '%s %s %s' % (ExtractGistFeature, url, output_file)
    os.system(cmd)
    query = np.genfromtxt(output_file)
    return query

_catalog_id_rules = []
def catalog_id_to_name(catalog_id):
    if not _catalog_id_rules:
        for ele in GOODS_CATEGORY:
            _catalog_id_rules.append((ele['prefix'], ele['name']))
    for rule in _catalog_id_rules:
        if catalog_id.startswith(rule[0]):
            return rule[1]
    return 'error'


if __name__=='__main__':
    pass
