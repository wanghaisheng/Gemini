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
import logging

import numpy as np



def try_load_npy(fn):

    ret = None
    if fn.endswith('.npy'):
        ret = np.load(fn)
    elif os.path.exists(fn+'.npy'):
        ret = np.load(fn+'.npy')
    else:
        ret = np.genfromtxt(fn)
        np.save(fn+'.npy', ret)
    return ret


def setup_logger(name, fn, level):

    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s-[%(name)s]-[%(levelname)s]: %(message)s")

    handler = logging.FileHandler(fn)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

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

if __name__=='__main__':
    pass
