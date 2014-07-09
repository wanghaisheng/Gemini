#! /usr/bin/env python
#coding:utf8

"""
 Author:  tao peng --<taopeng@meilishuo.com>
 Purpose:
     1. 荣国提供的下载图片，计算图片特征的so不是很方便，继续封装一层。
     2. 并提供多线程能力，并发下载和计算。
 History:
     1. 2014/7/5 23:43 : feature.py is created.
"""

import os
import sys
import re

import numpy as np

sys.path.append(os.path.dirname(__file__) + '/../')
from samelib.utils import flat2list, try_load_npy
import FeatureHandle
import getfeature

feature_handle = FeatureHandle.FeatureHandle()

def download_and_compute_feature(url):

    global feature_handle
    n_try = 0
    while n_try < 3:
        img_data = feature_handle.download(url)
        if img_data:
            break
        n_try += 1
    if not img_data:
        return None
    img = getfeature.Mat()
    getfeature.imgDecode(img_data, img)
    Gist = getfeature.GistFeature()
    gistfea = getfeature.Mat()
    gistfea.create(1, 960, 5)

    ret = Gist.ExtractGistFeature(img, gistfea)
    if ret != 0:
        return None
    feature = np.zeros((960,), dtype=float)

    for i in xrange(960):
        feature[i] = gistfea.at_float(0, i)

    return feature


if __name__ == '__main__':
    raise Exception("do not run it directly")
