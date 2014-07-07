#! /usr/bin/env python
#coding:utf8

"""
根据twitter_info文件，生成feature数据。
feature有两种方法： 第一，从leveldb中查询，第二，根据url下载计算。


"""

import sys
import re

import numpy as np
import leveldb
from contexttimer import Timer
import time



sys.path.append('./Gemini')
from samelib.utils import flat2list, try_load_npy
import FeatureHandle
import getfeature

feature_handle = FeatureHandle.FeatureHandle()

ITER_RANGE = 100

OLD_DB_PATH='./feature_old_leveldb'
NEW_DB_PATH='./feature_all_leveldb'

twitter_info_file='/home/work/taopeng/projects/20140529-detect-same-items/new-Gemini/Gemini/data/index_base/base_20140628_20140704/twitter_info_file'
# twitter_info_file='/home/work/taopeng/projects/20140529-detect-same-items/new-Gemini/Gemini/data/index_base/base_20140628_20140704/twitter_info_file.head'
feature_file_npy='/home/work/taopeng/projects/20140529-detect-same-items/new-Gemini/Gemini/data/index_base/base_20140628_20140704/feature_data.npy'
feature_file_txt = '/home/work/taopeng/projects/20140529-detect-same-items/new-Gemini/Gemini/data/index_base/base_20140628_20140704/feature_data'

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
    getfeature.imgDecode(img_data,img)
    Gist = getfeature.GistFeature()
    gistfea = getfeature.Mat()
    gistfea.create(1,960,5)

    ret = Gist.ExtractGistFeature(img,gistfea)
    if ret != 0:
        return None
    feature = np.zeros((960,),dtype=float)

    for i in xrange(960):
        feature[i] = gistfea.at_float(0,i)

    return feature


def main():
    
    db1 = leveldb.LevelDB(OLD_DB_PATH)
    db2 = leveldb.LevelDB(NEW_DB_PATH)
    with Timer() as t:
        twitter_info_data = flat2list(twitter_info_file)
        n = len(twitter_info_data)
        feature_data = np.zeros((n, 960), dtype=float)
    print "init %s feature in %s seconds" % (n, t.elapsed)

    count_db_already = 0
    count_db_hit = 0
    count_db_miss = 0

    db_already_1k = 0
    db_hit_1k = 0
    db_miss_1k = 0
    
    counter = 0
    s = time.time()
    e = time.time()
    s1k = time.time()
    e1k = time.time()
    # 很多图片不能处理，计算不成功，放在索引库中有各种问题。暂时忽略。
    twitter_info_ok = []
    j = 0
    for i in xrange(n):
        if counter % ITER_RANGE == 0:
            e1k = time.time()
            print "<%s> %s record in %s, db_hit=%s, db_miss=%s, db_already=%s" % (time.ctime(e1k), counter, e1k-s1k, db_hit_1k, db_miss_1k, db_already_1k)
            sys.stdout.flush()
            count_db_already +=  db_already_1k
            count_db_hit +=  db_hit_1k
            count_db_miss += db_miss_1k
            
            s1k = time.time()
            db_already_1k = 0
            db_hit_1k = 0
            db_miss_1k = 0
            
        counter += 1
        line = twitter_info_data[i]
        tid, gid, shopid, cat, url = line
        
        feature = None
        try:                            # 目标数据库
            val_r = db2.Get(url)
            tid_r, gid_r, shopid_r, cat_r, feature_r = val_r.split("\t", 4)
            if feature_r:
                feature = np.loads(feature_r)
            db_already_1k += 1
            
        except KeyError:                # 历史数据库
            try:
                val_r = db1.Get(url)
                tid_r, feature_r = val_r.split("\t", 1)
                
                # if tid_r != tid:
                #     print "query url %s with tid=%s hit tid=%s in db" % (url, tid, tid_r)
                #     raise KeyError
                
                val_w = tid + "\t" + gid + "\t" + shopid + "\t" + cat +  "\t" + feature_r
                if feature_r:
                    feature = np.loads(feature_r)
                db_hit_1k += 1
                
            except KeyError:            # 下载计算
                if url.startswith('/'):
                    full_url = 'http://imgst.meilishuo.net' + url
                else:  # 0.3%左右缺少开头的/: pic/_o/42/4d/fa0e774f3969972866b23d0de022_311_266.png
                    full_url = 'http://imgst.meilishuo.net/' + url
                feature = download_and_compute_feature(full_url)
                if feature is not None:
                    val_w = tid + "\t" + gid + "\t" + shopid + "\t" + cat +  "\t" + feature.dumps()
                else:
                    val_w = tid + "\t" + gid + "\t" + shopid + "\t" + cat +  "\t" + ""
                db_miss_1k += 1
                
            db2.Put(url, val_w)

        if feature is not None:
            twitter_info_ok.append(line)
            feature_data[j] = feature
            j += 1
            
        
    e = time.time()
    count_db_already +=  db_already_1k
    count_db_hit +=  db_hit_1k
    count_db_miss += db_miss_1k
    
    print "prepare data for %s in %s seconds" % (twitter_info_file, e-s)
    print "db hit = %s and mis = %s and already=%s" % (count_db_hit, count_db_miss, count_db_already)

    np.save(feature_file_npy, feature_data[:j])
    # feature_data.tofile(feature_file_txt, sep="\t", format="%.5f")
    np.savetxt(feature_file_txt, feature_data[:j], fmt="%.4f", delimiter="\t")
    fh = open(twitter_info_file + ".ok", "w")
    for line in twitter_info_ok:
        print >> fh, "\t".join(line)
    fh.close()

if __name__ == "__main__":
    main()
