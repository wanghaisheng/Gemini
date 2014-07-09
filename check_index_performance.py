#! /usr/bin/env python
#coding:utf8

import sys
import re
import json

from pyflann import FLANN
from contexttimer import Timer

from samelib.utils import try_load_npy

ALGORITHM = 'kmeans'
PARAS = {'iterations':5, 'branching':32, 'checks':65536}
feature_file = '/home/work/taopeng/projects/20140529-detect-same-items/new-Gemini/Gemini/data/index_base/base_20140628_20140704/feature_data.npy'
query_file = 'feature_data_rand1k.npy'
para_file = 'para_set3/flann_index_para'
index_file = 'para_set3/flann_index'

num_querys =  100 # 最大1000

def main():
    
    with Timer() as t:
        feature_data = try_load_npy(feature_file)        
    print "[%s] load feature file %s" % (t.elapsed, feature_file)
    sys.stdout.flush()


    with Timer() as t:
        flann0 = FLANN()
        flann0.build_index(feature_data, algorithm='linear')
    print "[%s] build linear index" % (t.elapsed)


    with Timer() as t:
        flann1 = FLANN()
        flann1.load_index(index_file, feature_data)
        para1 = json.load(open(para_file))
    print "[%s] load index" % (t.elapsed)
    sys.stdout.flush()
    
    query_data = try_load_npy(query_file)

    with Timer() as t:
        nn0, dis0 = flann0.nn_index(query_data[:num_querys], num_neighbors=10)
    print "[%s] query %s samples with linear index" % (t.elapsed, num_querys)
    sys.stdout.flush()

    with Timer() as t:
        nn1, dis1 = flann1.nn_index(query_data[:num_querys], num_neighbors=10, checks=para1['checks'])
    print "[%s] query %s samples with simple index" % (t.elapsed, num_querys)

    for i in range(len(nn0)):
        print "linear result: %s %s" % (nn0[i], dis0[i])
        print "linear result: %s %s" % (nn1[i], dis1[i])
        sys.stdout.flush()
        raw_input()
    

if __name__ == "__main__":
    main()
