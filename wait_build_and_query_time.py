#! /usr/bin/env python
#coding:utf8

import sys
import re
import json

from pyflann import FLANN
from contexttimer import Timer

from samelib.utils import try_load_npy

ALGORITHM = 'kmeans'
PARAS = {'iterations':10, 'branching':128, 'checks':4096}
feature_file = '/home/work/taopeng/projects/20140529-detect-same-items/new-Gemini/Gemini/data/index_base/base_20140628_20140704/feature_data.npy'
query_file = 'feature_data_rand1k.npy'
para_file = 'flann_index_para'
index_file = 'flann_index'

num_querys =  1000 # 最大1000

def main():
    flann = FLANN()
    with Timer() as t:
        feature_data = try_load_npy(feature_file)        
    print "[%s] load feature file %s" % (t.elapsed, feature_file)
    sys.stdout.flush()
    

    with Timer() as t:
        params = flann.build_index(feature_data, algorithm=ALGORITHM, **PARAS)
        json.dump(params, open(para_file, 'w'))
        flann.save_index(index_file)
        
    print "[%s] build %s index" % (t.elapsed, ALGORITHM)
    sys.stdout.flush()
    
    query_data = try_load_npy(query_file)

    with Timer() as t:
        flann.nn_index(query_data[:num_querys], num_neighbors=10)
    print "[%s] query %s samples with %s index" % (t.elapsed, num_querys, ALGORITHM)
    sys.stdout.flush()

if __name__ == "__main__":
    main()
