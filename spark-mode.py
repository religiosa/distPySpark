#encoding: utf-8

#Jenny Tyrväinen
#013483708

#!/usr/bin/env python

import os
import sys
import numpy
import math
from operator import add
import random

#from pyspark import SparkConf, SparkContext

# Data
TESTSAMPLE = '/cs/work/scratch/testdata/test-sample'
DATA1SAMPLE = '/cs/work/scratch/spark-data/data-1-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'

AppName = "humaloja-mode"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Create a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 20)  # do not be greedy :-)
        .set("spark.local.dir", TMPDIR))
sc = SparkContext(conf = conf)

def count_occurrences(fn, treshold, give_values):
    data = sc.textFile(fn).map(lambda s: float(s))
    occ = data.map(lambda s: (s, 1)).reduceByKey(add)
    if(not give_values):
        occ = occ.filter(lambda s: s[1] > treshold).count()
    else:
        occ = occ.filter(lambda s: s[1] > treshold).count()

    return occ
    

if __name__=="__main__":

    # Print amounts over treshold and finally values
#    print("Number of items that exist 2 times or more:")
#    print(count_occurrences(DATA1, 1, 0))
#    print("Number of items that exist 3 times or more:")
#    print(count_occurrences(DATA1, 2, 0))
#    print("Number of items that exist 4 times or more:")
#    print(count_occurrences(DATA1, 3, 0))
#    print("Number of items that exist 5 times or more:")
#    print(count_occurrences(DATA1, 4, 0))
#    print("Number of items that exist 6 times or more:")
#    print(count_occurrences(DATA1, 5, 0))
#    print("Number of items that exist 7 times or more:")
#    print(count_occurrences(DATA1, 6, 0))
    print("Values of modes:")
    print(count_occurrences(DATA1, 5, 1))

    sys.exit(0)

