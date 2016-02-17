#encoding: utf-8

#Jenny Tyrväinen
#013483708

#!/usr/bin/env python
import os
import sys

#from pyspark import SparkConf, SparkContext

TESTSAMPLE = '/cs/work/scratch/testdata/test-sample'
DATA1SAMPLE = '/cs/work/scratch/spark-data/data-1-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'

AppName = "humaloja-stats"
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

def get_stats(fn):
    data = sc.textFile(fn).map(lambda s: float(s))

    def add(a, b):
        return a+b

    # Calculate amount of numbers by mapping each value to 1
    # and then adding all together. Same as RDD.count()
    n = data.map(lambda s: 1).fold(0, add)

    minimum = data.reduce(lambda a, b: a if a<b else b)
    maximum = data.reduce(lambda a, b: a if a>b else b)
    x = data.reduce(lambda a,b: a+b) / n 

    # Calculate variance: sum from i=0 to n: (i-avg)^2 
    # divided by number of items.
    variance = data.map(lambda s: pow((s-x),2)).reduce(lambda a,b: a+b) / n

    return minimum, maximum, x, variance

if __name__=="__main__":

    # Print values: 
    minimum, maximum, mean, variance = get_stats(DATA1)
    print "Min: " + str(minimum) + " Max: " + str(maximum) + " Mean: " + str(mean) +  " Variance: " + str(variance) 

    sys.exit(0)

