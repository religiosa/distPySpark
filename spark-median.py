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

AppName = "humaloja-median"
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

# A function to import from the text file to a RDD.
# Mul is a variable, that tells, how many buckets are created.

def select_median(fn, mul):
    data = sc.textFile(fn).map(lambda s: float(s))
    return find_median_by_buckets(data, mul)

""" This function divides the value of the RDD into buckets. If mul is 1, then the numbers are divided by their int value. If mul is greater, then 0.12345678 is divided into bucket 0.12345678*100 -> 12.345678 -> bucket 12, by assigning a key. Then amounts of values in each bucket are counted and counts are sorted. The amounts from first bucket are summed until the index of the median has been passed. The bucket with median is sorted and median is selected and returned. """

def find_median_by_buckets(data, mul):
    ds = data.count()
    index_of_median = ds/2
    sum_until = 0

    # Used to deduce the rare case if median is between buckets.
    # If number of elements: 10, 10/2 = 5. The median is found
    # between 5 and 6 -> index of median (float) is 5.5.
    iom_float = ds/2.0 + 0.5

    # Check whether there is odd or even amount of numbers.
    if(ds%2 == 0): odd = 0 

    data = data.keyBy(lambda s: int(math.floor(s*mul)))

    partsizes = sorted(data.countByKey().items())
    buckets = []

    for (key, value) in partsizes:
        old_sum = sum_until
        sum_until = sum_until+value
        if(sum_until >= index_of_median):
            if(iom_float - sum_until == 0.5):
                # We know the median is between the buckets.
                buckets.append(key)
                buckets.append(next(partsizes)[1])
            buckets.append(key)
            break

    if len(buckets) == 2:
        # Median between buckets -> take both.
        newdata = data.filter(lambda s: s in buckets).values()
    else:
        # Select right bucket.
    	newdata = data.filter(lambda s: s[0] == buckets[0]).values()

    # Where the median is found. Takes into account all the 
    # discarded values on the left. 
    offset = index_of_median - old_sum

    l = sorted(newdata.collect())
    if(odd):
        return l[offset-1] 
    else:
        median = (l[offset-1] + l[offset]) / 2
        return median   


if __name__=="__main__":

    # Try with small data set.
#    print(select_median(DATA1SAMPLE, 1))

    # The real deal. I'm using 100 as mul, so that 10 000 buckets ar 
    # created. Also tried with just 100 buckets (mul = 1), but it was
    # too slow and it isn't good to sort 10 000 000 items here either

    print(select_median(DATA1, 100))

    sys.exit(0)

