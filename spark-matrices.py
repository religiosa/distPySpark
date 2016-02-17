#encoding: utf-8

#Jenny Tyrväinen
#013483708

#!/usr/bin/env python

import os
import sys
import numpy
from operator import add

#from pyspark import SparkConf, SparkContext


### Two data sets we will use, along with two samples
MATRIX = '/cs/work/scratch/testdata/matrixtest'
MATRIX2 = '/cs/work/scratch/testdata/matrixtest2'
DATA2SAMPLE = '/cs/work/scratch/spark-data/data-2-sample.txt'
DATA2 = '/cs/work/scratch/spark-data/data-2.txt'

### Some variables you may want to personalize
AppName = "humaloja-matrices"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 40)  # do not be greedy :-)
        .set("spark.driver.maxResultSize", "12G")
	.set("spark.executor.memory", "2G")
        .set("spark.local.dir", TMPDIR))
sc = SparkContext(conf = conf)

def get_matrix(fn):
    data = sc.textFile(fn)
    mat = data.map(lambda s: map(float, s.split()))
    return mat

def mul_matrix_parts(fn, parts):
    data = sc.textFile(fn, parts)
    mat = data.map(lambda s: map(float, s.split()))
    A_tA = transpose_times(mat)
    res = A_times(mat, A_tA)
    # Because the output matrix is also million times thousand, let's save the matrix into file... Didn't get this to work.
#    res.saveAsTextFile("/cs/work/scratch/humaloja/output_matrix")
    # return count to determine there are 1000000 rows.
    return res.count()

# For each row calculate item squared and sum items -> diagonal
def diag(mat):
    return mat.map(lambda s: reduce (lambda a, b: a+b, map(lambda x: x*x, s))).collect()

# A(^T) x A.
def transpose_times(mat):
    res = mat.mapPartitions(multiply).glom().reduce(lambda mat1, mat2: sum_mats(mat1, mat2))
    return res

# A x A(^T) xA
def A_times(mat, A_tA):
    return mat.mapPartitions(lambda s: multiply_with(s, A_tA)).coalesce(1)
    
# Sum two matrices elementwise.
def sum_mats(mat1, mat2):
    mat1 = list(mat1)
    mat2 = list(mat2)
    return map(lambda row: map(lambda elem: elem[0]+elem[1], zip(row[0], row[1])), zip(mat1, mat2))

def multiply(mat):
    mat = list(mat)
    trans = zip(*mat)
    # Combine two matrices A(^T) and A and set values of both into a tuple. Calculate and reduce the values that make up each elem s.
    return map(lambda row1: map(lambda row2: reduce(add, map(lambda s: s[0]*s[1],zip(row1, row2))), trans), trans)

# Used to multiply A x res, where res = A(^T) x A
def multiply_with(mat, other):
#    A_tA = B.value
    mat = list(mat)
    trans = zip(*other)
    # Combine two matrices A(^T) and A and set values of both into a tuple. Calculate and reduce the values that make up each elem s.
    return map(lambda row1: map(lambda row2: reduce(add, map(lambda s: s[0]*s[1],zip(row1, row2))), trans), mat)

if __name__=="__main__":

    
    print "Diagonal: "
    print diag(get_matrix(DATA2))
    print mul_matrix_parts(DATA2, 1000)

    sys.exit(0)

