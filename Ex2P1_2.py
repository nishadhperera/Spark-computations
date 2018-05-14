#!/usr/bin/env python

# Nishadh Aluthge 014562039

import os
import sys
import numpy as np

### Dataset
#DATA1 = '/cs/work/scratch/spark-data/data-2-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-2.txt'

AppName = "Ex-2-P1-2"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg.distributed import RowMatrix

conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max",50)
        .set("spark.local.dir", TMPDIR)
        .set("spark.executor.memory", "2G"))
sc = SparkContext(conf = conf)

# main mathod
if __name__=="__main__":        # main method

        data = sc.textFile(DATA1)
        mat = RowMatrix(data.map(lambda s: s.split(" ")))       # getting the data into a row matrix

        m = mat.numRows()       # calculate the number of rows in the matrix
        n = mat.numCols()       # calculate the number of columns in the matrix

        a = np.fromfile(DATA1, dtype=float, count=-1, sep=" ").reshape((m,n))   # generate array using numpy

        count = np.zeros((m,m))
        final = np.zeros((m,n)) # generating the resulting matrix

        for i in range(0,m-1):  # multiplication of A x A[T]
                for j in range(0,m-1):
                        count[i,j] = np.multiply(a[i,:],a[j,:]).sum()

        for i in range(0,m-1):  # multiplication of (A x A[T]) x A
               for j in range(0,n-1):
                       final[i,j] = np.multiply(count[i,:],a[:,j]).sum()

        sys.exit(0)

