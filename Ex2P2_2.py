#!/usr/bin/env python

# Nishadh Aluthge 014562039

import os
import sys
import numpy as np

### Dataset
#DATA1 = '/cs/work/scratch/spark-data/data-2-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-2.txt'

AppName = "Ex-2-P2-2"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg.distributed import RowMatrix

conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 50)
        .set("spark.local.dir", TMPDIR)
        .set("spark.executor.memory", "2G"))
sc = SparkContext(conf = conf)

# main mathod
if __name__=="__main__":

        data = sc.textFile(DATA1)
        mat = RowMatrix(data.map(lambda s: s.split(" ")))       # generating the row matrix from RDD

        m = mat.numRows()       # calculate the number of rows in the matrix
        n = mat.numCols()       # calculate the number of columns in the matrix

        a = np.fromfile(DATA1, dtype=float, count=-1, sep=" ").reshape((m,n))   # generate the matrix in numpy

        count = np.zeros((m,m)) # initiating resulting matrix

        for i in range(0,m-1):  # Calculation for the diagonal of A x A[T}
               for j in range(0,m-1):
                        if (i == j):
                                count[i,j] = np.multiply(a[i,:],a[j,:]).sum()   # value only if (i = j)
                        else:
                                count[i,j] = 0          # zero if (i != j)

        sys.exit(0)




