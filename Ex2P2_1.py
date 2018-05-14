#!/usr/bin/env python

# Nishadh Aluthge 014562039

import os
import sys
import numpy as np

### Dataset
#DATA1 = '/cs/work/scratch/spark-data/data-2-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-2.txt'

AppName = "Ex-2-P2-1"
TMPDIR = "/cs/work/scratch/spark-tmp"

### Creat a Spark context on Ukko cluster
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
        .setMaster("spark://ukko080:7077")
        .setAppName(AppName)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true")
        .set("spark.cores.max", 50)
        .set("spark.local.dir", TMPDIR)
        .set("spark.executor.memory", "2G"))
sc = SparkContext(conf = conf)

def RowCol(rw, cl):     # method to multiply row * column
        result = []
        for r in range(0, len(rw)):
                result.append(rw[r]*cl[r])
        return sum(result)

def mul(row, m2, a):    # method to multiply row * matrix 
        r = []
        for x in range(0, len(m2[0])):
                if (a == 1):
                        r.append(RowCol(row, m2[x][:]))
                else:
                        r.append(RowCol(row, m2[:][x]))
        return r

def multiply(m1, m2, a):        # method to multiply matrix * matrix
        x = []

        for i in range(0, len(m1)):
                x.append(mul(m1[i], m2, a))
        return x

# main method
if __name__=="__main__":

        data = sc.textFile(DATA1)

        data = data.map(lambda l: l.split())
        a = data.map(lambda x: [float(f) for f in x]).collect() # matrix craeation from text$

        result = []
        result = multiply(a, a, 1)      # A x A[T]

        for i in range(0, len(a)):		# conditional check to make it a diagonal matrix
                for j in range(0, len(a)):
                        if (i == j):
                                result[i][j] = result[i][j]
                        else:
                                result[i][j] = 0

        sys.exit(0)

