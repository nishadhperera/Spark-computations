#!/usr/bin/env python

# Nishadh Aluthge 014562039

import os
import sys

### Dataset
#DATA1 = '/cs/work/scratch/spark-data/data-1-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'

AppName = "Ex-1-Part1"
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
        .set("spark.executor.memory", "1G"))
sc = SparkContext(conf = conf)

### defining methods for calculations.

def findMax(data):      # method for finding maximum value
        data = data.takeOrdered(5, key=lambda x: -x)    # take the largest 5 nu$
        max = data[0]   # take the 0th index number as the maximum
        return max

def findMin(data):      # method for finding minimum value
        data = data.takeOrdered(5)      # take the smallest 5 numbers after sor$
        min = data[0]   # take the 0th index number as minimum
        return min

def findAvg(data):      # method for finding average value
        count = data.count()    # count of all numbers
        Ctotal = data.sum()     # sum of all numbers
        avg = Ctotal / count
        return avg

def findVar(data, avg): # method for finding variance value
        tot = 0
        count = data.count()
        data = data.map(lambda x: (x-avg)**2)   # difference of x and avg
        tot = data.sum()        # sum of difference
        var = tot / count

        return var

if __name__=="__main__":

        data = sc.textFile(DATA1)
        data = data.map(lambda s: float(s))

        calMax = findMax(data)
        calMin = findMin(data)
        calAvg = findAvg(data)
        calVar = findVar(data, calAvg)

        print "-------------------------------------------------------"
        print "Calculated Max. = %.8f" % calMax
        print "Calculated Min. = %.8f" % calMin
        print "Calculated Avg. = %.8f" % calAvg
        print "Calculated Var. = %.8f" % calVar
        print "-------------------------------------------------------"

        sys.exit(0)

