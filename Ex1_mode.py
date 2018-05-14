#!/usr/bin/env python

# Nishadh Aluthge 014562039

import os
import sys
import numpy as np

### Dataset
#DATA1 = '/cs/work/scratch/spark-data/data-1-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'

AppName = "Ex-1-Mode"
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

### defining methods for calculations.

if __name__=="__main__":

        data = sc.textFile(DATA1)

        data1 = data.map(lambda x: (float(x),1)) \
                        .reduceByKey(lambda x,y:x+y) \
                        .map(lambda x:(x[0],x[1])) \
                        .sortByKey(False).first()

        print "-------------------------------------------"
        print "Calculated Mode"
        print data1[0]
        print "-------------------------------------------"

        sys.exit(0)

