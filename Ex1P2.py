#!/usr/bin/env python

# Nishadh Aluthge 014562039

import os
import sys
import numpy as np

### Dataset
#DATA1 = '/cs/work/scratch/spark-data/data-1-sample.txt'
DATA1 = '/cs/work/scratch/spark-data/data-1.txt'

AppName = "Ex-1-Part2"
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
        data = data.map(lambda s: float(s))
        elements = data.count()

# median index calculation
        if (data.count() % 2) == 1:     # if number of elements is odd 
                medIndex = (data.count()-1) / 2
        else:                           # if number of elements is even
                medIndex = data.count()/2

# data is put into buckets
        data1 = data.map(lambda x: (int(x),1)) \
                        .reduceByKey(lambda x,y:x+y) \
                        .map(lambda x:(x[0],x[1])) \
                        .sortByKey(True) \
                        .collect()

# finding the appropriate bucket which contains the median
        counter = 0
        bucket = 0
        lastcounter = 0
        for x in data1:
                counter = counter + x[1]
                if counter >= medIndex:
                        bucket = x[0]
                        lastcounter = counter - x[1]
                        break

# locating the median value
        sdata = data.filter(lambda s: (((s-bucket)<1) and ((s-bucket)>0)))
        c = sdata.count()
        sdata = sdata.takeOrdered(medIndex-lastcounter+1)
        if (elements % 2) == 1:
                med1 = sdata[len(sdata)-1]
                med2 = sdata[len(sdata)-1]
        else:
                med1 = sdata[len(sdata)-1]
                med2 = sdata[len(sdata)-2]
        median = (med1+med2)/2

# printing the results
        print "-------------------------------------------"
        print "Median = %.8f " % median
        print "-------------------------------------------"

        sys.exit(0)

