# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 13:44:10 2020

@author: Window-Genoveffa
"""


from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///SparkCourse/customer-orders.csv")
priceCount = input.map(parseLine)

priceCounts = priceCount.reduceByKey(lambda x, y: x + y)
priceCountsSorted = priceCounts.map(lambda x: (x[1], x[0])).sortByKey()
#Changed for Python 3 compatibility:
#flipped = totalByCustomer.map(lambda (x,y):(y,x))
result = priceCountsSorted.collect();

for result in result:
    count = float(result[0])
    custID = str(result[1])
    print(custID + ":\t\t" + ("$ {:.2f}".format(count)))
