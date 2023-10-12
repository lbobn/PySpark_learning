# encoding:utf8

from pyspark import SparkConf,SparkContext
# import os


conf = SparkConf().setAppName("WordCount")

sc = SparkContext(conf=conf)

words = sc.textFile("../data/words.txt").flatMap(lambda line: line.split(" "))

collect = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()

print(collect)
