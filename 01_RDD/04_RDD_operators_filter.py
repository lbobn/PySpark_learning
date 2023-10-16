# coding:utf8
from pyspark import SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8])
    rdd_filter = rdd.filter(lambda x: x % 2 == 1)

    print(rdd_filter.collect())
