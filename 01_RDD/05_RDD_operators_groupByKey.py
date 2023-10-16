# coding:utf8
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")

    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([('a', 1), ('b', 2), ('a', 2), ('b', 3)])
    result = rdd.groupByKey()
    result_map = result.map(lambda a: (a[0], list(a[1])))
    print(result_map.collect())
