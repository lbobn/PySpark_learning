# coding:utf8
import json

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../data/input/order.txt")
    json_rdd = rdd.flatMap(lambda line: line.split("|"))
    dict_rdd = json_rdd.map(lambda x: json.loads(x))

    rdd_beijing = dict_rdd.filter(lambda x: x["areaName"] == '北京')
    category_rdd = rdd_beijing.map(lambda x: ('北京', x["category"])).distinct()
    print(category_rdd.collect())
