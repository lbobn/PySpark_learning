from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("../../data/input/apache.log")
    basic_rdd = rdd.map(lambda x: x.split(" "))
    basic_rdd.persist(storageLevel=StorageLevel.MEMORY_ONLY)

    # TODO: 计算当前网站PV    （被访问次数）
    print("访问次数", basic_rdd.count())
    # TODO: 当前访问的UV     （访问的用户数）
    print("访问用户数", basic_rdd.map(lambda x: x[1]).distinct().count())
    # TODO: 有哪些ip访问
    print("访问的ip", basic_rdd.map(lambda x: x[0]).distinct().collect())
    # TODO: 哪个页面访问量高
    url_rdd = basic_rdd.map(lambda x: (x[4], 1))
    # print(url_rdd.collect())
    result_rdd = url_rdd.reduceByKey(lambda x, y: x + y)
    result = result_rdd.sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(1)
    # result = result_rdd.max(lambda x:x[1])
    print("访问最多的页面", result)
