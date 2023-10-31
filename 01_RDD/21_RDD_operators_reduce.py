from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)
    rdd = sc.parallelize([1, 2, 3, 4, 5])

    print(rdd.reduce(lambda x, y: x + y))
