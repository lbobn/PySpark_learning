from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")

    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 3, 2, 5, 1, 8, 6, 4])
    print(rdd.takeOrdered(5))
    print(rdd.takeOrdered(5, lambda x: -x))
    # sample = rdd.takeSample(False, 5)
    # print(sample)
