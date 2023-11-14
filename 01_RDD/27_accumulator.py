from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    count = sc.accumulator(0)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3)

    def func(data):
        global count
        count += 1
        print(count)
    # 累加器计算所有RDD分区元素个数
    rdd.map(func).collect()
    print(count)

