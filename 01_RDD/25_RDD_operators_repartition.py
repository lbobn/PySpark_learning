from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")

    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 24, 1, 23, 5, 345, 34, 5, 6, 465, 4, 325],3)

    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())
    # rdd.persist(StorageLevel.MEMORY_AND_DISK)

    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5).getNumPartitions())
    print(rdd.coalesce(5,shuffle=True).getNumPartitions())
