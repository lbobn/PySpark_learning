from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")

    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 24, 1, 23, 5, 345, 34, 5, 6, 465, 4, 325],3)

    def func(partList):
        result = []
        for i in partList:
            result.append(i * 10)

        return result


    collect = rdd.mapPartitions(func).collect()
    print(collect)
