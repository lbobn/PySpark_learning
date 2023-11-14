from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    stu_info = [
        (1, "张大仙", 11),
        (2, "李四", 12),
        (3, "王五", 13),
        (4, "赵六", 14)]

    # 设置为广播变量
    broadcast = sc.broadcast(stu_info)

    score_info_rdd = sc.parallelize([
        (1, "语文", 88),
        (2, "数学", 88),
        (3, "英语", 88),
        (4, "语文", 88),
        (1, "python", 88),
        (2, "语文", 88),
        (3, "java", 88),
        (4, "英语", 88),
        (1, "语文", 88),
        (3, "数学", 88),
        (2, "物理", 88),
    ])


    def map_func(data):
        id = data[0]
        name = ''
        for stu in broadcast.value:
            if id == stu[0]:
                name = stu[1]

        return (name, data[1], data[2])


    print(score_info_rdd.map(map_func).collect())

