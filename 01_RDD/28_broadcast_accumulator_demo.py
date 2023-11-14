from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
import re

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    # TODO : 找到RDD中特殊字符个数和正常单词个数,使用广播变量和累加器完成
    rdd = sc.textFile("../data/input/accumulator_broadcast_data.txt")


    # 特殊字符
    abnormal_char = [",", ".", "!", "#", "$", "%"]
    # 设置累加器；将特殊字符包装为广播变量
    abnormal_num = sc.accumulator(0)
    broadcast = sc.broadcast(abnormal_char)

    # 过滤空行，并对每行切割得到单词
    lineRdd = rdd.filter(lambda line: line.strip())
    words_rdd = lineRdd.map(lambda line: line.strip())
    w = words_rdd.flatMap(lambda line: re.split("\s+", line))

    # print(w.collect())

    def func(data):
        global abnormal_num
        chars = broadcast.value
        if data in chars:
            abnormal_num += 1
            return False
        else:
            return True

    # 过滤特殊字符并对其计数，再对单词计数
    result = w.filter(func)
    rs = result.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)

    print("单词计数结果", rs.collect())
    print("特殊字符个数", abnormal_num)
