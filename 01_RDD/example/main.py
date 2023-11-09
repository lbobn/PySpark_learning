from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from operator import *
import jieba
from defs import *

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("test")
    sc = SparkContext(conf=conf)

    log_rdd = sc.textFile("../../data/input/SogouQ.txt")

    basic_rdd = log_rdd.map(lambda x: x.split("\t"))
    # 多次使用，设置缓存
    basic_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO : 需求1 用户搜索关键词前5

    str_rdd = basic_rdd.map(lambda x: x[2])
    words_rdd = str_rdd.flatMap(context_jieba)
    """
        院校 -> 帮
        博学 -> 博学谷
        传智播 —> 传智播客
    """
    rdd_filter = words_rdd.filter(filter_word)
    final_rdd = rdd_filter.map(append_word)
    result1 = final_rdd.reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: x[1], ascending=False, numPartitions=1) \
        .take(5)
    print("需求1：", result1)

    # TODO: 需求2 用户和关键词组合分析
    user_key_rdd = basic_rdd.map(lambda x: (x[1], x[2]))
    # 分词，并与用户id组合
    user_word_rdd = user_key_rdd.flatMap(getUserWord)
    # 对内容排序，取前5
    result2 = user_word_rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False,
                                                                   numPartitions=1).take(5)
    print("需求2：", result2)

    # TODO: 需求3 热门搜索时间段分析
    # 取出所有时间
    time_rdd = basic_rdd.map(lambda x: x[0])
    # 保留小时
    hour_rdd = time_rdd.map(lambda x: (x.split(":")[0], 1))
    # 分组排序聚合
    result3 = hour_rdd.reduceByKey(add) \
        .sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print("需求3:", result3)
