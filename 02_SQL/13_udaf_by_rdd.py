import string

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5], 3).map(lambda x: [x])
    df = rdd.toDF(["num"])

    # 利用RDD算子 实现 UDAF（多对一）
    # df.show()

    single_partition_rdd = df.rdd.repartition(1)


    def process(iter):
        sum = 0
        for row in iter:
            sum += row["num"]

        return [sum]


    print(single_partition_rdd.mapPartitions(process).collect())
