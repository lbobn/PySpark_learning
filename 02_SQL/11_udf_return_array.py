from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([["hadoop spark flink"], ["hdfs hadoop mapreduce"]])
    df = rdd.toDF(["line"])


    # 数组返回类型的UDF
    def split_line(line):
        return line.split(" ")


    # TODO 1: 方式一
    udf = spark.udf.register("udf", split_line, ArrayType(StringType()))
    df.createTempView("lines")
    spark.sql("select udf(line) from lines").show(truncate=False)  # SQL
    df.select(udf(df["line"])).show(truncate=False)  # DSL

    # TODO 2 : 方式2
    udf2 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf2(df["line"])).show(truncate=False)
