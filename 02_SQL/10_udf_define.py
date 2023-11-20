from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9]).map(lambda x: [x])
    df = rdd.toDF(["num"])


    def num_ride_10(num):
        return num * 10


    # TODO 1: 定义方式1
    udf1 = spark.udf.register("udf1", num_ride_10, IntegerType())
    df.selectExpr("udf1(num)").show()  # SQL风格
    df.select(udf1(df["num"])).show()  # DSL风格

    # TODO 2： 定义方式2 只用于DSL风格
    udf3 = F.udf(num_ride_10, IntegerType())
    df.select(udf3(df["num"])).show()  # DSL风格
