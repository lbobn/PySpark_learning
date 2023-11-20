import string

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([[2], [3], [4]])
    df = rdd.toDF(["num"])


    # 返回字典类型
    def getChar(data):
        return {"num": data, "char": string.ascii_letters[data]}


    udf = spark.udf.register("udf", getChar,
                             StructType().add("num", IntegerType(), nullable=True).add("char", StringType(),
                                                                                       nullable=True))

    df.selectExpr("udf(num)").show(truncate=False)
    df.select(udf(df["num"])).show(truncate=False)
