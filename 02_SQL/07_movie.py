from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    schema = StructType().add("user_id", StringType(), nullable=True) \
        .add("movie_id", StringType(), nullable=True) \
        .add("rank", IntegerType(), nullable=True) \
        .add("ts", StringType(), nullable=True)
    # 读取数据
    df = spark.read.format("csv").option("sep", "\t").option("header", False).option("encoding", "utf-8").schema(
        schema=schema).load("../data/input/sql/u.data")

    # TODO 1 :
