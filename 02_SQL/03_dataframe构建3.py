from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile("../data/input/sql/people.txt")

    rdd1 = rdd.map(lambda line: line.split(",")).map(lambda x: [x[0], int(x[1])])

    # 构建方式3
    schema = StructType().add("name", StringType(), True).add("age", IntegerType(), True)

    df = rdd1.toDF(schema=schema)
    # df = rdd1.toDF(["name","age"])

    # 打印表结构
    df.printSchema()
    df.show(20, False)
