from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql import functions as F

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    # TODO：SQL风格
    rdd = sc.textFile("../data/input/words.txt")
    rdd_2 = rdd.flatMap(lambda x: x.split(" ")).map(lambda x: [x])
    df = rdd_2.toDF(["word"])
    df.createTempView("words")
    spark.sql("select word,count(*) as count from words group by word order by count desc").show()

    # df.show()

    # TODO: DSL风格
    df = spark.read.format("text").load("../data/input/words.txt")

    df2 = df.withColumn("value", F.explode(F.split(df["value"], " ")))
    df2.groupBy("value").count().withColumnRenamed("value", "word").orderBy("count", ascending=False).show()
