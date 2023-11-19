from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").schema("id INT,subject STRING, score INT").load("../data/input/sql/stu_score.txt")
    # df.show()

    # SQL风格
    df.createTempView("score")
    df.createOrReplaceTempView("score1")
    df.createGlobalTempView("score2")

    spark.sql("select subject,count(*) as count from score group by subject").show()
    spark.sql("select subject,count(*) as count from score1 group by subject").show()
    spark.sql("select subject,count(*) as count from global_temp.score2 group by subject").show()
