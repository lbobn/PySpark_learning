from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    data_frame = spark.read.csv("../data/input/stu_score.txt", sep=",", header=False)
    df = data_frame.toDF("id", "name", "score")
    df.printSchema()
    df.show()

    df.createTempView("score")

    # SQL 风格
    spark.sql("select * from score where name='语文' limit 5").show()

    # DSL风格
    df.where("name='语文'").limit(5).show()
    