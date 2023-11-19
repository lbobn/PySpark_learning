from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").schema("id INT,subject STRING, score INT").load("../data/input/sql/stu_score.txt")
    # df.show()

    # DSL风格
    # select
    df.select("id", "subject").show()
    df.select(["id", "subject"]).show()
    # 获取Column对象
    id_col = df['id']
    subject_col = df["subject"]
    df.select(id_col, subject_col).show()

    # filter 过滤
    # df.filter("score < 99").show()
    # df.filter(df['score'] < 99).show()

    # where
    df.where("score < 99").show()
    df.where(df['score'] < 99).show()

    # group by
    """
    返回值为GroupedData,需要聚合函数后转为 dataFrame
    """
    df.groupBy("subject").count().show()
    df.groupBy(df["subject"]).count().show()
