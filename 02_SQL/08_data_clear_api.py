from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    df = spark.read.format("csv").option("sep", ";").option("header", True).load("../data/input/sql/people.csv")

    # 去重
    df.dropDuplicates().show()
    df.dropDuplicates(['age', 'job']).show()  # 对age和Job去重

    # 数据清洗 ： 缺失值
    df.dropna().show()
    df.dropna(thresh=3).show()  # 至少3个有效列
    df.dropna(thresh=2, subset=["name", "age"]).show()  # 对于name，age列至少2个有效列

    # 填充
    df.fillna("N/A").show()
    df.fillna("N/A", subset=["job"]).show()
    df.fillna({"name": "未知姓名", "age": 1, "job": "worker"}).show()
