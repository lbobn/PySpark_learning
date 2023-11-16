from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile("../data/input/sql/people.txt")

    rdd1 = rdd.map(lambda line: line.split(",")).map(lambda x: [x[0], int(x[1])])

    # 构建方式1
    df = spark.createDataFrame(rdd1, schema=["name", "age"])

    # 打印表结构
    df.printSchema()
    df.show(20,False)


    # 生成临时视图，为使用SQL
    df.createTempView("stu")
    spark.sql("select * from stu where age<30").show()