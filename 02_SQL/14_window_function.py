import string

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ("张三", "class_1", 99),
        ("李四", "class_2", 35),
        ("王五", "class_1", 45),
        ("赵六", "class_3", 66),
        ("阿斯克", "class_1", 77),
        ("王鹏", "class_2", 23),
        ("丹姐", "class_3", 45),
        ("阿西", "class_1", 46),
        ("阿才能", "class_1", 88),
        ("丹尼斯", "class_2", 87),
        ("参数", "class_4", 79),
        ("三叉戟", "class_5", 80),
        ("阿九", "class_5", 67),
        ("女警", "class_3", 59),
        ("不吃饱", "class_1", 69),
        ("施斌", "class_2", 68),
        ("茨木", "class_2", 69),
        ("琴行", "class_3", 70),
        ("欸乃", "class_4", 20),
    ])

    schema = StructType().add("name", StringType()).add("class", StringType()).add("score", IntegerType())

    df = rdd.toDF(schema)

    df.createTempView("stu")

    # 聚合窗口函数
    spark.sql("""
        select 
            *,
            avg(score) over() as avg_score 
        from stu
     """).show()

    # 排序窗口
    spark.sql("""
        select 
            *,
            row_number() over (order by score desc) as no_of_all,
            dense_rank() over (partition by class order by score desc ) as no_of_class,
            rank() over (order by score) as rank        
        from stu
        """).show()
