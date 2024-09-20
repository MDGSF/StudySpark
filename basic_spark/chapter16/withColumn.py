from pyspark.sql import SparkSession
from pyspark.sql.functions import hash, col


# 创建一个 SparkSession 实例
spark = SparkSession.builder.appName("example").getOrCreate()

# 定义员工数据序列
employees = [
    (1, "John", 26, "Male"),
    (2, "Lily", 28, "Female"),
    (3, "Raymond", 30, "Male"),
]

# 将序列转换为 DataFrame，并指定列名
employeesDF = spark.createDataFrame(employees, ["id", "name", "age", "gender"])

employeesDF.withColumn("crypto", hash(col("age"))).show()

# 输出:
# +---+-------+---+------+----------+
# | id|   name|age|gender|    crypto|
# +---+-------+---+------+----------+
# |  1|   John| 26|  Male|1221056600|
# |  2|   Lily| 28|Female|-964985136|
# |  3|Raymond| 30|  Male|1497690768|
# +---+-------+---+------+----------+
