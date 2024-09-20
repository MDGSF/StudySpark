from pyspark.sql import SparkSession

# 创建一个 SparkSession 实例
spark = SparkSession.builder.appName("example").getOrCreate()

# 创建员工信息表
seq = [
    (1, "Mike", 28, "Male"),
    (2, "Lily", 30, "Female"),
    (3, "Raymond", 26, "Male"),
    (5, "Dave", 36, "Male"),
]
employees = spark.createDataFrame(seq, ["id", "name", "age", "gender"])

# 创建薪资表
seq2 = [(1, 26000), (2, 30000), (4, 25000), (3, 20000)]
salaries = spark.createDataFrame(seq2, ["id", "salary"])

# 内连接薪资表和员工信息表
jointDF = salaries.join(employees, salaries["id"] == employees["id"], "inner")

# 显示薪资表
salaries.show()

# 显示员工信息表
employees.show()

# 显示连接后的 DataFrame
jointDF.show()
