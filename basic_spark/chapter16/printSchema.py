from pyspark.sql import SparkSession

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

# 打印 DataFrame 的 schema 信息
employeesDF.printSchema()
