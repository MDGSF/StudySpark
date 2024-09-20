from pyspark.sql import SparkSession

# 创建一个 SparkSession 实例
spark = SparkSession.builder.appName("example").getOrCreate()

# 定义数据序列
seq = [("Alice", 18), ("Bob", 14)]

# 将序列转换为 DataFrame
df = spark.createDataFrame(seq, ["name", "age"])

# 创建临时视图
df.createTempView("t1")

# 定义 SQL 查询
query = "SELECT * FROM t1"

# 执行查询并获取结果
result = spark.sql(query)

# 显示结果
result.show()
