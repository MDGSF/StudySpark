from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, desc, asc, col

# 创建一个 SparkSession 实例
spark = SparkSession.builder.appName("example").getOrCreate()

# 创建员工信息表
seq = [(1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male")]
employees = spark.createDataFrame(seq, ["id", "name", "age", "gender"])

# 创建薪水表
seq2 = [(1, 26000), (2, 30000), (4, 25000), (3, 20000)]
salaries = spark.createDataFrame(seq2, ["id", "salary"])

# 显示员工信息表
employees.show()

# 显示薪水表
salaries.show()

# 内连接员工信息表和薪水表
jointDF = salaries.join(employees, on="id", how="inner")

# 显示连接后的 DataFrame
jointDF.show()

# 按性别分组，计算总薪水和平均薪水
aggResult = jointDF.groupBy("gender").agg(
    sum("salary").alias("sum_salary"), avg("salary").alias("avg_salary")
)

# 显示聚合结果
aggResult.show()

# 按总薪水降序、性别升序排序并显示
aggResult.sort(desc("sum_salary"), asc("gender")).show()

# 或者使用 orderBy 进行排序
aggResult.orderBy(desc("sum_salary"), asc("gender")).show()
