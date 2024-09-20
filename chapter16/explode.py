from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col


# 创建一个 SparkSession 实例
spark = SparkSession.builder.appName("example").getOrCreate()

# 定义员工数据序列，包含兴趣列表
seq = [
    (1, "John", 26, "Male", ["Sports", "News"]),
    (2, "Lily", 28, "Female", ["Shopping", "Reading"]),
    (3, "Raymond", 30, "Male", ["Sports", "Reading"]),
]

# 将序列转换为 DataFrame，并指定列名
employeesDF = spark.createDataFrame(seq, ["id", "name", "age", "gender", "interests"])

# 显示原始 DataFrame
employeesDF.show()

# 使用 explode 函数展开 'interests' 列，并创建新列 'interest'
employeesDF.withColumn("interest", explode(col("interests"))).show()

employeesDF.withColumn("interest", explode(col("interests"))).drop("interests").show()
