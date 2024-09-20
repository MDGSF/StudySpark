from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession

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

employeesDF.selectExpr("id", "name", "concat(id, '_', name) as id_name").show()

# 输出：
# +---+-------+---------+
# | id|   name|  id_name|
# +---+-------+---------+
# |  1|   John|   1_John|
# |  2|   Lily|   2_Lily|
# |  3|Raymond|3_Raymond|
# +---+-------+---------+
