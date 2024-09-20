from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler

# 初始化SparkSession
spark = SparkSession.builder.appName("HousePrices").getOrCreate()

# 定义文件路径
root_path = "house-prices-advanced-regression-techniques"
file_path = f"{root_path}/train.csv"

# 从CSV文件创建DataFrame
train_df = spark.read.format("csv").option("header", "true").load(file_path)

# 提取用于训练的特征字段与预测标的（房价SalePrice）
selected_fields = train_df.select(
    "LotArea", "GrLivArea", "TotalBsmtSF", "GarageArea", "SalePrice"
)

# 将所有字段都转换为整型Int
typed_fields = (
    selected_fields.withColumn("LotAreaInt", col("LotArea").cast(IntegerType()))
    .withColumn("GrLivAreaInt", col("GrLivArea").cast(IntegerType()))
    .withColumn("TotalBsmtSFInt", col("TotalBsmtSF").cast(IntegerType()))
    .withColumn("GarageAreaInt", col("GarageArea").cast(IntegerType()))
    .withColumn("SalePriceInt", col("SalePrice").cast(IntegerType()))
    .drop("LotArea", "GrLivArea", "TotalBsmtSF", "GarageArea", "SalePrice")
)

# 打印模式结构
typed_fields.printSchema()


# 待捏合的特征字段集合
features = ["LotAreaInt", "GrLivAreaInt", "TotalBsmtSFInt", "GarageAreaInt"]

# 准备“捏合器”，指定输入特征字段集合与捏合后的特征向量字段名
assembler = VectorAssembler(inputCols=features, outputCol="features")

# 调用捏合器的transform函数，完成特征向量的捏合
features_added = (
    assembler.transform(typed_fields)
    .drop("LotAreaInt")
    .drop("GrLivAreaInt")
    .drop("TotalBsmtSFInt")
    .drop("GarageAreaInt")
)

# 打印模式结构
features_added.printSchema()
