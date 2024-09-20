import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

 
val rootPath: String = "house-prices-advanced-regression-techniques"
val filePath: String = s"${rootPath}/train.csv"
 
// 从CSV文件创建DataFrame
val trainDF: DataFrame = spark.read.format("csv").option("header", true).load(filePath)
 
// 提取用于训练的特征字段与预测标的（房价SalePrice）
val selectedFields: DataFrame = trainDF.select("LotArea", "GrLivArea", "TotalBsmtSF", "GarageArea", "SalePrice")
 
// 将所有字段都转换为整型Int
val typedFields = selectedFields
  .withColumn("LotAreaInt",col("LotArea").cast(IntegerType)).drop("LotArea")
  .withColumn("GrLivAreaInt",col("GrLivArea").cast(IntegerType)).drop("GrLivArea")
  .withColumn("TotalBsmtSFInt",col("TotalBsmtSF").cast(IntegerType)).drop("TotalBsmtSF")
  .withColumn("GarageAreaInt",col("GarageArea").cast(IntegerType)).drop("GarageArea")
  .withColumn("SalePriceInt",col("SalePrice").cast(IntegerType)).drop("SalePrice")
 
typedFields.printSchema
 