import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.VectorAssembler
 
 
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
 

// 待捏合的特征字段集合
val features: Array[String] = Array("LotAreaInt", "GrLivAreaInt", "TotalBsmtSFInt", "GarageAreaInt")
 
// 准备“捏合器”，指定输入特征字段集合，与捏合后的特征向量字段名
val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")
 
// 调用捏合器的transform函数，完成特征向量的捏合
val featuresAdded: DataFrame = assembler.transform(typedFields)
.drop("LotAreaInt")
.drop("GrLivAreaInt")
.drop("TotalBsmtSFInt")
.drop("GarageAreaInt")
 
featuresAdded.printSchema
 