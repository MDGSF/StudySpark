import org.apache.spark.sql.DataFrame
 
val rootPath: String = "house-prices-advanced-regression-techniques"
val filePath: String = s"${rootPath}/train.csv"
 
// 从CSV文件创建DataFrame
val trainDF: DataFrame = spark.read.format("csv").option("header", true).load(filePath)
 
trainDF.show
trainDF.printSchema