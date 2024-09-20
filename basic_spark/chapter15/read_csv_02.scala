import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

val schema:StructType = StructType( Array(
StructField("name", StringType),
StructField("age", IntegerType)
))

val csvFilePath: String = _
val df: DataFrame = spark.read.format("csv").schema(schema).option("header", true).load(csvFilePath)
