import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

val csvFilePath: String = "test.csv"

val schema:StructType = StructType( Array(
StructField("name", StringType),
StructField("age", IntegerType)
))

val df: DataFrame = spark.read.format("csv").schema(schema).option("header", true).option("mode", "dropMalformed").load(csvFilePath)
// df: org.apache.spark.sql.DataFrame = [name: string, age: int]
df.show
/** 结果打印
+-----+---+
| name| age|
+-----+---+
| alice| 18|
| bob| 14|
+-----+---+
*/