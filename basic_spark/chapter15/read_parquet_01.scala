val parquetFilePath: String = _
val df: DataFrame = spark.read.format("parquet").load(parquetFilePath)