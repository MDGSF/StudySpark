from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# 创建一个 SparkSession 对象
spark = SparkSession.builder.appName("KafkaStreamingCPUAggregator").getOrCreate()

# 读取 Kafka 流数据
df_cpu = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "hostname1:9092,hostname2:9092,hostname3:9092")
    .option("subscribe", "cpu-monitor")
    .load()
)

# 处理数据：转换 key 和 value 为字符串类型，并按 key 聚合计算平均值
df_cpu_aggregated = (
    df_cpu.withColumn("key", col("key").cast("string"))
    .withColumn("value", col("value").cast("string"))
    .groupBy("key")
    .agg(avg("value").cast("string").alias("value"))
)

# 写入 Kafka 流
query = (
    df_cpu_aggregated.writeStream.outputMode("complete")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "cpu-monitor-agg-result")
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)

# 等待查询终止
query.awaitTermination()
