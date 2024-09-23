from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# 创建一个 SparkSession 对象
spark = SparkSession.builder.appName("KafkaStreamingCPUAggregator").getOrCreate()

# 读取 Kafka 流数据，并添加 SCRAM-SHA-256 认证选项
df_cpu = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka_url1:9092,kafka_url2:9092,kafka_url3:9092",
    )
    .option("subscribe", "test-cpu-monitor")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="xxxxx" password="xxxxx";',
    )
    .load()
)

# 处理数据：转换 key 和 value 为字符串类型，并按 key 聚合计算平均值
df_cpu_aggregated = (
    df_cpu.withColumn("key", col("key").cast("string"))
    .withColumn("value", col("value").cast("string"))
    .groupBy("key")
    .agg(avg("value").cast("string").alias("value"))
)

# 写入 Kafka 流，并添加 SCRAM-SHA-256 认证选项
query = (
    df_cpu_aggregated.writeStream.outputMode("complete")
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka_url1:9092",
    )
    .option("topic", "test-cpu-monitor-agg-result")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="xxxxx" password="xxxxx";',
    )
    .option("checkpointLocation", "/tmp/checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)

# 等待查询终止
query.awaitTermination()
