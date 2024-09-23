from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

# 初始化SparkSession
spark = SparkSession.builder.appName("VideoInteractionStreaming").getOrCreate()

# 定义路径
root_path = "./data"

# 定义Schema
post_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("postTime", TimestampType(), True),
    ]
)

action_schema = StructType(
    [
        StructField("userId", IntegerType(), True),
        StructField("videoId", IntegerType(), True),
        StructField("event", StringType(), True),
        StructField("eventTime", TimestampType(), True),
    ]
)

# 读取数据流
post_stream = (
    spark.readStream.format("csv")
    .option("header", "true")
    .option("path", f"{root_path}/videoPosting")
    .schema(post_schema)
    .load()
)
post_stream_with_watermark = post_stream.withWatermark("postTime", "5 minutes")

action_stream = (
    spark.readStream.format("csv")
    .option("header", "true")
    .option("path", f"{root_path}/interactions")
    .schema(action_schema)
    .load()
)
action_stream_with_watermark = action_stream.withWatermark("eventTime", "5 minutes")

# 进行Join操作
joint_df = action_stream_with_watermark.join(
    post_stream_with_watermark,
    expr(
        """
    videoId = id AND
    eventTime >= postTime AND
    eventTime <= postTime + interval 1 hour
"""
    ),
)

# 写入流到控制台
query = (
    joint_df.writeStream.format("console")
    .option("truncate", False)
    .outputMode("append")
    .start()
)

# 等待查询终止
query.awaitTermination()
