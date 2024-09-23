from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window
from pyspark.sql.streaming import StreamingQuery

# 创建SparkSession
spark = SparkSession.builder.appName("StreamingJoinExample").getOrCreate()

# 设置根路径
root_path = "./data"

# 读取静态DataFrame
static_df = (
    spark.read.format("csv")
    .option("header", "true")
    .load(f"{root_path}/userProfile/userProfile.csv")
)

# 定义流式数据的Schema
action_schema = "userId INT, videoId INT, event STRING, eventTime TIMESTAMP"

# 读取流式数据
streaming_df = (
    spark.readStream.format("csv")
    .option("header", "true")
    .schema(action_schema)
    .load(f"{root_path}/interactions/*")
)

# 使用watermark和窗口操作
streaming_df = (
    streaming_df.withWatermark("eventTime", "30 minutes")
    .groupBy(window(col("eventTime"), "1 hours"), col("userId"), col("event"))
    .count()
)

# 进行join操作
# 如果需要使用广播变量来优化，可以取消注释下面两行
# from pyspark.sql.functions import broadcast
# joint_df = streaming_df.join(broadcast(static_df), streaming_df["userId"] == static_df["id"])
joint_df = streaming_df.join(static_df, streaming_df["userId"] == static_df["id"])

# 写入流到控制台
query: StreamingQuery = (
    joint_df.writeStream.format("console")
    .option("truncate", "false")
    .outputMode("update")
    .start()
)

# 等待查询终止
query.awaitTermination()
