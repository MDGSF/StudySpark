from pyspark.sql import SparkSession
from pyspark.sql.functions import split, element_at, col, window, explode
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import TimestampType


# 创建SparkSession
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# 设置需要监听的本机地址与端口号
host = "127.0.0.1"
port = "9999"

# 从监听地址创建DataFrame
df = spark.readStream.format("socket").option("host", host).option("port", port).load()


# 分割"value"列并创建新的"inputs"列
df = df.withColumn("inputs", split(col("value"), ","))

# 提取事件时间，并将其转换为timestamp类型
df = df.withColumn("eventTime", element_at(col("inputs"), 1).cast(TimestampType()))

# 提取单词序列
df = df.withColumn("words", split(element_at(col("inputs"), 2), " "))

# 拆分单词
df = df.withColumn("word", explode(col("words")))

# 按照Tumbling Window与单词做分组，并统计计数
df = df.groupBy(
    window(col("eventTime"), "5 minutes").alias("time_window"), col("word")
).count()


# 将Word Count结果写入到终端（Console）
# outputMode: complete 或 update
query: StreamingQuery = (
    df.writeStream.format("console")
    .option("truncate", False)
    .outputMode("update")
    .start()
)

# 等待中断指令
query.awaitTermination()
