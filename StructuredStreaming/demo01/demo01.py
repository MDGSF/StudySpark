from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
from pyspark.sql.streaming import StreamingQuery

# 创建SparkSession
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

# 设置需要监听的本机地址与端口号
host = "127.0.0.1"
port = "9999"

# 从监听地址创建DataFrame
df = spark.readStream.format("socket").option("host", host).option("port", port).load()

# 使用DataFrame API完成Word Count计算
# 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
# 把数组words展平为单词word
# 以单词word为Key做分组
# 分组计数
df = (
    df.withColumn("words", split(df["value"], " "))
    .withColumn("word", explode("words"))
    .groupBy("word")
    .count()
)

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
