import org.apache.spark.sql.DataFrame
 
// 设置需要监听的本机地址与端口号
val host: String = "127.0.0.1"
val port: String = "9999"

// 从监听地址创建DataFrame
var df: DataFrame = spark.readStream
.format("socket")
.option("host", host)
.option("port", port)
.load()

/**
使用DataFrame API完成Word Count计算
*/
 
// 首先把接收到的字符串，以空格为分隔符做拆分，得到单词数组words
df = df.withColumn("words", split($"value", " "))
 
// 把数组words展平为单词word
.withColumn("word", explode($"words"))
 
// 以单词word为Key做分组
.groupBy("word")
 
// 分组计数
.count()



/**
将Word Count结果写入到终端（Console）
*/
 
df.writeStream
// 指定Sink为终端（Console）
.format("console")
 
// 指定输出选项
.option("truncate", false)
 
// 指定输出模式
.outputMode("complete")
//.outputMode("update")
 
// 启动流处理应用
.start()
// 等待中断指令
.awaitTermination()

