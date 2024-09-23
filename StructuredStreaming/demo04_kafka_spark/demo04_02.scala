import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._
import org.apache.spark.sql.types.StringType
 
 
// 依然是依赖readStream API
val dfCPU:DataFrame = spark.readStream
// format要明确指定Kafka
.format("kafka")
// 指定Kafka集群Broker地址，多个Broker用逗号隔开
.option("kafka.bootstrap.servers", "hostname1:9092,hostname2:9092,hostname3:9092")
// 订阅相关的Topic，这里以cpu-monitor为例
.option("subscribe", "cpu-monitor")
.load()


dfCPU
.withColumn("clientName", $"key".cast(StringType))
.withColumn("cpuUsage", $"value".cast(StringType))
// 按照服务器做分组
.groupBy($"clientName")
// 求取均值
.agg(avg($"cpuUsage").cast(StringType).alias("avgCPUUsage"))
.writeStream
.outputMode("Complete")
// 以Console为Sink
.format("console")
// 每10秒触发一次Micro-batch
.trigger(Trigger.ProcessingTime(10.seconds))
.start()
.awaitTermination()
