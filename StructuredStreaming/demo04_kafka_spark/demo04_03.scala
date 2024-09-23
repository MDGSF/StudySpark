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
.withColumn("key", $"key".cast(StringType))
.withColumn("value", $"value".cast(StringType))
.groupBy($"key")
.agg(avg($"value").cast(StringType).alias("value"))
.writeStream
.outputMode("Complete")
// 指定Sink为Kafka
.format("kafka")
// 设置Kafka集群信息，本例中只有localhost一个Kafka Broker
.option("kafka.bootstrap.servers", "localhost:9092")
// 指定待写入的Kafka Topic，需事先创建好Topic：cpu-monitor-agg-result
.option("topic", "cpu-monitor-agg-result")
// 指定WAL Checkpoint目录地址
.option("checkpointLocation", "/tmp/checkpoint")
.trigger(Trigger.ProcessingTime(10.seconds))
.start()
.awaitTermination()