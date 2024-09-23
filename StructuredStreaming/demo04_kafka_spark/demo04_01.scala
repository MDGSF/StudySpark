import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._
 
 
// 依然是依赖readStream API
val dfCPU:DataFrame = spark.readStream
// format要明确指定Kafka
.format("kafka")
// 指定Kafka集群Broker地址，多个Broker用逗号隔开
.option("kafka.bootstrap.servers", "hostname1:9092,hostname2:9092,hostname3:9092")
// 订阅相关的Topic，这里以cpu-monitor为例
.option("subscribe", "cpu-monitor")
.load()


dfCPU.writeStream
.outputMode("Complete")
// 以Console为Sink
.format("console")
// 每10秒钟，触发一次Micro-batch
.trigger(Trigger.ProcessingTime(10.seconds))
.start()
.awaitTermination()
