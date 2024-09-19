# StudySpark

- <https://github.com/wulei-bj-cn/learn-spark>
- spark on k8s: <https://spark.apache.org/docs/latest/running-on-kubernetes.html>
- rdd: <https://spark.apache.org/docs/latest/rdd-programming-guide.html>
- spark configuration: <https://spark.apache.org/docs/latest/configuration.html>
- <https://sparkbyexamples.com/>
- <https://github.com/spark-examples/pyspark-examples>
- <https://github.com/jadianes/spark-py-notebooks>

```sh
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark

# 该镜像是基于 ubuntu22.04 的
docker run -it --rm spark:3.5.2-scala2.12-java17-python3-ubuntu bash
docker run -it --rm spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/spark-shell
docker run -it --rm spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/pyspark

docker run -it --rm \
  -v $(pwd):/opt/spark/work-dir \
  spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/spark-shell

docker run -it --rm \
  -v $(pwd):/opt/spark/work-dir \
  spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/pyspark

docker run -it --rm \
  -v $(pwd):/opt/spark/work-dir \
  spark:3.5.2-scala2.12-java17-python3-ubuntu bash -c \
  "/opt/spark/bin/spark-submit count_word.py"


docker run -it --rm \
  -v $(pwd):/opt/spark/work-dir \
  spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/pyspark \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g
```

## notes

- spark-shell 用于启动交互式的分布式运行环境，
- spark-submit 则用于向 Spark 计算集群提交分布式作业。

在 Spark 中，创建 RDD 的典型方式有两种：

- 通过 SparkContext.parallelize 在内部数据之上创建 RDD；
- 通过 SparkContext.textFile 等 API 从外部数据创建 RDD。

```sh
spark-shell --master local[*]
```

- local 关键字表示部署模式为 Local，也就是本地部署；
- 方括号里面的数字表示的是在本地部署中需要启动多少个 Executors，星号则意味着这个数量与机器中可用 CPU 的个数相一致。

Shuffle 的本意是扑克牌中的“洗牌”，在大数据领域的引申义，表示的是集群范围内跨进程、跨节点的数据交换。

```sh
# 运行 spark 自带的测试例子
$SPARK_HOME/bin/run-example org.apache.spark.examples.SparkPi
MASTER=local $SPARK_HOME/bin/run-example org.apache.spark.examples.SparkPi
MASTER=spark://node0:7077 $SPARK_HOME/bin/run-example org.apache.spark.examples.SparkPi
```

- “–master spark://ip:host”就代表 Standalone 部署模式
- “–master yarn”就代表 YARN 模式

用一句话来概括从 DAG 到 Stages 的拆分过程，那就是：以 Actions 算子为起点，从后向前回溯 DAG，以 Shuffle 操作为边界去划分 Stages。

### 修改配置

对于这 3 种方式，Spark 会按照“SparkConf 对象 -> 命令行参数 -> 配置文件”的顺序，依次读取配置项的参数值。对于重复设置的配置项，Spark 以前面的参数取值为准。

#### 修改默认配置文件

`spark-defaults.conf`

```text
spark.executor.cores 2
spark.executor.memory 4
gspark.local.dir /ssd_fs/large_dir
```

#### 修改命令行参数

```sh
spark-shell \
  --master local[*] \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=4g \
  --conf spark.local.dir=/ssd_fs/large_dir
```

#### SparkConf 对象

```scala
import org.apache.spark.SparkConf
val conf = new SparkConf()
conf.set("spark.executor.cores", "2")
conf.set("spark.executor.memory", "4g")
conf.set("spark.local.dir", "/ssd_fs/large_dir")   
```

### Spark SQL

- SparkContext 通过 textFile API 把源数据转换为 RDD
- SparkSession 通过 read API 把源数据转换为 DataFrame
- DataFrame 是一种带 Schema 的分布式数据集
- DataFrame 背后的计算引擎是 Spark SQL
- RDD 的计算引擎是 Spark Core

### sparkSession read

```scala
sparkSession.read.format("文件格式").option("key", "value").load("文件路径")
```

- 文件格式: CSV（Comma Separated Values）、Text、Parquet、ORC、JSON。Spark SQL
- `option(选项 1, 值 1).option(选项 2, 值 2)`
- 文件路径
  - 本地文件系统中的“/dataSources/wikiOfSpark.txt”，
  - HDFS 分布式文件系统中的“hdfs://hostname:port/myFiles/userProfiles.csv”，
  - Amazon S3 上的“s3://myBucket/myProject/myFiles/results.parquet”

### 数据处理生命周期

- 数据加载
  - parallelize
  - textFile
- 数据准备
  - union
  - sample
- 数据预处理
  - coalesce
  - repartition
- 数据处理
  - map
  - flatMap
  - filter
  - sortByKey
  - reduceByKey
  - aggregateByKey
- 结果收集
  - take
  - first
  - collect
  - saveAsTextFile

### 常用 RDD 算子

- map、mapPartitions、flatMap 和 filter
  - 不会引入 shuffle
  - 使用范围：任意 RDD
- groupByKey、reduceByKey、aggregateByKey 和 sortByKey
  - 会引入 shuffle，会有性能隐患
  - 使用范围：Paired RDD

#### toDebugString

```python
rdd.toDebugString()
```

#### map

#### mapPartitions

#### flatMap

#### filter

#### groupByKey

较少使用，资源消耗大。groupByKey 是无参算子。以全量原始数据记录在集群范围内进行落盘与网络分发，会带来巨大的性能开销。

#### reduceByKey

reduceByKey 算子的局限性，在于其 Map 阶段与 Reduce 阶段的计算逻辑必须保持一致，这个计算逻辑统一由聚合函数 f 定义。

#### aggregateByKey

aggregateByKey 算子，Map 阶段与 Reduce 阶段的计算逻辑可以不同。

```scala
val rdd: RDD[(Key类型，Value类型)] = _
rdd.aggregateByKey(初始值)(f1, f2)
```

- 初始值类型，必须与 f2 的结果类型保持一致；
- f1 的形参类型，必须与 Paired RDD 的 Value 类型保持一致；
- f2 的形参类型，必须与 f1 的结果类型保持一致。

#### sortByKey

按照 Key 进行排序

- 升序排序：调用 sortByKey()、或者 sortByKey(true)；
- 降序排序：调用 sortByKey(false)。

#### union

它常常用于把两个类型一致、但来源不同的 RDD 进行合并

```scala
rdd1.union(rdd2)
```

#### sample

```scala
sample(withReplacement, fraction, seed)
```

- withReplacement 的类型是 Boolean，它的含义是“采样是否有放回”，如果这个参数的值是 true，那么采样结果中可能会包含重复的数据记录，相反，如果该值为 false，那么采样结果不存在重复记录。
- fraction 它的类型是 Double，值域为 0 到 1，其含义是采样比例，也就是结果集与原数据集的尺寸比例。
- seed 参数是可选的，用于控制每次采样的结果是否一致。

#### repartition

可以使用 repartition 算子随意调整（提升或降低）RDD 的并行度，而 coalesce 算子则只能用于降低 RDD 并行度。

结合经验来说，把并行度设置为可用 CPU 的 2 到 3 倍，往往是个不错的开始。

这个算子有个致命的弊端，那就是它会引入 Shuffle。

#### coalesce

coalesce 算子则只能用于降低 RDD 并行度。

coalesce 用法和 repartition 一样，但是不会引入 Shuffle。

#### first

#### take

#### collect

collect 算子有两处性能隐患，一个是拉取数据过程中引入的网络开销，另一个 Driver 的 OOM（内存溢出，Out of Memory）

#### saveAsTextFile

```scala
saveAsTextFile(path: String)
```

其中 path 代表的是目标文件系统目录，它可以是本地文件系统，也可以是 HDFS、Amazon S3 等分布式文件系统。

### 共享变量

Spark 提供了两类共享变量，分别是广播变量（Broadcast variables）和累加器（Accumulators）

#### 广播变量（Broadcast variables）

在 Driver 与 Executors 之间，普通变量的分发与存储，是以 Task 为粒度的，因此，它所引入的网络与内存开销，会成为作业执行性能的一大隐患。

在使用广播变量的情况下，数据内容的分发粒度变为以 Executors 为单位。

当你遇到需要多个 Task 共享同一个大型变量（如列表、数组、映射等数据结构）的时候，就可以考虑使用广播变量来优化你的 Spark 作业。

#### 累加器（Accumulators）

主要作用是全局计数（Global counter）

- longAccumulator 定义Long类型的累加器
- doubleAccumulator 用于对 Double 类型的数值做全局计数；
- 而 collectionAccumulator 允许开发者定义集合类型的累加器

就这 3 种累加器来说，尽管类型不同，但它们的用法是完全一致的。都是先定义累加器变量，然后在 RDD 算子中调用 add 函数，从而更新累加器状态，最后通过调用 value 函数来获取累加器的最终结果。
