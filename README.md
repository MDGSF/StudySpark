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

docker run -it --rm --user root spark:3.5.2-scala2.12-java17-python3-ubuntu bash
docker run -it --rm --user root spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/spark-shell
docker run -it --rm --user root spark:3.5.2-scala2.12-java17-python3-ubuntu /opt/spark/bin/pyspark

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


docker run -it --rm myspark01:0.0.2 bash
docker run -it --rm myspark01:0.0.2 /opt/spark/bin/spark-shell
docker run -it --rm \
  -v $(pwd):/opt/spark/work-dir \
  myspark01:0.0.2 /opt/spark/bin/pyspark


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

### Spark SQL 开发入口

Spark SQL 支持两类开发入口：

- 一个是大家所熟知的结构化查询语言：SQL
- 另一类是 DataFrame 开发算子。

就开发效率与执行效率来说，二者并无优劣之分，选择哪种开发入口，完全取决于开发者的个人偏好与开发习惯。

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

### createTempView

- createTempView 创建的临时表，其生命周期仅限于 SparkSession 内部
- createGlobalTempView 创建的临时表，可以在同一个应用程序中跨 SparkSession 提供访问。

### DataFrame 常用算子

- 同源类算子
  - 数据转换: map, mapPartitions, flatMap, filter
  - 数据聚合: groupByKey, reduce
  - 数据准备: union, sample
  - 数据预处理: repartition, coalesce
  - 结构收集: first, take, collect
- 探索类算子
  - 查看数据模式: columns, schema, printSchema
  - 查看数据的模样: show
  - 查看数据分布: describe
  - 查看数据的执行计划: explain
- 清洗类算子
  - drop: 删除掉 DataFrame 的列数据
  - distinct: 去重
  - dropDuplicates: 按照指定列去重
  - na: null 值处理
- 转换类算子
  - select: 按照列名对数据做投影
  - selectExpr: 以 SQL 语句为参数生成、提取数据
  - where: 以 SQL 语句为参数做数据过滤
  - withColumnRenamed: 字段重命名
  - withColumn: 生成新的数据列
  - explode: 展开数组类的数据列
- 分析类算子
  - join: 两个 DataFrame 之间做数据关联
  - groupBy: 按照某些列对数据做分组
  - agg: 分组后做数据聚合，Spark SQL 支持丰富的聚合算子
  - sort、orderBy: 按照某些列做排序
- 持久化类算子
  - write: 将 DataFrame 写入到文件系统、数据库等

#### drop 算子

drop 算子允许开发者直接把指定列从 DataFrame 中予以清除。

假设我们想把性别列清除，那么直接调用 employeesDF.drop(“gender”) 即可。

如果要同时清除多列，只需要在 drop 算子中用逗号把多个列名隔开即可。

#### distinct 算子

当有多条数据记录的所有字段值都相同时，使用 distinct 可以仅保留其中的一条数据记录。

#### dropDuplicates 算子

指定列进行去重。

```scala
employeesDF.dropDuplicates("gender").show
```

#### na 算子

- employeesDF.na.drop 用于删除 DataFrame 中带 null 值的数据记录
- employeesDF.na.fill(0) 则将 DataFrame 中所有的 null 值都自动填充为整数零

#### where 算子

想要过滤出所有性别为男的员工，我们就可以用 employeesDF.where(“gender = ‘Male’”) 来实现。

#### withColumnRenamed 算子

如果打算把 employeesDF 当中的“gender”重命名为“sex”

```scala
employeesDF.withColumnRenamed(“gender”, “sex”)。
```

#### withColumn 算子

withColumnRenamed 是重命名现有的数据列，而 withColumn 则用于生成新的数据列。

withColumn 和 selectExpr 有点像。

#### write 算子

在 read API 中，mode 选项键用于指定读取模式，如 permissive, dropMalformed, failFast。

但在 write API 中，mode 用于指定“写入模式”，分别有 Append、Overwrite、ErrorIfExists、Ignore 这 4 种模式。

- Append: 以追加的方式写入。
- Overwrite: 覆盖写入。
- ErrorIfExists: 如果目标存储路径已存在，则报异常。
- Ignore: 如果目标存储路径已存在，则放弃数据写入。

### join

- 按照关联形式（Join Types）来划分，数据关联分为内关联、外关联、左关联、右关联，等等。
  - 内关联: inner
  - 左外关联: left, leftouter, left_outer
  - 右外关联: right, rightouter, right_outer
  - 全外关联: outer, full, fullouter, full_outer
  - 左半关联: leftsemi, left_semi
  - 左逆关联: leftanti, left_anti
- 从实现机制的角度，Join 又可以分为
  - NLJ（Nested Loop Join）
  - SMJ（Sort Merge Join）
  - HJ（Hash Join）
- 从数据分发模式的角度出发，数据关联又可以分为
  - Shuffle Join
  - Broadcast Join

#### 内关联和外关联

这里的“内”，它指的是，在关联结果中，仅包含满足关联条件的那些数据记录；而“外”，它的含义是，在关联计算的结果集中，还包含不满足关联条件的数据记录。而外关联中的“左”、“右”、“全”，恰恰是在表明，那些不满足关联条件的记录，来自于哪里。

#### inner 内关联

内关联的效果，是仅仅保留左右表中满足关联条件的那些数据记录。以上表为例，关联条件是 salaries(“id”) === employees(“id”)，而在员工表与薪资表中，只有 1、2、3 这三个值同时存在于他们各自的 id 字段中。相应地，结果集中就只有 id 分别等于 1、2、3 的这三条数据记录。

#### leftsemi 左半关联

需要和 inner 内关联进行对比。

首先，左半关联是内关联的一个子集；其次，它只保留左表 salaries 中的数据。

#### leftanti 左逆关联

需要和 inner 内关联，leftsemi 左半关联进行对比。

左逆关联同样只保留左表的数据，但与左半关联不同的是，它保留的，是那些不满足关联条件的数据记录。

#### NLJ Nested Loop Join

我们又常常把左表称作是“驱动表”，而把右表称为“基表”。

驱动表的体量往往较大，在实现关联的过程中，驱动表是主动扫描数据的那一方。

而基表相对来说体量较小，它是被动参与数据扫描的那一方。

在 NLJ 的实现机制下，算法会使用外、内两个嵌套的 for 循环，来依次扫描驱动表与基表中的数据记录。

NLJ 算法的计算复杂度是 `O(M * N)`。

执行效率显然很差。

#### SMJ Sort Merge Join

算法思想：先排序，在归并。

如果已经排序好，计算复杂度是 `O(M + N)`。

如果还需要先排序，计算复杂度是 `O(M*log(M) + N*log(N))`。

Sort Merge Join 就没有内存方面的限制。不论是排序、还是合并，SMJ 都可以利用磁盘来完成计算。所以，在稳定性这方面，SMJ 更胜一筹。

#### HJ Hash Join

- build 阶段，计算复杂度是 `O(N)`。
- probe 阶段，计算复杂度是 `O(M)`。

Hash Join 这种算法对于内存的要求比较高，适用于内存能够容纳基表数据的计算场景。

#### NLJ vs SMJ vs HJ

NLJ 效率最差。

如果准备参与 Join 的两张表是有序表，那么这个时候采用 SMJ 算法来实现关联简直是再好不过了。

执行高效的 HJ 和 SMJ 只能用于等值关联，也就是说关联条件必须是等式，像 salaries(“id”) < employees(“id”) 这样的关联条件，HJ 和 SMJ 是无能为力的。相反，NLJ 既可以处理等值关联（Equi Join），也可以应付不等值关联（Non Equi Join），可以说是数据关联在实现机制上的最后一道防线。

#### Shuffle Join vs Broadcast Join

Spark SQL 之所以在默认情况下一律采用 Shuffle Join，原因在于 Shuffle Join 的“万金油”属性。也就是说，在任何情况下，不论数据的体量是大是小、不管内存是否足够，Shuffle Join 在功能上都能够“不辱使命”，成功地完成数据关联的计算。然而，有得必有失，功能上的完备性，往往伴随着的是性能上的损耗。

Broadcast Join 可以用来提升性能。
对于参与 Join 的两张表，我们可以把其中较小的一个封装为广播变量，然后再让它们进行关联。

尽管广播变量的创建与分发同样需要消耗网络带宽，但相比 Shuffle Join 中两张表的全网分发，因为仅仅通过分发体量较小的数据表来完成数据关联，Spark SQL 的执行性能显然要高效得多。

#### join 组合策略

- 对于等值关联（Equi Join），Spark SQL 优先考虑采用 Broadcast HJ 策略，其次是 Shuffle SMJ，最次是 Shuffle HJ。
- 对于不等值关联（Non Equi Join），Spark SQL 优先考虑 Broadcast NLJ，其次是 Shuffle NLJ。

不论是等值关联、还是不等值关联，只要 Broadcast Join 的前提条件成立，Spark SQL 一定会优先选择 Broadcast Join 相关的策略。

Broadcast Join 得以实施的基础，是被广播数据表的全量数据能够完全放入 Driver 的内存、以及各个 Executors 的内存

### RDD 数据处理生命周期

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
