# StudySpark

- <https://github.com/wulei-bj-cn/learn-spark>
- spark on k8s: <https://spark.apache.org/docs/latest/running-on-kubernetes.html>
- rdd: <https://spark.apache.org/docs/latest/rdd-programming-guide.html>

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
  spark:3.5.2-scala2.12-java17-python3-ubuntu bash -c \
  "/opt/spark/bin/spark-submit count_word.py"
```

## notes

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

### 常用算子

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
