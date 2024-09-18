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
