# StudySpark

- <https://github.com/wulei-bj-cn/learn-spark>
- spark on k8s: <https://spark.apache.org/docs/latest/running-on-kubernetes.html>
- rdd: <https://spark.apache.org/docs/latest/rdd-programming-guide.html>

```sh
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark

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
