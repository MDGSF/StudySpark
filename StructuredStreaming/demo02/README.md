# demo02

- 开启一个终端，执行 `nc -lk 9999`，建立一个 socket 服务端，监听在 9999 端口。
- 再开启一个终端，执行 `pyspark`
- 把 demo02.py 中的代码都复制到 pyspark 终端中执行。
- 然后切换到 nc 终端，输入如下字符，看看是否能收到，记得换行。

```text
2021-10-01 09:30:00,Apache Spark
2021-10-01 09:34:00,Spark Logo
2021-10-01 09:36:00,Structured Streaming
2021-10-01 09:39:00,Spark Streaming
```
