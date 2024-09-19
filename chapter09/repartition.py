from pyspark import SparkContext

sc = SparkContext()

arr = [i for i in range(100)]
rdd = sc.parallelize(arr)

rdd.getNumPartitions()

rdd1 = rdd.repartition(2)
rdd1.getNumPartitions()

rdd2 = rdd.repartition(4)
rdd2.getNumPartitions()
