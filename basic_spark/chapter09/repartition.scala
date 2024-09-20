// 生成0到99的整型数组
val arr = (0 until 100).toArray
// 使用parallelize生成RDD
val rdd = sc.parallelize(arr)
 
rdd.partitions.length
// 4
 
val rdd1 = rdd.repartition(2)
rdd1.partitions.length
// 2
 
val rdd2 = rdd.repartition(8)
rdd2.partitions.length
// 8