import random
from pyspark import SparkContext

sc = SparkContext()

arr = [random.randint(0, 99) for _ in range(100)]
rdd = sc.parallelize(arr)

l1 = rdd.sample(False, 0.1).collect()
print(l1)

l2 = rdd.sample(False, 0.1).collect()
print(l2)

l3 = rdd.sample(False, 0.1, 123).collect()
print(l3)

l4 = rdd.sample(False, 0.1, 123).collect()
print(l4)

l5 = rdd.sample(True, 0.1, 456).collect()
print(l5)

l6 = rdd.sample(True, 0.1, 456).collect()
print(l6)
