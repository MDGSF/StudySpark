from pyspark import SparkContext

textFile = SparkContext().textFile("./wikiOfSpark.txt")
wordRDD = textFile.flatMap(lambda line: line.split(" "))
cleanWordRDD = wordRDD.filter(lambda word: word != "")
kvRDD = cleanWordRDD.map(lambda word: (word, 1))
wordCountsRDD = kvRDD.reduceByKey(lambda x, y: x + y)

wordCountsRDD.cache()  # 使用cache算子告知Spark对wordCounts加缓存
wordCountsRDD.count()  # 触发wordCounts的计算，并将wordCounts缓存到内存

wordCount = wordCountsRDD.sortBy(lambda x: x[1], False).take(5)
print(wordCount)

targetPath = "output.txt"
wordCountsRDD.saveAsTextFile(targetPath)
