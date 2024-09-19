from pyspark import SparkContext

lineRDD = SparkContext().textFile("./wikiOfSpark.txt")

print(lineRDD.first())

wordRDD = lineRDD.flatMap(lambda line: line.split(" "))
cleanWordRDD = wordRDD.filter(lambda word: word != "")

print(cleanWordRDD.take(3))

kvRDD = cleanWordRDD.map(lambda word: (word, 1))
wordCountsRDD = kvRDD.reduceByKey(lambda x, y: x + y)
print(wordCountsRDD.collect())
