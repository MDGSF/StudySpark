from pyspark import SparkContext

textFile = SparkContext().textFile("./wikiOfSpark.txt")
wordRDD = textFile.flatMap(lambda line: line.split(" "))
cleanWordRDD = wordRDD.filter(lambda word: word != "")
kvRDD = cleanWordRDD.map(lambda word: (word, 1))
wordCountsRDD = kvRDD.reduceByKey(lambda x, y: x + y)
wordCount = wordCountsRDD.sortBy(lambda x: x[1], False).take(5)
print(wordCount)
