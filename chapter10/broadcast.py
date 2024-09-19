from pyspark import SparkContext

sc = SparkContext()
lineRDD = sc.textFile("./wikiOfSpark.txt")
wordRDD = lineRDD.flatMap(lambda line: line.split(" "))

wordList = ["Apache", "Spark"]
bc = sc.broadcast(wordList)

cleanWordRDD = wordRDD.filter(lambda word: word in bc.value)
kvRDD = cleanWordRDD.map(lambda word: (word, 1))
wordCountsRDD = kvRDD.reduceByKey(lambda x, y: x + y)
print(wordCountsRDD.collect())
