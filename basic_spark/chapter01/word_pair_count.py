from pyspark import SparkContext

textFile = SparkContext().textFile("./wikiOfSpark.txt")
wordRDD = textFile.flatMap(
    lambda line: [
        "-".join(line.split()[i : i + 2]) for i in range(len(line.split()) - 1)
    ]
)
cleanWordRDD = wordRDD.filter(lambda word: word != "")
kvRDD = cleanWordRDD.map(lambda word: (word, 1))
wordCountsRDD = kvRDD.reduceByKey(lambda x, y: x + y)
wordCount = wordCountsRDD.sortBy(lambda x: x[1], False).take(5)
print(wordCount)
