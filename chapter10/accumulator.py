from pyspark import SparkContext

sc = SparkContext()
lineRDD = sc.textFile("./wikiOfSpark.txt")
wordRDD = lineRDD.flatMap(lambda line: line.split(" "))

ac = sc.accumulator(0)


def wordFilter(word):
    global ac
    if word == "":
        ac.add(1)
        return False
    return True


cleanWordRDD = wordRDD.filter(wordFilter)
kvRDD = cleanWordRDD.map(lambda word: (word, 1))
wordCountsRDD = kvRDD.reduceByKey(lambda x, y: x + y)
print(wordCountsRDD.collect())
print(ac.value)
