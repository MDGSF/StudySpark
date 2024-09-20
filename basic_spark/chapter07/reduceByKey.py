import random
from pyspark import SparkContext

# random.seed(42)

textFile = SparkContext().textFile("./wikiOfSpark.txt")

wordRDD = textFile.flatMap(
    lambda line: [
        "-".join(line.split()[i : i + 2]) for i in range(len(line.split()) - 1)
    ]
)


def cleanWordRDDFilter(s):
    parts = s.split("-")
    if len(parts) != 2:
        return False
    invalid_chars = {"&", "|", "#", "^", "@", ";"}
    for part in parts:
        if not part or any(c in invalid_chars for c in part):
            return False
    return True


cleanWordRDD = wordRDD.filter(cleanWordRDDFilter)

kvRDD = cleanWordRDD.map(lambda word: (word, random.randint(1, 100)))
wordCountsRDD = kvRDD.reduceByKey(max)
wordCount = wordCountsRDD.sortBy(lambda x: x[1], False).take(50)
print(wordCount)
