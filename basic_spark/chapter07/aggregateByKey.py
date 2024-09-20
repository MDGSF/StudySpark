from pyspark import SparkContext

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

kvRDD = cleanWordRDD.map(lambda word: (word, 1))

wordCountsRDD = kvRDD.aggregateByKey(
    (0, 0),  # 初始值 (sum, max)
    lambda acc, count: (acc[0] + count, max(acc[1], acc[0] + count)),  # seqOp
    lambda acc1, acc2: (acc1[0] + acc2[0], max(acc1[1], acc2[1])),  # combOp
)

wordCount = wordCountsRDD.sortBy(lambda x: x[1], False).take(50)

print(wordCount)
