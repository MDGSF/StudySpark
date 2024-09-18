import org.apache.spark.rdd.RDD
 
// 这里的下划线"_"是占位符，代表数据文件的根目录
val rootPath: String = _
val file: String = s"${rootPath}/wikiOfSpark.txt"
 
// 读取文件内容
val lineRDD: RDD[String] = spark.sparkContext.textFile(file)

// 以行为单位提取相邻单词
val wordPairRDD: RDD[String] = lineRDD.flatMap( line => {
  // 将行转换为单词数组
  val words: Array[String] = line.split(" ")
  // 将单个单词数组，转换为相邻单词数组
  for (i <- 0 until words.length - 1) yield words(i) + "-" + words(i+1)
})

val cleanWordRDD: RDD[String] = wordPairRDD.filter(word => !word.equals(""))
 
// 把RDD元素转换为（Key，Value）的形式
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))
// 按照单词做分组计数
val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) => x + y)
 
// 打印词频最高的5个词汇
wordCounts.map{case (k, v) => (v, k)}.sortByKey(false).take(5)