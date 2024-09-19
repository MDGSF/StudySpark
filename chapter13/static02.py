from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import count, max

conf = SparkConf()
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "8g")
conf.set("spark.executor.cores", "4")
conf.set("spark.cores.max", 16)
# conf.set("spark.local.dir", rootPath)
spark = SparkSession(SparkContext(conf=conf))

rootPath = "/opt/spark/work-dir/2011-2019小汽车摇号数据"


hdfs_path_apply = rootPath + "/apply"
applyNumbersDF = spark.read.option("basePath", rootPath).parquet(
    hdfs_path_apply + "/*/*.parquet"
)

hdfs_path_lucky = rootPath + "/lucky"
luckyDogsDF = spark.read.option("basePath", rootPath).parquet(
    hdfs_path_lucky + "/*/*.parquet"
)

# 过滤2016年以后的中签数据，且仅抽取中签号码carNum字段
filteredLuckyDogs = luckyDogsDF.filter(luckyDogsDF["batchNum"] >= "201601").select(
    "carNum"
)

# 摇号数据与中签数据做内关联，Join Key为中签号码carNum
jointDF = applyNumbersDF.join(filteredLuckyDogs, "carNum", "inner")

# 以batchNum、carNum做分组，统计倍率系数
multipliers = jointDF.groupBy(["batchNum", "carNum"]).agg(
    count("batchNum").alias("multiplier")
)

# 以carNum做分组，保留最大的倍率系数
uniqueMultipliers = multipliers.groupBy("carNum").agg(
    max("multiplier").alias("multiplier")
)

# 以multiplier倍率做分组，统计人数
result = (
    uniqueMultipliers.groupBy("multiplier")
    .agg(count("carNum").alias("cnt"))
    .orderBy("multiplier")
)

result.show(40)
res = result.collect()
