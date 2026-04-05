from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("prepare_input_clean").getOrCreate()

df = spark.read.parquet("file:///a.parquet")

docs = (
    df.select(
        F.col("id").cast("string").alias("id"),
        F.regexp_replace(F.col("title"), r"[\t\r\n]+", " ").alias("title"),
        F.regexp_replace(F.col("text"), r"[\t\r\n]+", " ").alias("text"),
    )
    .dropna()
    .limit(1000)
    .rdd
    .map(lambda r: f"{r['id']}\t{r['title']}\t{r['text']}")
)

docs.saveAsTextFile("hdfs://cluster-master:9000/input/data")
spark.stop()