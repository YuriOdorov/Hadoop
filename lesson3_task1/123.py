from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import NGram
import re

DATASET = "/data/wiki/en_articles_part"

spark = SparkSession.builder.appName('Spark DF first').master('yarn').getOrCreate()
schema = StructType(fields=[
    StructField("id", StringType()),
    StructField("text", StringType())])

def parseLine(line):
    id, text = line.lower().split('\t', 1)
    text = re.sub("[^\w\s]", "", text).split()
    return id, ' '.join(text)

sc = spark.sparkContext
lines = sc.textFile(DATASET)
parsed_logs = lines.map(parseLine).cache()

df = spark.createDataFrame(parsed_logs, schema=schema)\
.select(f.split('text', ' ').alias('text'))

ngram = NGram(n=2, inputCol="text", outputCol="bigrams")
df = ngram.transform(df)\
.select(f.posexplode('bigrams'))\
.where(f.col('col').like('narodnaya%'))\
.select(f.regexp_replace('col', ' ', '_').alias('word_pair'))\
.groupBy('word_pair').count()\
.orderBy('word_pair').cache()
for i in df.collect():
    print(i[0], i[1])