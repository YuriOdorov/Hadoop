from pyspark.sql.types import StructType, StructField, IntegerType
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import NGram
import re
from pyspark.sql.functions import *

DATASET = "/data/wiki/en_articles_part"
spark = SparkSession.builder.appName('Spark DF first').master('yarn').getOrCreate()

schema = StructType([
    StructField("except", StringType(), False)
])
exceptions = spark.read.csv('/data/wiki/stop_words_en-xpo6.txt', sep="\t", schema=schema)\
.toPandas()['except'].tolist() + ['']


schema = StructType(fields=[
    StructField("id", StringType()),
    StructField("text", StringType())])

def parseLine(line):
    id, text = line.lower().split('\t', 1)
    text = [re.sub("[^\w\s]", "", i) for i in text.split()]
    text = [i for i in text if i not in exceptions]
    return id, ' '.join(text)

sc = spark.sparkContext
lines = sc.textFile(DATASET)
parsed_logs = lines.map(parseLine).cache()

df = spark.createDataFrame(parsed_logs, schema=schema)\
.select(f.split('text', ' ').alias('text'))

all_word = df.select(posexplode('text')).groupBy(col('col').alias('words')).count().cache()

count_word = df.select(posexplode('text')).count()

ngram = NGram(n=2, inputCol="text", outputCol="bigrams")
df = ngram.transform(df)\
.select(f.posexplode('bigrams'))\
.withColumn('word_pair', regexp_replace('col', ' ', '_'))\
.withColumn('par', split('col', ' '))

count_bigram = df.count()

all_bigram = df.select('word_pair', col('par')[0].alias('word_1'), col('par')[1].alias('word_2'))\
.groupBy('word_pair', 'word_1', 'word_2').count().withColumnRenamed('count', 'count_bigram')\
.where(col('count_bigram') >= 500)\
.join(all_word, col('word_1') == col('words'))\
.drop('words').withColumnRenamed('count', 'count_word_1')\
.join(all_word, col('word_2') == col('words'))\
.drop('words').withColumnRenamed('count', 'count_word_2')

rez = all_bigram.withColumn('NPMI',  -1 * log((col('count_bigram') / count_bigram)
                        / ((col('count_word_1') / count_word)
                            * (col('count_word_2') / count_word)))
                    / log(col('count_bigram') / count_bigram))\
.orderBy(col('NPMI').desc()).select('word_pair')\
.toPandas()['word_pair'].tolist()

print(*rez, sep='\n')