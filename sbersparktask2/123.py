from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.enableHiveSupport().master("yarn").getOrCreate()  # master('local[2]')

def spark_BFS(fr, to):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("to", StringType(), False),
        StructField("from", StringType(), False)
    ])

    forvard_edges = spark.read.csv('/data/twitter/twitter_sample.txt', sep="|", header=True, schema=schema) \
        .select('to', 'from').cache()

    dist_schema = StructType([
        StructField("vertex", IntegerType(), False),
        StructField("dist", IntegerType(), False),
        StructField("way", StringType(), False)
    ])

    distances = spark.createDataFrame([(to, 0, str(to))], dist_schema)
    d = 0
    while True:
        candidates = distances.join(forvard_edges, col('from') == col('vertex')) \
            .select(col('to').alias('vertex'),
                    (col('dist') + 1).alias("dist2"),
                    concat_ws(',', 'to', 'way').alias('way2')).cache()
        new_distances = distances.join(candidates, on='vertex', how='full_outer') \
            .select('vertex', when(col('way').isNotNull(), col('way')) \
                    .otherwise(col('way2')).alias('way'),
                    when(col('dist').isNotNull(), col('dist')) \
                    .otherwise(col('dist2')).alias('dist')).persist()

        res = new_distances.where(col('vertex') == fr)
        if res.count() != 0:
            return (res.take(1)[0]['way'])
        if new_distances.where(col('dist') == d + 1).count() > 0:
            d += 1
            distances = new_distances
        else:
            return ('Нет пути между вершинами')


print(spark_BFS(12, 34))
