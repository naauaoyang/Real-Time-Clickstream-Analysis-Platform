from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
import json


if __name__ == "__main__":
    
    spark = SparkSession \
        .builder \
        .appName("wiki") \
        .getOrCreate()
    
    lines = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "ec2-34-192-175-58.compute-1.amazonaws.com:9092") \
      .option("subscribe", "wiki") \
      .load() \
      .selectExpr("CAST(value AS STRING)")
    
    #split_col = split(lines['value'], ':')
    #df = lines.withColumn('NAME1', split_col.getItem(0))
    #df = lines.withColumn('NAME2', split_col.getItem(1))
    
    query = lines\
         .writeStream\
         .outputMode('append')\
         .format('console')\
         .start()
         
    query.awaitTermination()
    spark.stop()
    