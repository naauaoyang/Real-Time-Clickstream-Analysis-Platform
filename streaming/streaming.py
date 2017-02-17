from __future__ import print_function
import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils

# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        df = spark.read.json(rdd)

        # Creates a temporary view using the DataFrame
        df.createOrReplaceTempView("wiki")

        sqlDF = spark.sql("SELECT prev_title as source, count(*) as count, max(timestamp) as timestamp FROM wiki group by prev_title")
        
        sqlDF.show()
        
        sqlDF.write \
             .format("org.apache.spark.sql.cassandra") \
             .mode('append') \
             .options(table="realtime", keyspace="wiki") \
             .save()
        
    except:
        pass

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: streaming <bootstrap.servers>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="wiki")
    ssc = StreamingContext(sc, 1)

    kafkaStream = KafkaUtils.createDirectStream(ssc, 
                                                ["clickstream"], 
                                                {"bootstrap.servers": sys.argv[1]})
    
    lines = kafkaStream.map(lambda x: x[1])
    
    lines.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()
    
