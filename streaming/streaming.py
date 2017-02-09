from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.streaming.kafka import KafkaUtils
import json

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

        #df = spark.createDataFrame(rowRdd)
        df = spark.read.json(rdd)
        #df.show()

        # Creates a temporary view using the DataFrame
        df.createOrReplaceTempView("wiki")

        #sqlDF = spark.sql("SELECT * FROM wiki")
        sqlDF = spark.sql("SELECT prev_title as source, count(*) as count, max(timestamp) as timestamp FROM wiki group by prev_title")
        #sqlDF = spark.sql("SELECT prev_title as source, curr_title as topic, count(*) as count, max(timestamp) as timestamp FROM wiki group by prev_title, curr_title")
        sqlDF.show()
        
        sqlDF.write \
             .format("org.apache.spark.sql.cassandra") \
             .mode('append') \
             .options(table="realtime", keyspace="wiki") \
             .save()
      
    except:
        pass

if __name__ == "__main__":
    sc = SparkContext(master='spark://ip-172-31-1-142:7077', appName="wiki")
    ssc = StreamingContext(sc, 1)

    kafkaStream = KafkaUtils.createDirectStream(ssc, 
                                                ["clickstream"], 
                                                {"bootstrap.servers": "ec2-34-192-175-58.compute-1.amazonaws.com:9092"})
    
    lines = kafkaStream.map(lambda x: x[1])
    
    lines.foreachRDD(process)
    
    ssc.start()
    ssc.awaitTermination()
    
