from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

def main():
    #conf = SparkConf().setAppName('wiki').setMaster('spark://ip-172-31-1-142:7077')
    conf = SparkConf().setAppName('wiki')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark = SparkSession \
        .builder \
        .appName("wiki") \
        .getOrCreate()
        
# .config("spark.some.config.option", "some-value") \

    df = spark.read.json('hdfs://ec2-34-192-175-58.compute-1.amazonaws.com:9000/user/*.dat')
    #df.show()

    df.createOrReplaceTempView("wiki")
    
    sqlDF = spark.sql("SELECT COUNT(DISTINCT prev_title, curr_title) FROM wiki")
    sqlDF.show()
  
    
    '''
    # save top 100 sources to Cassandra, create constant as partitionkey so all the data are stored in one partition in Cassandra
    sqlDF = spark.sql("SELECT 1 as constant, prev_title as source, sum(n) as total_count FROM wiki GROUP BY prev_title ORDER BY total_count DESC LIMIT 100")
    sqlDF.show()
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('overwrite') \
         .options(table="batch_source_top100", keyspace="wiki") \
         .save()
    '''
    
    '''
    # save top 100 topics to Cassandra, create constant as partitionkey so all the data are stored in one partition in Cassandra
    sqlDF = spark.sql("SELECT 1 as constant, curr_title as topic, sum(n) as total_count FROM wiki GROUP BY curr_title ORDER BY total_count DESC LIMIT 100")
    #sqlDF.show()
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('overwrite') \
         .options(table="batch_topic_top100", keyspace="wiki") \
         .save() 
            
    #sqlDF = spark.sql("SELECT prev_title as source, n as count, curr_title as topic FROM wiki")
    #sqlDF.show()
    '''
    
    '''
    # TABLE batch_source use source as partitionkey
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('overwrite') \
         .options(table="batch_source", keyspace="wiki") \
         .save()
    '''
    
    '''
    # TABLE batch_topic use topic as partitionkey
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('overwrite') \
         .options(table="batch_topic", keyspace="wiki") \
         .save()
    '''

if __name__ == "__main__":
    main()

  