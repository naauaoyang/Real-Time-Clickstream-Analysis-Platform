from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("batch_processing")\
        .getOrCreate()

    df = spark.read.json('hdfs://ec2-34-192-175-58.compute-1.amazonaws.com:9000/user/*.dat')
    #df.show()

    df.createOrReplaceTempView("wiki")
    sqlDF = spark.sql("SELECT prev_title as source, n as count, curr_title as topic FROM wiki")
    sqlDF.show()
    
    '''
    # TABLE batch_source use source as partitionkey
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('overwrite') \
         .options(table="batch_source", keyspace="wiki") \
         .save()
    
    # TABLE batch_topic use topic as partitionkey
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('overwrite') \
         .options(table="batch_topic", keyspace="wiki") \
         .save()
    '''

  