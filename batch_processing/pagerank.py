from __future__ import print_function
import sys
from operator import add
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

        
def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: batch_process <hdfs_path>", file=sys.stderr)
        exit(-1)
    
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
        
    df = spark.read.json(sys.argv[1]) \
         .select(concat(col("prev_title"), lit(" "), col("curr_title")))
    
    lines = df.rdd.map(lambda r: r[0])
    
    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(10):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    rankDF = spark.createDataFrame(ranks)
    rankDF.createOrReplaceTempView("rank")
    sqlDF = spark.sql("SELECT _1 AS topic, _2 AS rank, current_date() as date FROM rank order by _2 DESC LIMIT 100") 
    sqlDF.show()

    '''
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('append') \
         .options(table="pagerank", keyspace="wiki") \
         .save()
    '''
    
    spark.stop()