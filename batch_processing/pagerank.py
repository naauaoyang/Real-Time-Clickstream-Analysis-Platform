from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession


def computeContribs(urlAndWeights, rank):
    """Calculates URL contributions to the rank of other URLs."""
    for urlAndWeight in urlAndWeights:
        yield (urlAndWeight[0], rank * urlAndWeight[1])

        
# recompute weight
def getWeights(urlWeights):
    weightSum = 0 
    for urlWeight in urlWeights[1]:
        weightSum += urlWeight[1]
    new_urlWeights = list()
    for urlWeight in urlWeights[1]:
        new_urlWeights.append((urlWeight[0], urlWeight[1]/weightSum))   
    return urlWeights[0], new_urlWeights


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: batch_process <hdfs_path> <iterations>", file=sys.stderr)
        exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
        
    df = spark.read.json(sys.argv[1]).select("prev_title", "curr_title", "n")
    df.createOrReplaceTempView("df")
    
    # make the (prev_title, curr_title) unique
    uniqueDF = spark.sql("SELECT prev_title, curr_title, sum(n) AS n FROM df GROUP BY prev_title, curr_title") 
    uniqueDF.createOrReplaceTempView("uniqueDF")
    
    # add nodes only having inlinks
    inlinkDF1 = spark.sql("SELECT DISTINCT a.curr_title FROM uniqueDF AS a LEFT JOIN uniqueDF AS b on a.curr_title = b.prev_title WHERE b.prev_title IS NULL")
    inlinkDF1.createOrReplaceTempView("inlinkDF1")
    inlinkDF2 = spark.sql("SELECT curr_title AS prev_title, curr_title, 1 AS n FROM inlinkDF1") 
    mergedDF1 = df.unionAll(inlinkDF2)
    
    # add nodes only having outlinks
    outlinkDF1 = spark.sql("SELECT DISTINCT a.prev_title FROM uniqueDF AS a LEFT JOIN uniqueDF AS b on a.prev_title = b.curr_title WHERE b.curr_title IS NULL")
    outlinkDF1.createOrReplaceTempView("outlinkDF1")
    outlinkDF2 = spark.sql("SELECT prev_title, prev_title AS curr_title, 0 AS n FROM outlinkDF1") 
    mergedDF2 = mergedDF1.unionAll(outlinkDF2)
        
    # Loads all URLs from input file and initialize their neighbors.
    links = mergedDF2.rdd.map(lambda urls: (urls["prev_title"], (urls["curr_title"], float(urls["n"])))).groupByKey()
     
    weightedLinks = links.map(lambda urlWeights: getWeights(urlWeights)).cache()
    
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = weightedLinks.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = weightedLinks.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and save them to Cassandra.
    rankDF = spark.createDataFrame(ranks)
    rankDF.createOrReplaceTempView("rank")
    sqlDF = spark.sql("SELECT _1 AS topic, _2 AS rank, current_date() as date FROM rank order by _2 DESC LIMIT 100") 
    #sqlDF.show()
    
    sqlDF.write \
         .format("org.apache.spark.sql.cassandra") \
         .mode('append') \
         .options(table="pagerank", keyspace="wiki") \
         .save()
    
    spark.stop()