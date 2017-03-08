# A Real-time Clickstream Analysis Platform

## Project Overview
The motivation for building such a platform is that clickstream data is very common for most companies as long as they have their own websites. For example, the table below shows part of the information we can get from one click. The source means where the click come from and topic is the page that the click goes to. The timestamp is the when the click happens. Using such kind of data, we can do a lot of things, such as user behavior analysis, website resources allocation and etc.

| Source        | topic         | timestamp           |
| ------------- |:-------------:| -------------------:|
| Google        | Super Bowl    | 2017-02-07 16:34:48 |

## Data Source
I use the February 2015 data from Wikipedia Clickstream dataset. The data can be downloaded from this [link](https://figshare.com/articles/Wikipedia_Clickstream/1305770). The documentation of the data can be found in this [link](https://ewulczyn.github.io/Wikipedia_Clickstream_Getting_Started/)

## Data Pipeline
The data pipeline for the platform is shown in the following picture. First generate clickstream data from Wikipedia clickstream dataset. Then use Kafka to ingest the message. Then there is one batch line and one streaming line. The batch line use HDFS to store all the raw data and use spark to do batch processing. The streaming line use Spark Streaming to do nearly real time processing. Both batch and real-time lines will store processed data in Cassandra. Finally use Flask to visualize it.

![pipeline](/image/pipeline.png?raw=true "pipeline")

## Usage Instruction
This code was run on Amazon AWS servers.
### Install
This project needs Hadoop, Spark, Zookeeper, Kafka and Cassandra. You can install and configure them according to the official tutorials, or you can use [pegasus](https://github.com/InsightDataScience/pegasus).
### Preprocessing
Download February 2015 data to data folder and then run:
`$ python preprocessing/data_preprocess.py`
### Generate Data
`$ python kafka/producer.py data/preprocessed_data.csv <Kafka bootstrap_servers>`
### Batch Processing
#### Consume the Data From Kafka and Save It To HDFS
`$ python batch_processing/consumer-hdfs.py <Kafka bootstrap_servers>`
#### Do Batch Processing and Save Results to Cassandra
`$ spark-submit --master <spark master url> --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 batch_processing/batch_process.py <hdfs url>/user/*.dat`
#### Run PageRank algorithm and Save Results to Cassandra
`$ spark-submit --master <spark master url> --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 batch_processing/pagerank.py <hdfs url>/user/*.dat 10`
### Streaming Processing
`$ spark-submit --master <spark master url> --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.0-M3 streaming/streaming.py <Kafka bootstrap_servers>`
### Start Website
`$ sudo -E python flask/tornadoapp.py <public_dns>`

## Demo
The presentation is available [here](https://docs.google.com/presentation/d/1U2U0Doo2EPh9osboorMb4jAOHzCneGV_Z_hDvs8WjpM/edit?usp=sharing)
<br>
The video for demo is available [here](https://youtu.be/gIu9z1A_NwI)
