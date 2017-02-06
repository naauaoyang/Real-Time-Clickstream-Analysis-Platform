CREATE KEYSPACE wiki WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE wiki;
 
CREATE TABLE batch_source (source text, topic text, count int, PRIMARY KEY(source, count)) with CLUSTERING ORDER B
Y (count DESC);

CREATE TABLE batch_topic (source text, topic text, count int, PRIMARY KEY(topic, count)) with CLUSTERING ORDER BY 
(count DESC);

CREATE TABLE batch_source_top100 (constant int, source text, total_count int, PRIMARY KEY(constant, total_count)) 
WITH CLUSTERING ORDER BY (total_count DESC);

CREATE TABLE batch_topic_top100 (constant int, topic text, total_count int, PRIMARY KEY(constant, total_count)) WI
TH CLUSTERING ORDER BY (total_count DESC);

CREATE TABLE realtime_source (source text, topic text, count int, timestamp timestamp, PRIMARY KEY(topic, timestam
p, count)) WITH CLUSTERING ORDER BY (timestamp DESC, count DESC);