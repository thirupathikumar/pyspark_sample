Ref - https://groups.google.com/g/mongodb-user/c/tXwdIgdDC5g?pli=1

The MongoDB Spark Connector automatically partitions the data according to the partitioner config (see the partitioner section on the input configuration).  The default partitioner is the MongoSamplePartitioner.  It uses the average document size and random sampling of the collection to determine suitable partitions for the collection. The partition size is configurable and it defaults to approximately 64MB chunks of data. This causes the extra queries as each partition of the collection gets a min and max _id to query against.

It should be noted that none of the connector partitioners use the supplied pipeline query, they just chunk up the raw collection and the aggregation pipeline is applied afterwards.  This may be inefficient depending on your needs and may result in empty partitions. However, given the flexibility required for the partitioners it fits all use cases.  You can supply your own partitioner with custom logic should any of the defaults not meet your need.  There is an example of that in the test suite with the HalfwayPartitioner and its used in this test case.

So to answer your question, the number 262 comes from the partitioner dividing up the collection. You can change partitioning strategy by configuring an alternative partitioner or even supply your own.

Ref - https://www.mongodb.com/docs/spark-connector/current/configuration/read/#std-label-partitioner-conf
Partitioner Configurations:
1.SamplePartitioner
2.ShardedPartitioner
3.PaginateBySizePartitioner
4.PaginateIntoPartitionsPartitioner
5.SinglePartitionPartitioner

Note: If you use SparkConf to set the connector's read configurations, prefix each property with spark.mongodb.read.partitioner.options. instead of partitioner.options..

You must specify this partitioner using the full classname: com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner.

Property name								Description
partitioner.options.partition.field			The field to use for partitioning, which must be a unique field.
Default: _id

partitioner.options.partition.size			The size (in MB) for each partition. Smaller partition sizes create more partitions containing fewer documents.
Default: 64

partitioner.options.samples.per.partition	The number of samples to take per partition. The total number of samples taken is:
											samples per partiion * ( count / number of documents per partition)
Default: 10

Example: For a collection with 640 documents with an average document size of 0.5 MB, the default SamplePartitioner configuration values creates 5 partitions with 128 documents per partition.

The MongoDB Spark Connector samples 50 documents (the default 10 per intended partition) and defines 5 partitions by selecting partition field ranges from the sampled documents.

ShardedPartitioner Configuration - The ShardedPartitioner automatically determines the partitions to use based on your shard configuration.
You must specify this partitioner using the full classname: com.mongodb.spark.sql.connector.read.partitioner.ShardedPartitioner.
Note: This partitioner is not compatible with hashed shard keys.




