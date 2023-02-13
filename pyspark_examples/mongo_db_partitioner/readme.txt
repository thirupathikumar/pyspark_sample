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

****************************************************************************************************************************************
Mongo DB Error and Solution:
-----------------------------

https://stackoverflow.com/questions/38096502/stack-overflow-error-when-loading-a-large-table-from-mongodb-to-spark

I add another java option "-Xss32m" to spark driver to raise the memory of stack for every thread , and this exception is not throwing any more. How stupid was I , I should have tried it earlier. But another problem is shown, I will have to check more. still great thanks for your help.


https://medium.com/@thomaspt748/how-to-load-millions-of-data-into-mongo-db-using-apache-spark-3-0-8bcf089bd6ed
Load millions of data into MongoDB using apache spark 3.0 

mongo db write config - writeConcern.w "majority".

“majority” means Write operation returns acknowledgement after propagating to M-number of data-bearing voting members (primary and secondaries)

Possible writeConcern.w option:
----------------------------------
"majority" - Requests acknowledgment that write operations have propagated to the calculated majority of the data-bearing voting members (i.e. primary and secondaries with members[n].votes greater than 0). { w: "majority" } is the default write concern for most MongoDB deployments. For example, consider a replica set with 3 voting members, Primary-Secondary-Secondary (P-S-S). For this replica set, 
calculated majority is two, and the write must propagate to the primary and one secondary to acknowledge the write concern to the client.

After the write operation returns with a w: "majority" acknowledgment to the client, the client can read the result of that write with a "majority" readConcern. If you specify a "majority" write concern for a multi-document transaction and the transaction fails to replicate to the calculated majority of replica set members, then the transaction may not immediately roll back on replica set members. The replica set will be eventually consistent. A transaction is always applied or rolled back on all replica set members.

<number>: Requests acknowledgment that the write operation has propagated to the specified number of mongod instances. For example:
w: 1 - Requests acknowledgment that the write operation has propagated to the standalone mongod or the primary in a replica set. Data can be rolled back if the primary steps down before the write operations have replicated to any of the secondaries.
w: 0 - Requests no acknowledgment of the write operation. However, w: 0 may return information about socket exceptions and networking errors to the application. Data can be rolled back if the primary steps down before the write operations have replicated to any of the secondaries.

If you specify w: 0 but include j: true, the j: true prevails to request acknowledgment from the standalone mongod or the primary of a replica set.w greater than 1 requires acknowledgment from the primary and as many data-bearing secondaries as needed to meet the specified write concern. For example, consider a 3-member replica set with a primary and 2 secondaries. Specifying w: 2 would require acknowledgment from the primary and one of the secondaries. Specifying w: 3 would require acknowledgment from the primary and both secondaries.

custom write concern name - Requests acknowledgment that the write operations have propagated to tagged members that satisfy the custom write concern defined in settings.getLastErrorModes. For an example, see Custom Multi-Datacenter Write Concerns.Data can be rolled back if the custom write concern only requires acknowledgment from the primary and the primary steps down before the write operations have replicated to any of the secondaries.

Ref - https://www.mongodb.com/docs/manual/reference/write-concern/#std-label-wc-w

"","CASE_STATUS","EMPLOYER_NAME","SOC_NAME","JOB_TITLE","FULL_TIME_POSITION","PREVAILING_WAGE","YEAR","WORKSITE","lon","lat"
"1","CERTIFIED-WITHDRAWN","UNIVERSITY OF MICHIGAN","BIOCHEMISTS AND BIOPHYSICISTS","POSTDOCTORAL RESEARCH FELLOW","N",36067,2016,"ANN ARBOR, MICHIGAN",-83.7430378,42.2808256
"2","CERTIFIED-WITHDRAWN","GOODMAN NETWORKS, INC.","CHIEF EXECUTIVES","CHIEF OPERATING OFFICER","Y",242674,2016,"PLANO, TEXAS",-96.6988856,33.0198431
"3","CERTIFIED-WITHDRAWN","PORTS AMERICA GROUP, INC.","CHIEF EXECUTIVES","CHIEF PROCESS OFFICER","Y",193066,2016,"JERSEY CITY, NEW JERSEY",-74.0776417,40.7281575
"4","CERTIFIED-WITHDRAWN","GATES CORPORATION, A WHOLLY-OWNED SUBSIDIARY OF TOMKINS PLC","CHIEF EXECUTIVES","REGIONAL PRESIDEN, AMERICAS","Y",220314,2016,"DENVER, COLORADO",-104.990251,39.7392358
"5","WITHDRAWN","PEABODY INVESTMENTS CORP.","CHIEF EXECUTIVES","PRESIDENT MONGOLIA AND INDIA","Y",157518.4,2016,"ST. LOUIS, MISSOURI",-90.1994042,38.6270025
"6","CERTIFIED-WITHDRAWN","BURGER KING CORPORATION","CHIEF EXECUTIVES","EXECUTIVE V P, GLOBAL DEVELOPMENT AND PRESIDENT, LATIN AMERI","Y",225000,2016,"MIAMI, FLORIDA",-80.1917902,25.7616798
"7","CERTIFIED-WITHDRAWN","BT AND MK ENERGY AND COMMODITIES","CHIEF EXECUTIVES","CHIEF OPERATING OFFICER","Y",91021,2016,"HOUSTON, TEXAS",-95.3698028,29.7604267
"8","CERTIFIED-WITHDRAWN","GLOBO MOBILE TECHNOLOGIES, INC.","CHIEF EXECUTIVES","CHIEF OPERATIONS OFFICER","Y",150000,2016,"SAN JOSE, CALIFORNIA",-121.8863286,37.3382082
"9","CERTIFIED-WITHDRAWN","ESI COMPANIES INC.","CHIEF EXECUTIVES","PRESIDENT","Y",127546,2016,"MEMPHIS, TEXAS",NA,NA
"10","WITHDRAWN","LESSARD INTERNATIONAL LLC","CHIEF EXECUTIVES","PRESIDENT","Y",154648,2016,"VIENNA, VIRGINIA",-77.2652604,38.9012225











****************************************************************************************************************************************


