from pyspark import SparkContext, SparkConf
pyList=[2.3,5,6.7,123.12,23.3,78,5.7]
#Create spark context object
sc = SparkContext(master="local", appName="Example1")
#Create RDD from python list
rdd=sc.parallelize(pyList,2)
print("Get number of partitions")
print(rdd.getNumPartitions())
print("Get first element")
print(rdd.first())
print("Get first two elements")
print(rdd.take(2))
print("Get all elements")
print(rdd.collect())




