from pyspark import SparkContext, SparkConf
from datetime import datetime
print(str(datetime.now()) + " Start")
pro2001 = ['PRO1', 'PRO2', 'PRO3', 'PRO4', 'PRO5', 'PRO6', 'PRO7']
pro2002 = ['PRO3', 'PRO4', 'PRO7', 'PRO8', 'PRO9']
pro2003 = ['PRO4', 'PRO8', 'PRO10', 'PRO11', 'PRO12']
sc = SparkContext(master="local", appName="Example3")
rdd2001=sc.parallelize(pro2001)
rdd2002=sc.parallelize(pro2002)
rdd2003=sc.parallelize(pro2003)
print("Number of projects initiated in the three years")
print(rdd2001.union(rdd2002).union(rdd2003).distinct().count())
print("Projects initiated in the three years")
print(rdd2001.union(rdd2002).union(rdd2003).distinct().collect())

proCompletedInFirstYear=rdd2001.subtract(rdd2002)
print("Projects completed in first year")
print(proCompletedInFirstYear.collect())

print(rdd2001.union(rdd2002).subtract(rdd2003).distinct())
proCompletedInFirstTwoYear=rdd2001.union(rdd2002).subtract(rdd2003).distinct().collect()
print("Projects completed in first two year")
print(proCompletedInFirstTwoYear)

proInTwoyears = rdd2001.intersection(rdd2002)
proFrom2001to2003=proInTwoyears.intersection(rdd2003)
print("Projects started in 2001 and continued through 2003")
print(proFrom2001to2003.collect())

print(str(datetime.now()) + " End")
