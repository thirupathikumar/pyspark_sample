from pyspark import SparkContext, SparkConf
from datetime import datetime
print(str(datetime.now()) + " Start")
#Input Columns:StudentId,Year,Semester1Marks,Semester2Marks
studentMarks = [["S1","Y1",72.5,73.55],
["S1","Y2",64.94,76.64],
["S2","Y1",68.26,72.95],
["S2","Y2",54.49,64.8],
["S3","Y1",64.08,79.84],
["S3","Y2",54.45,87.72],
["S4","Y1",50.03,66.54],
["S4","Y2",71.26,69.54],
["S5","Y1",52.74,76.27],
["S5","Y2",50.39,68.58],
["S6","Y1",74.86,60.8],
["S6","Y2",58.29,62.38],
["S7","Y1",83.95,74.51],
["S7","Y2",66.69,56.92]]
sc = SparkContext(master="local", appName="Example2")
rdd= sc.parallelize(studentMarks)
print("Average grades per semester, each year, for each student")
avgRdd=rdd.map(lambda x : [x[0],x[1],(x[2]+x[3])/2])
print(avgRdd.collect())
print("Top three students who have the highest average grades in the second year")
secYearRdd=avgRdd.filter(lambda x : "Y2" in x)
secYearTopRdd=secYearRdd.takeOrdered(num=3,key=lambda x: -x[2])
print(secYearTopRdd)
print("Bottom three students who have the highest average grades in the second year")
secYearBottomRdd=secYearRdd.takeOrdered(num=3,key=lambda x: x[2])
print(secYearBottomRdd)
print("Students more than 70% avg")
morethan75Rdd=secYearRdd.filter(lambda x : x[2] > 70)
print(morethan75Rdd.collect())
print(str(datetime.now()) + " End")

