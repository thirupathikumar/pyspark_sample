from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

#Note: we have to select the spark version2 to run the mongo application with pyspark. the spark2 is fully compatiable with mongo db
#export SPARK_MAJOR_VERSION=2  

#spark-submit --packages org.mongodb.park:mongo-spark-connector_2.11:2.3.2 pyspark_mongo2.py

#to run mongo shell run mongo | show dbs (to show all the databases | use db_name | show collections | db.users.find( {user_id: 100} )

def parseInput(line):
    fields = line.split('|')
    return Row(user_id = int(fields[0]), age = int(fields[1]), gender = fields[2], occupation = fields[3], zip = fields[4])

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.\
    builder.appName("MongoDBIntegration").\
    getOrCreate()

    # Build RDD on top of users data file
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/mongodb/movies.user")
    
    # Creating new RDD by passing the parser fuction
    users = lines.map(parseInput)
    
    # Convert RDD into a DataFrame
    usersDataset = spark.createDataFrame(users)

    # Write the data into MongoDB
    #it will automatically generate the _id column which is the unique id for that row
    usersDataset.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri","mongodb://127.0.0.1/moviesdata.users")\
        .mode('append')\
        .save()

    # Read it back from MongoDB into a new Dataframe
    readUsers = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    #databae(moviesdata).collection(users) mongo db does not have any concepts like table it has collection which is simliar to 
    #table and contains different sort of documents and the documents can have different schema
    .option("uri","mongodb://127.0.0.1/moviesdata.users")\ 
    .load()

    readUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users WHERE age < 20")
    sqlDF.show()

    # Stop the SparkSession
    spark.stop()