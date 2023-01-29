from pyspark.sql import SparkSession
from pyspark.sql.functions import *
#from pyspark.sql.types import *

import sys
import datetime

if __name__ == "__main__":
    print("PySpark Mysql Application Started ...")

    if len(sys.argv) == 2:
        get_data_date = sys.argv[1]
        print("get_data_date: " + get_data_date)
    else:
        print("Failed, provide the get_data_date to start.")
        exit(1)

    '''spark = SparkSession \
        .builder \
        .appName("PySpark Mysql") \
        .master("local[*]") \
        .config("spark.jars", "file:///C://Users//Thiruppathi//PycharmProjects//pyspark_mysql_demo//mysql-connector-java-5.1.46.jar") \
        .config("spark.executor.extraClassPath", "file:///C://Users//Thiruppathi//PycharmProjects//pyspark_mysql_demo//mysql-connector-java-5.1.46.jar") \
        .config("spark.executor.extraLibrary", "file:///C://Users//Thiruppathi//PycharmProjects//pyspark_mysql_demo//mysql-connector-java-5.1.46.jar") \
        .config("spark.driver.extraClassPath", "file:///C://Users//Thiruppathi//PycharmProjects//pyspark_mysql_demo//mysql-connector-java-5.1.46.jar") \
        .enableHiveSupport()\
        .getOrCreate()'''

    spark = SparkSession \
        .builder \
        .appName("PySpark Mysql Demo") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    if get_data_date is None:
        current_datetime = datetime.datetime.now()
        start_date = current_datetime.strftime("%Y-%m-%d")
        print("start_date: " + start_date)
        print(type(current_datetime))
        get_datetime = current_datetime + datetime.timedelta(days=-1)
        get_data_date = get_datetime.strftime("%Y-%m-%d")
        print("get_data_date: " + get_data_date)

    mysql_db_driver_class = "com.mysql.jdbc.Driver"
    table_name = "cc_transaction_tbl"
    host_name = "localhost"
    port_no = str(3306)
    user_name = "root"
    password = "root"
    database_name = "ecommerce_db"

    mysql_select_query = None
    mysql_select_query = "(select * from " + table_name + " where date_format(transaction_datetime, '%Y-%m-%d') = '" \
                            + get_data_date + "') as cc_transaction_tbl"

    mysql_jdbc_url = "jdbc:mysql://" + host_name + ":" + port_no + "/" + database_name

    print("Printing JDBC Url: " + mysql_jdbc_url)

    trans_detail_tbl_data_df = spark.read.format("jdbc") \
        .option("url", mysql_jdbc_url) \
        .option("driver", mysql_db_driver_class) \
        .option("dbtable", mysql_select_query) \
        .option("user", user_name) \
        .option("password", password) \
        .load()

    trans_detail_tbl_data_df.show(2, False)

    hdfs_output_path = "/user/hadoop/data/ecommerce_data/transactional_detail" + "/" + get_data_date
    print("Printing hdfs_output_path: " + hdfs_output_path)

    trans_detail_tbl_data_df\
        .write \
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
        .csv(hdfs_output_path, header=True, sep=",")

    print("PySpark Mysql Demo Completed.")