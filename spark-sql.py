# SparkSession = Spark 2.0 way of working with SparkSQL
from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    """Transform unstructured data into Row objects in order to create DF"""
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

# read in csv file into an RDD
lines = spark.sparkContext.textFile("/home/cohara/github/taming_big_data_spark/data/fakefriends.csv")
# create RDD with structured Row objects
people = lines.map(mapper)

# create dataframe from our RDD of Row objects and cache for continuous use
schemaPeople = spark.createDataFrame(people).cache()
# register the dataframe as a table in memory > allows us to query the dataframe
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# use collect action to retreive and show results  
for teen in teenagers.collect():
  print(teen)

# alternatively we can also use functions instead of SQL queries 
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
