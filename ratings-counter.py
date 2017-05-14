# SparkConf allows you to configure SparkContext
# SparkContext provides the methods needed to create an RDD
from pyspark import SparkConf, SparkContext
import collections

# save SparkConf configurations into conf object
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# create SparkContext with configuration object
sc = SparkContext(conf = conf)

# each line in text file will be one value in RDD
linesRDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/ml-100k/u.data")

# for each line in lines RDD, split the line into a list of words
# then return the 3rd word (movie rating) as the new value
# the ratings RDD will store the movie rating only
ratingsRDD = linesRDD.map(lambda x: x.split()[2])

# call action method countByValue() to get desired result in the form of a tuple
# ex: (3,2) = the value 3 occured 2 times in the dataset
# this is an action = returning Python object! (not an RDD!)
result = ratingsRDD.countByValue()

# regular Python code
# use collections package to create an ordered dictionary
# looping through ordered dictionary to print each key and its associated value
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
