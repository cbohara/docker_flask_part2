import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# use regex to parse words
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

RDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/Book.txt")
# RDD contains each word in the book
wordsRDD = RDD.flatMap(normalizeWords)

# wordsRDD.map() will return a RDD (word, 1)
# for each word reduceByKey() will count the number of instances and return a tuple (word, # of instances)
wordCountsRDD = wordsRDD.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# wordCountsRDD.map() willl return the tuple (# of instances, word)
# then sortByKey() will sort the key, which is now the number of instances
wordCountsSortedRDD = wordCountsRDD.map(lambda x: (x[1], x[0])).sortByKey()

# collect() action will return list of tuples [(5, them), (2, words)]
results = wordCountsSortedRDD.collect()

# print word and count for each item in the list
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word.decode() + ": " + count)
