import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# use regex to break up text based on words and then return as lowercase for consistency
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

RDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/Book.txt")
# apply regex function to separate out all words in the book
wordsRDD = RDD.flatMap(normalizeWords)
# now apply countByValue action to get the count per word as a Python dictionary
wordCounts = wordsRDD.countByValue()
# loop through dictionary and print
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
