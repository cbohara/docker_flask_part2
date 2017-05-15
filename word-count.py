from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# each value in the rdd is about a paragraph of the book
RDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/Book.txt")

# using flatMap() we can split the value into a list of words
# each item in the list of words will be returned to the flatMap() function
# and each word will become an entry of its own in the wordsRDD
wordsRDD = RDD.flatMap(lambda x: x.split())

# countByValue() action returns the count of each unique value in the RDD as a dictionary of (value, count) pairs 
wordCounts = wordsRDD.countByValue()

# loop through dictionary
for word, count in wordCounts.items():
    # ensure words will be displayed without errors even if they contain special chars
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
