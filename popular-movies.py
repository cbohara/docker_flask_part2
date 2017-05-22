from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# read in data into RDD
linesRDD = sc.textFile("file:///home/cohara/github/taming_big_data_spark/data/ml-100k/u.data")
# create key-value pair RDD of (movieID, 1)
movieIdRDD = linesRDD.map(lambda x: (int(x.split()[1]), 1))
# count the instances of each movieID
movieCountsRDD = movieIdRDD.reduceByKey(lambda x, y: x + y)

# now flip so the keys are the values and the values are the keys
flippedRDD = movieCountsRDD.map(lambda xy: (xy[1], xy[0]))
# sort RDD by count of number of occurances
sortedMoviesRDD = flippedRDD.sortByKey()
# finally call action to return python as an array of tuples
results = sortedMoviesRDD.collect()
print(type(results))

# print the movieID and total count
for result in results:
    print(result)
