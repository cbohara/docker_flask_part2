from pyspark import SparkConf, SparkContext

def loadMovieNames():
    """Return dictionary with movieID as key and the movie title as the value."""
    movieNames = {}
    with open("data/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# pass in the {'movieID': 'movieTitle'} dictionary in order to broadcast the dictionary object to all executors
nameDict = sc.broadcast(loadMovieNames())

# read in data into RDD
linesRDD = sc.textFile("file:///home/cohara/github/taming_big_data_spark/data/ml-100k/u.data")
# create key-value pair RDD of (movieID, 1)
movieIdRDD = linesRDD.map(lambda x: (int(x.split()[1]), 1))
# count the instances of each movieID
movieCountsRDD = movieIdRDD.reduceByKey(lambda x, y: x + y)

# flip so the keys are the values and the values are the keys
flippedRDD = movieCountsRDD.map(lambda xy: (xy[1], xy[0]))
# sort RDD by count of number of occurances
sortedMoviesRDD = flippedRDD.sortByKey()

# call .value() on the nameDict object that was broadcasted to each executor in order to look up the movie title for the movieID
sortedMoviesWithNamesRDD = sortedMoviesRDD.map(lambda (count, movieID): (nameDict.value[movieID], count))

# finally call action to return python as a list of tuples
results = sortedMoviesWithNamesRDD.collect()
print(type(results))

# print the movieID and total count
for result in results:
    print(result)
