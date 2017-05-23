from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def loadMovieNames():
    """Load in data into dictionary of movieID and movie title"""
    movieNames = {}
    with open("data/ml-100k/u.item", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].encode("utf8")
    return movieNames


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# load up our movie ID -> name dictionary
nameDict = loadMovieNames()

# import unstructured data into RDD 
lines = spark.sparkContext.textFile("file:////home/cohara/github/taming_big_data_spark/data/ml-100k/u.data")
# convert it to a RDD of Row objects with a single column (movieID)
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
# convert RDD to a DataFrame
movieDataset = spark.createDataFrame(movies)

# some SQL-style magic to sort all movies by popularity in one line!
topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()

# show the results at this point:

#|movieID|count|
#+-------+-----+
#|     50|  584|
#|    258|  509|
#|    100|  508|

topMovieIDs.show()

# take the top 10
top10 = topMovieIDs.take(10)

# print the results
print("\n")
for result in top10:
    # Each row has movieID, count as above.
    print("%s: %d" % (nameDict[result[0]], result[1]))

# stop the session
spark.stop()
