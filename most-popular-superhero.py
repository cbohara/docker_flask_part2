from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    """Count number of friends"""
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    """ Return (superhero ID, name) RDD"""
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

# read in txt file with ID and name
names = sc.textFile("file:///home/cohara/github/taming_big_data_spark/data/Marvel-Names.txt")
# create key-value RDD (ID, name)
namesRdd = names.map(parseNames)

# each ID is followed by ID that represent friends
# ex: 345 567 435 223 means hero ID 345 has 4 friends
lines = sc.textFile("file:///home/cohara/github/taming_big_data_spark/data/Marvel-Graph.txt")
# returns RDD containing (superhero id, number of friends)
pairings = lines.map(countCoOccurences)
# a superhero id can have more than 1 entry = why we need to reduceByKey
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
# flip so we now have (number of friends, superhero id)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))

# more efficient to search for max value if it is a key
# will return (number of friends, superhero id) tuple
mostPopular = flipped.max()

# input superhero ID to lookup action in namesRDD > returns a list of a single string > extract that value 
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
