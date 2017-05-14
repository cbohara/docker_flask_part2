from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    # for each line split input text into the fields list
    fields = line.split(',')
    # grab the 3rd value in the list and save it as an int 
    age = int(fields[2])
    # grab the 4th value in the list and save it as an int 
    numFriends = int(fields[3])
    # return as the (key, value) pair
    return (age, numFriends)

linesRDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/fakefriends.csv")
# create key, value RDD
keyValueRDD = linesRDD.map(parseLine)

# if I am not modifying the keys, use mapValues() instead of map()
# this is important for efficiency
# allows Spark to maintain the original partitioning in the RDD + not have to shuffle the data around

# mapValues() will only pass in the value to the lambda function
# keyValueRDD.mapValues(lambda x: (x, 1)) will pass in the number of friends as x
# the result of each mapValues() is (the original key age, (the original value number of friends, 1))
# ex: (33, (385, 1)) 
#     (33, (2, 1))
# there are two 33 year olds in our data set, one with 385 friends and one with 2 friends
# the logic behind this step is to be able to count the number of 33 year olds in the dataset
# this will allow us to divide the total number of friends 33 year olds have by the # of 33 year olds
ageCounterRDD = keyValueRDD.mapValues(lambda x: (x, 1))

# the entire result of keyValueRDD.mapValues(lambda x: (x, 1)) is a new RDD
# then we will apply the reduceByKey() method to this new RDD 
# reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]) will take x as the number of friends and y as the count
# ex: (original key age, (total # of friends for age group, total # of 33 year olds)
# ex output (33, (387, 2))
totalsByAgeRDD = ageCounterRDD.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# totalsByAgeRDD each RDD value is setup like (33, (387, 2))
# pass in the value = tuple = (387, 2) into the lambda function
# divide total number of friends / total # of 33 year olds = avg num of friends for each age
# returns RDD containing (original key age, avg number of friends)
# ex: (33, 193.5)
averagesByAgeRDD = totalsByAgeRDD.mapValues(lambda x: x[0] / x[1])

# finally apply collect() action to RDD to return the (age, avg) RDD as a list of objects 
results = averagesByAgeRDD.collect()
# for each tuple in the list, print the tuple (age, avg)
for result in results:
    print(result)
