from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# ex line from csv: ITE00100554,18000101,TMIN,-148,,,E,
def parseLine(line):
    fields = line.split(',')
    # keep station ID in order to find the min temp per station
    stationID = fields[0]
    # need to know the observation type - specifically looking for TMIN
    entryType = fields[2]
    # convert temp C to F 
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    # return tuple
    return (stationID, entryType, temperature)

# read in weather data to find the minimum temp in the year 1800 for each weather station
linesRDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/1800.csv")
# resulting RDD will contain values with (stationID, entryType, temperature) 
parsedLinesRDD = linesRDD.map(parseLine)
# filters each row in RDD and returns the row for the minTempsRDD if the entryType == TMIN
minTempsRDD = parsedLinesRDD.filter(lambda x: "TMIN" in x[1])
# from our RDD containging only min temp rows of data, create new RDD with only (stationID, temperature)
stationTempsRDD = minTempsRDD.map(lambda x: (x[0], x[2]))
# for each each weather station, compare two temperatures until you find the minimum temp per weather station   
# reduceByKey takes the accumulated value for the given key and summing it with the next value of that key
minTempsRDD = stationTempsRDD.reduceByKey(lambda x, y: min(x,y))
# use collect action to return (stationID, minimum temperature in the year 1800) as a list of tuples
results = minTempsRDD.collect()
# loop through list and for each tuple 
for result in results:
    # print the stationID and the temp (2 decimal places)
    print(result[0] + "\t{:.2f}F".format(result[1]))
