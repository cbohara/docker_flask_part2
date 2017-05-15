from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("maxTemp")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temp = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temp)

linesRDD = sc.textFile("file:///Users/cbohara/code/taming_big_data_spark/data/1800.csv")
parseLinesRDD = linesRDD.map(parseLine)
maxTempsRDD = parseLinesRDD.filter(lambda x: "TMAX" in x[1])
stationTempRDD = maxTempsRDD.map(lambda x: (x[0], x[2]))
maxTempForYearRDD = stationTempRDD.reduceByKey(lambda x, y: max(x, y))
list_of_tuples = maxTempForYearRDD.collect()
for value in list_of_tuples:
    print(value[0] + " {:.2f}F".format(value[1]))
