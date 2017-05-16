from pyspark import SparkConf, SparkContext

# setup Spark app
conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

# parse input CSV 
def splitCSV(line):
    csv_to_list = line.split(",")
    custID = int(csv_to_list[0])
    spending = float(csv_to_list[2])
    return (custID, spending)

# read in CSV into RDD
linesRDD = sc.textFile("///Users/cbohara/code/taming_big_data_spark/data/customer-orders.csv")
# each RDD value is (custID, spending) tuple
custIDspendingRDD = linesRDD.map(splitCSV)
# sum spending per customerID
sumPerCustomerRDD = custIDspendingRDD.reduceByKey(lambda accum, x: accum + x)
# use collect to return (custID, total spending) as a list of tuples
results = sumPerCustomerRDD.collect()
# print results
for result in results:
    print("Customer " + str(result[0]) + " spent ${:.2f}".format(result[1]))
