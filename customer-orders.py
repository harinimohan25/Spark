from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("CustomerSpending")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    custID = fields[0]
    itemAmt = float(fields[2])
    return (custID, itemAmt)

lines = sc.textFile("file:///C:/Users/harin/OneDrive/Documents/GitHub/Spark/customer-orders.csv")
data = lines.map(parseLine)
AmtSpent = data.reduceByKey(lambda x, y: (x + y))
flipped = AmtSpent.map(lambda x: (x[1], x[0]))
totalByCustomerSorted = flipped.sortByKey()
results = totalByCustomerSorted.collect();

for line in results:
    print("Customer ID: " + str(line[1]) + " spent $" + str(line[0]))

