from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("spark://129.63.16.205:7077").setAppName("Customer Amount")
sc = SparkContext(conf = conf)

def parseLines(line):
    fields = line.split(",")
    customerID = fields[0]
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile("file:///data/customer-orders.csv")
rdd = lines.map(parseLines)
totalAmounts = rdd.reduceByKey(lambda x, y : x + y).map(lambda (x, y) : (y, x)).sortByKey()
results = totalAmounts.collect()

for customer in results:
    print customer[1] + ":" + str(customer[0])