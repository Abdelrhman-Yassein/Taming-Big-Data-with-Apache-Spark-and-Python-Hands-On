from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Customer")
sc = SparkContext(conf=conf)


def parseLine(text):
    fields = text.split(",")
    return (int(fields[0]), float(fields[2]))


input = sc.textFile("data/customer-orders.csv")
customerOrders = input.map(parseLine)
custreduc = customerOrders.reduceByKey(lambda x, y: x+y)
results = custreduc.collect()
for result in results:
    print(result)
