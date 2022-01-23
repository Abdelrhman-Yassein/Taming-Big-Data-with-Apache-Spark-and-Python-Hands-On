from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile("data/1800.csv")
parseLines = lines.map(parseLine)
maxTemp = parseLines.filter(lambda x: "TMAX" in x[1])
stationTemp = maxTemp.map(lambda x: (x[0], x[2]))
maxTemps = stationTemp.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()


for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
