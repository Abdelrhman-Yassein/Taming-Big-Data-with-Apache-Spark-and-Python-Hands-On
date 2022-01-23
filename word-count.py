from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("data/book.txt")
words = input.flatMap(lambda x: x.split())
wordsCounts = words.countByValue()

for word, count in wordsCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if(cleanWord):
        print(cleanWord.decode() + " " + str(count))
