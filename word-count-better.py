import re
from pyspark import SparkContext, SparkConf


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

input = sc.textFile("data/book.txt")
words = input.flatMap(normalizeWords)
wordsCounts = words.countByValue()

for word, count in wordsCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if(cleanWord):
        print(cleanWord.decode() + " " + str(count))
