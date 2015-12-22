import re

from pyspark import SparkConf, SparkContext

def normalizedWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("spark://129.63.16.205:7077").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///data/book.txt")
words = input.flatMap(normalizedWords)
wordCounts = words.map(lambda x : (x, 1)).reduceByKey(lambda x, y : x + y)
wordCountsSorted = wordCounts.map(lambda (x, y) : (y, x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print word + ":\t\t" + count
