from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("spark://129.63.16.205:7077").setAppName("Super Hero")
sc = SparkContext(conf = conf)

def coutCoOccurences(line):
    fields = line.split()
    return (int(fields[0]), len(fields) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf-8"))

names = sc.textFile("file:///data/marvel/marvelnames.txt")
namesRDD = names.map(parseNames)

lines = sc.textFile("file:///data/marvel/marvelgraph.txt")
pairings = lines.map(coutCoOccurences)

totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda (x, y) : (y, x))

mostPop = flipped.max()
mostPopName = namesRDD.lookup(mostPop[1])[0]

print mostPopName