from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("spark://129.63.16.205:7077").setAppName("SuperHero Degrees of Seperation")
sc = SparkContext(conf = conf)

startCharacterID = 5306 #Spider Man
targetCharacterID = 14 #ADAM 3,031

hitCounter = sc.accumulator(0)

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))
    color = "W"
    distance = 9999

    if (heroID == startCharacterID):
        color = "G"
        distance = 0

    return (heroID, (connections, distance, color))

def createStartingRDD():
    inputFile = sc.textFile("file:///data/marvel/marvelgraph.txt")
    return inputFile.map(convertToBFS)

def BFSMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if (color == "G"):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = "G"
            if (targetCharacterID == connection):
                hitCounter.add(1)
            

itrRDD = createStartingRDD()
for interation in range(0, 10):
    mapped = itrRDD.flatMap(BFS)


