from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("spark://129.63.16.205:7077").setAppName("Popular Movies With Names")
sc = SparkContext(conf = conf)

def loadMovieNames():
    movieNames = {}
    with open("/data/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///data/ml-100k/u.data")
movies = lines.map(lambda x : ((int)(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y : x + y)

flipped = movieCounts.map(lambda (x, y) : (y, x))
sortedMovies = flipped.sortByKey()


sortedMoviesWithNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))
results = sortedMoviesWithNames.collect()

for result in results:
    print result