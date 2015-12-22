from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("spark://129.63.16.205:7077").setAppName("Popular Movies")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///data/ml-100k/u.data")
movies = lines.map(lambda x : ((int)(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y : x + y)

flipped = movieCounts.map(lambda (x, y) : (y, x))
sortedMovies = flipped.sortByKey()
results = sortedMovies.collect()

for result in results:
    print result