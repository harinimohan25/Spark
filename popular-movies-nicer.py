from pyspark import SparkConf, SparkContext
import csv

def loadMovieNames():
    movieNames = {}
    with open("ml/movies.csv",'r') as f:
        csvreader = csv.reader(f)
        next(csvreader)
        for line in csvreader:
             movieNames[int(line[0])] = line[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///C:/Users/harin/OneDrive/Documents/GitHub/Spark/ml/ratings.csv")
tagsheader = lines.first()
lines = lines.filter(lambda row: row != tagsheader)
movies = lines.map(lambda x: (int(x.split(',')[1]), 1)) # Taking only the movie id column and then value for that key is just 1
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
