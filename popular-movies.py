from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///C:/Users/harin/OneDrive/Documents/GitHub/Spark/ml/ratings.csv")
tagsheader = lines.first()
lines = lines.filter(lambda row: row != tagsheader)
movies = lines.map(lambda x: (int(x.split(',')[1]), 1)) # Taking only the movie id column and then value for that key is just 1
movieCounts = movies.reduceByKey(lambda x, y: x + y) # Summing up per movie how many times its mentioend in the rating doc
flipped = movieCounts.map(lambda xy: (xy[1],xy[0]) ) # Flip the data so that the sum is the key and movie is the values
sortedMovies = flipped.sortByKey() # Sorting so that we can see which movie is popular

results = sortedMovies.collect()

for result in results:
    print(result)
