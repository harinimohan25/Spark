from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Ratings")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///C:/Users/harin/OneDrive/Documents/GitHub/Spark/ml/ratings.csv")
tagsheader = lines.first()
lines = lines.filter(lambda row: row != tagsheader)
#header = sc.parallelize([tagsheader])
#lines = tags.subtract(header)
ratings = lines.map(lambda x: x.split(',')[2])
results = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(results.items()))
for key, val in sortedResults.iteritems():
        print "%s, %i" % (key, val)
