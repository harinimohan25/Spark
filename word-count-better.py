from pyspark import SparkConf, SparkContext
import re

def normalize(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///C:/Users/harin/OneDrive/Documents/GitHub/Spark/Book.txt")
words = input.flatMap(normalize) 
#wordCounts = words.countByValue() # WordCounts is a type of dictionary. Not an RDD anymore

# Below is a way of implementing the Countbyvalue using map function 
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    counts = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + "--> " + str(counts))
