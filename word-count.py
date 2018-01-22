from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///C:/Users/harin/OneDrive/Documents/GitHub/Spark/Book.txt")
words = input.flatMap(lambda x: x.split()) # Map will transform from one rdd to another rdd the entries wont change. The count will rename the same. 
# Flatmap can blow to multiple entires
wordCounts = words.countByValue()
print(wordCounts)
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
