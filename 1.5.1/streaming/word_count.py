# coding=utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# create a streamingcontext
sc = SparkContext(appName='test')
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream('127.0.0.1', 9999)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()

pairs.count()


ssc.start()
ssc.awaitTermination()
