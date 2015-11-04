# coding=utf-8
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# create sc
sc = SparkContext(appName='hdfs_file_streaming')

num = 10

ssc = StreamingContext(sc, 10)

lines = ssc.textFileStream('/user/hdfs/rawlog/www_vblogmo2pao0ap5yjlj8_nginx/')
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()

ssc.start()
ssc.awaitTermination()
