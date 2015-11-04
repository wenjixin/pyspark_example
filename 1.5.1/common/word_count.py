# coding=utf-8
from pyspark import SparkContext, SparkConf
import time

conf = SparkConf()


def func_map(item):
    time.sleep(60 * 10)
    return item

with SparkContext(conf=conf) as sc:
    data = [1, 2, 3, 4, 5]
    rdd = sc.parallelize(data)
    print rdd.map(func_map).collect()
