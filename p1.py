import sys
from pyspark import SparkConf, SparkContext
import re

word = sys.argv[1]
conf = SparkConf().setAppName('WordCount')
sc = SparkContext(conf=conf)


def spliter(line):
    return re.sub(r'\W+', ' ', line).split()


def searcher(line):
    if word in line:
        return {line, ''}


sc.textFile('input.txt')

rdd = sc.parallelize()

rdd = rdd.map(spliter())
rdd = rdd.filter(searcher())
rdd = rdd.reduceByKey() \
    .saveAsTextFile("output.txt")
