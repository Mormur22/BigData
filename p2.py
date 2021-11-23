from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setAppName('WordCount')
sc = SparkContext(conf = conf)

rdd = sc.textFile('input.txt')
pairs=rdd.map(lambda line: re.search(r"(?<=\").+?(?=\")",line).group().split(' ')).collect()
urls= pairs()
rdd_urls= sc.parallelize(pairs)
map(lambda x : x[6])