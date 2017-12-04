from __future__ import print_function

import sys
import pprint
from os.path import expanduser, join, abspath
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
	    .enableHiveSupport() \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

sc = SparkContext("local[2]", "NetworkWordCount")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 20)

lines = ssc.socketTextStream("localhost", 3333)
words = lines.map(lambda line: line.split(","))

def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            print("Start of process")
            rdd.foreach(print)
	    spark = getSparkSessionInstance(rdd.context.getConf())
	    rowRdd= rdd.map(lambda x: Row(userId=x[0],movieId=x[1],rating=x[2],t=x[3]))
            rowRdd.foreach(print)
	    wordsDataFrame = spark.createDataFrame(rowRdd)
	    wordsDataFrame.show()
	    wordsDataFrame.write.mode("append").insertInto("movies.ratings")
            print("end of process") 
	except:
            pass

words.foreachRDD(process)

ssc.start()            
ssc.awaitTermination()  
