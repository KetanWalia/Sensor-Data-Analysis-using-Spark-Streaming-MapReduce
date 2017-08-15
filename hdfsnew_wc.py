#

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in new text files created in the given directory
 Usage: hdfs_wordcount.py <directory>
   <directory> is the directory that Spark Streaming will use to find and read new text files.

 To run this on your local machine on directory `localdir`, run this example
    $ bin/spark-submit examples/src/main/python/streaming/hdfs_wordcount.py localdir

 Then create a text file in `localdir` and the words in the file will get counted.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, 30)
    ssc.checkpoint("hdfs:///user/kw475/check")
    lines = ssc.textFileStream(sys.argv[1])
    words = lines.map(lambda line: line.split(","))\
	    .map(lambda a: ((int(a[1]),int(a[2])),int(a[3])))\
	    .reduceByKeyAndWindow(lambda a,b:[a]+[b],90,30)
		

   # words.pprint()
    def process(time, rdd):
        print("========= %s =========" % str(time))

        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w[0], val=w[1]))
	    print(rowRdd.count())
 		    
 	    #row_df = rdd.toDF()
	    # row.df_show()

	    #print(rowRdd.collect())
            wordsDataFrame = spark.createDataFrame(rowRdd)

            # Creates a temporary view using the DataFrame.
            wordsDataFrame.createOrReplaceTempView("words")

            # Do word count on table using SQL and print it
            wordCountsDataFrame = \
                spark.sql("SELECT word, MAX(val)-MIN(val) as var from words group by word")
	    
            wordCountsDataFrame.show()
	   # wordCountsDataFrame.toPandas().to_csv('hdfs://user/kw475/')
	    wordCountsDataFrame.write.json("hdfs:///user/kw475/getfile")
	
        except Exception, e:
            print('======================ERROR==========================')
            print (str(e))
            pass
               

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()


