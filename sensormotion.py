import pyspark 
import matplotlib
import string
import json
import pandas as pd
#import sensormotion as sm
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType
from pyspark.sql.functions import *
from datetime import datetime
from datetime import timedelta

from pyspark import SparkConf
from pyspark.context import SparkContext

spark = SparkSession.builder\
	.master("local").appName("hdfs_phone").getOrCreate()
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
df = spark.read.text('hdfs://hdp:9000/home/hadoop/logstash.log')


df2=df.withColumn("tstamp", substring(col('value'),1,25)) \
                    .withColumn("c1", split(col('value'),' ')[3]) \
                    .withColumn("c2", split(col('value'),' ')[4]) \
                    .withColumn("c3", split(col('value'),' ')[5]) 
df2=df2.withColumn("co1", regexp_replace(col('c1'),",",'')) \
                    .withColumn("co2", regexp_replace("c2",",",'')) \
                    .withColumn("co3", regexp_replace("c3",",",'')) 
df3=df2.select("tstamp",substring("co1",2,5).alias("c1"),col("co2").alias("c2"),split(col('co3'),']')[0].alias("c3"))        
df3.show(truncate=False)
