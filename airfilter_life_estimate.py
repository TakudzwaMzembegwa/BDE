import pyspark 
import matplotlib
from pyspark import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, asc,desc,avg,mean,sum,min,max,datediff,count
#import plotly

spark = SparkSession.builder\
	.master("local").appName("hdfs_test").getOrCreate()

mttf=2500
rpm10=10

airfilterdate='2022-01-01 00:00:00'
print(airfilterdate)
aqschema = StructType() \
                        .add("timestamp","timestamp") \
                        .add("entryid","integer") \
                        .add("PM1_0","double") \
                        .add("PM2_5","double") \
                        .add("PM10","double") \
                        .add("UptimeMinutes","integer") \
                        .add("RSSI_dbm","integer") \
                        .add("Temperature_F","integer") \
                        .add("Humidity_pct","integer") \
                        .add("PM2_5_ATM","double")

aqdata1 = spark.read.format("csv") \
                    .option("header", True) \
                    .option("inferSchema", True) \
                    .load("hdfs://hdp:9000/airquality/ElsiesRiver09_01_2021_05_31_2022.csv", schema=aqschema)

#aqdata.printSchema()
#aqdata.show()
#aqdata.filter(aqdata.timestamp.gt(airfilterdate))
#aqdata=aqdata1.filter("timestamp > date'2021-12-01'")
aqdata=aqdata1.where(aqdata1.timestamp > airfilterdate)

pmmindate=aqdata.select(min("timestamp"))
pmmaxdate=aqdata.select(max("timestamp"))
pm10total=aqdata.select(sum("PM10"))
pmperiod=aqdata.select(datediff(max("timestamp"),min("timestamp")))
pmcount=aqdata.select(count("timestamp"))
pm10total.show()     
pmperiod.show()
pmcount.show()
pmmindate.show()
pmmaxdate.show()
#aqdata.select(sum("PM10").alias("PM_10_TOTAL"),datediff(max("timestamp"),min("timestamp")).alias("period"),count("timestamp")).show()


#aqdata.select(aqdata.columns[4:5]).show()
#aqdata.groupBy("timestamp").agg(sum("PM10").alias("PM10_AVG")).filter(col("PM10_AVG") > 30).sort(desc("timestamp")).show()
#aq_pm10.orderBy(col("timestamp").desc()).show

#aqdata1=aqdata.groupBy("timestamp").avg("temperature_F").alias("temp")

#aqdata1=aqdata1.orderBy(col("timestamp").desc())
#aqdata1.select(aqdata1["timestamp"],aqdata1["avg(temperature_F)"].alias("temp"))
