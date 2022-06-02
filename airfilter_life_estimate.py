# 2022/06/02 YB
# Calculate the actual lifetime of airfilter based on air sensor data that measures dust levels / pm10
#
import pyspark 
import matplotlib
from pyspark import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType
from pyspark.sql.functions import *


spark = SparkSession.builder\
	.master("local").appName("hdfs_test").getOrCreate()

# mean time to failure as per manufacturer spec, with expected average of 10 parts per million for dust / pm10
mttf=2500
rpm10=10
expected_days=int(mttf/24)
print('expected_days ' + str(expected_days))

# airfilter installation date -- needs to be input variable?
airfilterdate='2022-03-01 00:00:00'

print('new airfilter installed on ' + str(airfilterdate))

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

# filter the data to only after the airfilter was installed
aqdata=aqdata1.where(aqdata1.timestamp > airfilterdate)

# for testing
pmmindate=aqdata.select(min("timestamp"))
pmmaxdate=aqdata.select(max("timestamp"))
pm10total=aqdata.select(sum("PM10"))
pmperiod=aqdata.select(datediff(max("timestamp"),min("timestamp")))
pmcount=aqdata.select(count("timestamp").alias("pmcount"))
#pm10total.show()     
#pmperiod.show()
#pmcount.show()
#pmmindate.show()
#pmmaxdate.show()

# Do the math
aqdata2=aqdata.groupBy(to_date("timestamp").alias("tdate")).agg(sum("PM10").alias("PM10_SUM"),count("timestamp").alias("num_count"),avg("PM10").alias("PM_10_avg"))

aqdata2=aqdata2.withColumn('estpartPM10',((col('num_count') * lit(10)))).withColumn('estdiff',((col('PM10_SUM') - col('num_count') * lit(10))))
                
aqdata2=aqdata2.orderBy(col("tdate"))
windowSpec=Window.partitionBy().orderBy("tdate")
windowSpec2=Window.partitionBy().orderBy("tdate").rowsBetween(Window.unboundedPreceding,Window.currentRow)
aqdata2=aqdata2.withColumn("days_used",row_number().over(windowSpec))
aqdata2=aqdata2.withColumn("pm10_rolling_total", sum("PM10_SUM").over(windowSpec2)) \
                .withColumn("estpm10_rolling_total", sum("estpartPM10").over(windowSpec2)) \
                .withColumn("estdiff_rolling_total", sum("estdiff").over(windowSpec2))
aqdata2=aqdata2.withColumn("days_remaining", lit(expected_days) - col("days_used"))
aqdata2=aqdata2.withColumn("actual_days_remaining", col("days_remaining") - (col("estdiff_rolling_total")/(col("estpm10_rolling_total")/col("days_used"))))
#aqdata2.show()
aqdata3=aqdata2.select(col("tdate").alias("Date"),col("days_remaining"), \
               col("actual_days_remaining").cast('int'), \
               col("pm10_rolling_total").cast('int').alias("pm10_measured_total"), \
               col("estpm10_rolling_total").cast('int').alias("pm10_estimated_total"), \
               col("PM_10_avg").cast('int').alias("measured_pm10_average")  
               )
aqdata3.filter(aqdata3['actual_days_remaining']==0).select(first(col("Date")).alias("AirfilterExpirationDate")).show()

# plot the graphs
aqdata4=aqdata3.toPandas()
aqdata4.plot(x='Date',y=['days_remaining','actual_days_remaining'])
aqdata4.plot(x='Date',y=['pm10_measured_total','pm10_estimated_total'],ylim=(0,1500000))
aqdata4.plot(x='Date',y='measured_pm10_average',ylim=(0,100))
