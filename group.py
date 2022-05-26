# -*- coding: utf-8 -*-
"""
Created on Thu May 26 12:46:48 2022

@author: Tee~Kay
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.master("local[*]") \
        .appName("bde.com") \
        .config("spark.port.maxRetries", "1000") \
        .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
# Comments contain information that might be useful for the report
def first_read(path: str):
     # original count = 402314
     df = spark.read.option("multiline","true").json(path)
     # after removing nulls count = 278330
     df = df.withColumn("activity", explode("activity")).select("*", col("activity")["activity"].alias("activity1"))
     # after splitting by transport mode count = 852808
     df = df.withColumn("activity1", explode("activity1")).select("*", col("activity1")["confidence"].alias("confidence"), col("activity1")["type"].alias("type"))
     return df
def transport_filter(df):
    # After keeping only type = "IN_VEHICLE" with confidence > 60 count = 12440
    df = df.filter((df.type == "IN_VEHICLE") & (df.confidence > 60))
    return df         
try:
    df = first_read("file:///{path}/Documents/CSc/CSC736/MapRecords.json")[["accuracy", "deviceDesignation", "deviceTag", "latitudeE7", "longitudeE7", "source", "timestamp", "confidence", "type"]]
    df = transport_filter(df)
    df.write.json("hdfs://{host}:{port}/GPSLocations")
finally:
    spark.stop()