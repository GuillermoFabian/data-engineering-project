#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_2017', required=True)
parser.add_argument('--input_2018', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_2017 = args.input_2017
input_2018 = args.input_2018
output = args.output

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-central1-244522147737-m8nwrtom')

df_2017 = spark.read.option("header", "true").csv(input_2017)

df_17 = df_2017 \
    .withColumnRenamed('starttime', 'start_time') \
    .withColumnRenamed('stoptime', 'stoptime')

df_2018 = spark.read.option("header", "true").csv(input_2018)

df_2018 = df_2018 \
    .withColumnRenamed('starttime', 'start_time') \
    .withColumnRenamed('stoptime', 'stoptime')

common_colums = [
    'tripduration',
    'start_time',
    'stoptime',
    'usertype',
    'gender',
    'startstationid'
]

df_17_sel = df_17 \
    .select(common_colums) \
    .withColumn('usertype', F.lit('18'))

df_18_sel = df_2018 \
    .select(common_colums) \
    .withColumn('usertype', F.lit('18'))

df_bikes_data = df_17_sel.unionAll(df_18_sel)

df_bikes_data.registerTempTable('bikes_data')

print(df_bikes_data.columns)

print('LLEGA AL SELECT ---------------------------')

df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    startstationid AS station_id_start,
    date_trunc('day', start_time) AS duration_day, 
    usertype, 
    gender,
    -- Revenue calculation 
    SUM(tripduration) AS total_daily_trip_duration,
   
    -- Additional calculations
    max(tripduration) AS max_trip_duration,
    min(tripduration) AS min_trip_duration
FROM
    bikes_data
GROUP BY
    1, 2, 3, 4
""")

df_result.write.format('bigquery') \
    .option('table', output) \
    .option('partitionField', 'duration_day') \
    .save()
