import pytest
import pyspark
from myfunctions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

tableName    = "tweets_apple_1"
dbName       = "demo001"
columnName   = "location"
columnValue  = "India"

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

# Create fake data for the unit tests to run against.
# In general, it is a best practice to not run unit tests
# against functions that work with data in production.

schema = StructType([ \
    StructField("username",StringType(),True), \
    StructField("description",StringType(),True), \
    StructField("location",StringType(),True), \
    StructField("following", IntegerType(), True), \
    StructField("followers", IntegerType(), True), \
    StructField("totaltweets", IntegerType(), True), \
    StructField("retweetcount", IntegerType(), True), \
    StructField("text", StringType(), True), \
    StructField("hashtags", StringType(), True), \
  ])

data = [ ("demo01",   "random data", "Japan", 161, 455, 326, 395, "My first tweet", "['AirPods', 'Apple', 'Tech']" ), \
         ("demo02",   "random data 2", "Germany", 81, 955, 126, 795, "My second tweet", "['Macbook', 'iphone', 'Technology']")  ]

df = spark.createDataFrame(data, schema)

# Does the table exist?
def test_tableExists():
  assert tableExists(tableName, dbName) is True

# Does the column exist?
def test_columnExists():
  assert columnExists(df, columnName) is True

# Is there at least one row for the value in the specified column?
def test_numRowsInColumnForValue():
  assert numRowsInColumnForValue(df, columnName, columnValue) > 0