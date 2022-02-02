# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col as col
spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()
file_location = "/FileStore/tables/integer-2.txt"
odd_even=spark.read.format("text").load(file_location)
odd_even.show()

# COMMAND ----------

odd_even.groupBy((col("value")%2!=0).alias("Number_is_Odd")).count().show()

# COMMAND ----------

odd_even.groupBy((col("value")%2==0).alias("Number_is_Even")).count().show()
