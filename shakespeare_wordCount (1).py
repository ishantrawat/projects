# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Demo").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", 2)
RDD = spark.sparkContext.textFile("/FileStore/tables/shakespeare_1.txt")

# COMMAND ----------

RDD.collect()

# COMMAND ----------

words = spark.sparkContext.textFile("/FileStore/tables/shakespeare_1.txt").flatMap(lambda line: line.split(" "))
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

# COMMAND ----------

wordCounts.collect()

# COMMAND ----------

for WORD in ["SHAKESPEARE","GUTENBERG","WILLIAM","LIBRARY","COLLEGE","WORLD","THIS"]:
  words_filter = wordCounts.filter(lambda x: WORD in x)
  print(words_filter.collect())

# COMMAND ----------

output = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(True).take(20)
print("Bottom 20 words are :")
for (word, count) in output:
  print(count, word )

# COMMAND ----------

output = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(False).take(20)
print("Top 20 words are :")
for (count, word) in output:
  print(word, count)
