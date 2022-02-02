# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Demo").getOrCreate()
RDD = spark.sparkContext.textFile("/FileStore/tables/salary.txt")

# COMMAND ----------

RDD.collect()

# COMMAND ----------

rdd1 = RDD.map(lambda a: a.split(" "))
rdd2 = rdd1.map(lambda a: (a[0],int(a[1])))
rdd_salary = rdd2.reduceByKey(lambda a,b: a+b)
rdd_salary.collect()
