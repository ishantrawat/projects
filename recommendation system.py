# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Movie recommendation system").getOrCreate()

# COMMAND ----------

file_location = "/FileStore/tables/movies.csv"
file_type = "csv"
 
# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","
 
# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
 
display(df)

# COMMAND ----------

df.describe().show()

# COMMAND ----------

from pyspark.sql import functions as F
movie_ids_with_avg_ratings_df = df.groupBy('movieId')\
                                  .agg(F.count(df.rating)\
                                  .alias("count"), F.avg(df.rating)\
                                  .alias("average_movie_ratings"))\
                                  .orderBy(desc("average_movie_ratings"))\
                                  .show(10)                                                                                                                                              

# COMMAND ----------

high_rating_movies = df.groupBy("movieId")\
                       .agg(count("rating")\
                       .alias("rating"))\
                       .orderBy(desc("rating"))\
                       .show(10)

# COMMAND ----------

#Most ratings provided by user
most_ratingsby_user = df.groupBy("userId")\
                       .agg(count("rating")\
                       .alias("highest_count"))\
                       .orderBy(desc("highest_count"))\
                       .show(10)
 

# COMMAND ----------

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

#Train test split
train_1, test_1 = df.randomSplit([0.7, 0.3])
train_2, test_2 = df.randomSplit([0.8, 0.2])

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
als = ALS(userCol="userId", itemCol = "movieId", ratingCol ="rating", coldStartStrategy = "drop",nonnegative=True)

# COMMAND ----------

model_1 = als.fit(train_1)
prediction_1 = model_1.transform(test_1)
prediction_1.show(10)
 

# COMMAND ----------

evaluator_rmse = RegressionEvaluator(metricName="rmse", labelCol="rating",  predictionCol="prediction")
evaluator_mse = RegressionEvaluator(metricName="mse", labelCol="rating",  predictionCol="prediction")
evaluator_mae = RegressionEvaluator(metricName="mae", labelCol="rating",  predictionCol="prediction")
rmse = evaluator_rmse.evaluate(prediction_1)
print("RMSE OF MODEL_1  : ", format(rmse))
mse = evaluator_mse.evaluate(prediction_1)
print("MSE OF MODEL_1  : ", format(mse))
mae = evaluator_mae.evaluate(prediction_1)
print("MAE OF MODEL_1  : ", format(mae))

# COMMAND ----------

model_2 = als.fit(train_2)
prediction_2 = model_2.transform(test_2)
prediction_2.show(10)

# COMMAND ----------

rmse = evaluator_rmse.evaluate(prediction_2)
print("RMSE  OF MODEL_2 : ", format(rmse))
mse = evaluator_mse.evaluate(prediction_2)
print("MSE  OF MODEL_2 : ", format(mse))
mae = evaluator_mae.evaluate(prediction_2)
print("MAE  OF MODEL_2  : ", format(mae))

# COMMAND ----------

from pyspark.ml.tuning import ParamGridBuilder, CrossValidator,TrainValidationSplit
paramGrid = (ParamGridBuilder()\
             .addGrid(als.regParam, [0.5, 1, 0.3])\
             .addGrid(als.rank,[6,8,10])\
             .addGrid(als.maxIter, [10,15]))\
             .build()
(train,test) = df.randomSplit([0.8,0.2], seed = 33)

trainvs = TrainValidationSplit(estimator=als,estimatorParamMaps=paramGrid, evaluator=evaluator_rmse,trainRatio = 0.8)
model = trainvs.fit(train)
model_best = model.bestModel
prediction_ = model_best.transform(test)
rmse = evaluator_rmse.evaluate(prediction_)
print("RMSE : ", format(rmse))
print("Rank : ", model_best._java_obj.parent().getRank()), 
print("Max Iteations: ", model_best._java_obj.parent().getMaxIter()) 
print("Regularization: ",model_best._java_obj.parent().getRegParam()) 


# COMMAND ----------

#User 11
user_recommedation= model_best.recommendForUserSubset(df.filter('userId=11')\
                              .sort('rating',ascending=False),50)
recom =(user_recommedation.select("userId",explode("recommendations")\
                           .alias("new_recommendation"))\
                           .select("userId","new_recommendation.*"))\
                           .sort('rating',ascending=False)
recom.select("movieId","rating","userId")
new_recommendation_forUser11 = recom.join(df.filter('userId=11')\
                                    .sort('rating',ascending=False),on=["movieId"],how = 'left_anti')
new_recommendation_forUser11.show(15)

# COMMAND ----------

#User 23
user_recommedation= model_best.recommendForUserSubset(df.filter('userId=23')\
                              .sort('rating',ascending=False),50)
recom =(user_recommedation.select("userId",explode("recommendations")\
                           .alias("new_recommendation"))\
                           .select("userId","new_recommendation.*"))\
                           .sort('rating',ascending=False)
recom.select("movieId","rating","userId")
new_recommendation_forUser11 = recom.join(df.filter('userId=23')\
                                    .sort('rating',ascending=False),on=["movieId"],how = 'left_anti')
new_recommendation_forUser11.show(15)
