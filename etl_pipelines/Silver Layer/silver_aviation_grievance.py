# Databricks notebook source
bronze_layer_aviation_grievance_df=spark.table("bronze_lyr.aviation_grievance")

# COMMAND ----------

display(bronze_layer_aviation_grievance_df)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col, cast

silver_layer_aviation_grievance_df=bronze_layer_aviation_grievance_df\
                                   .withColumn("type",regexp_replace(col("type"), "&amp;", "&"))\
                                   .withColumn("totalReceived",col("totalReceived").cast("int"))\
                                   .withColumn("activeGrievancesWithoutEscalation",col("activeGrievancesWithoutEscalation").cast("int"))\
                                   .withColumn("activeGrievancesWithEscalation",col("activeGrievancesWithEscalation").cast("int"))\
                                   .withColumn("closedGrievancesWithoutEscalation",col("closedGrievancesWithoutEscalation").cast("int"))\
                                   .withColumn("closedGrievancesWithEscalation",col("closedGrievancesWithEscalation").cast("int"))\
                                   .withColumn("successfulTransferIn",col("successfulTransferIn").cast("int"))\
                                   .withColumn("grievancesWithoutRatings",col("grievancesWithoutRatings").cast("int"))\
                                   .withColumn("grievancesWithRatings",col("grievancesWithRatings").cast("int"))\
                                    .withColumn("grievancesWithVeryGoodRating",col("grievancesWithVeryGoodRating").cast("int"))\
                                    .withColumn("grievanceswithgoodrating",col("grievanceswithgoodrating").cast("int"))\
                                    .withColumn("grievancesWithOKRating",col("grievancesWithOKRating").cast("int"))\
                                    .withColumn("grievancesWithBadRating",col("grievancesWithBadRating").cast("int"))\
                                    .withColumn("grievancesWithVeryBadRating",col("grievancesWithVeryBadRating").cast("int"))\
                                    .withColumn("twitterGrievances",col("twitterGrievances").cast("int"))\
                                    .withColumn("facebookGrievances",col("facebookGrievances").cast("int"))\
                                    .withColumn("grievancesAdditionalInfoProvided",col("grievancesAdditionalInfoProvided").cast("int"))\
                                    .withColumn("grievancesAdditionalInfoNotProvided",col("grievancesAdditionalInfoNotProvided").cast("int"))\
                                    .withColumn("grievancesWithoutFeedback",col("grievancesWithoutFeedback").cast("int"))\
                                    .withColumn("grievancesWithFeedback",col("grievancesWithFeedback").cast("int"))\
                                    .withColumn("grievancesWithFeedbackIssueNotResolved",col("grievancesWithFeedbackIssueNotResolved").cast("int"))\
                                    .withColumn("grievancesWithFeedbackIssueResolved",col("grievancesWithFeedbackIssueResolved").cast("int"))

# COMMAND ----------

display(silver_layer_aviation_grievance_df)

# COMMAND ----------

silver_layer_aviation_grievance_df.write.saveAsTable("silver_lyr.aviation_grievance",mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_lyr.aviation_grievance;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe silver_lyr.aviation_grievance;