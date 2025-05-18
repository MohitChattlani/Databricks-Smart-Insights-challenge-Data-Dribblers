# Databricks notebook source
bronze_layer_flight_schedule_df=spark.table("bronze_lyr.flight_schedule")

# COMMAND ----------

display(bronze_layer_flight_schedule_df)

# COMMAND ----------

#changing date columns
from pyspark.sql.functions import col,to_date

silver_layer_flight_schedule_df=bronze_layer_flight_schedule_df\
    .withColumn("validFrom",to_date(col("validFrom"),"dd-MM-yyyy"))\
    .withColumn("validTo",to_date(col("validTo"),"dd-MM-yyyy"))

# COMMAND ----------

#handling days of week
from pyspark.sql.functions import when, col

days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

for day in days:
    silver_layer_flight_schedule_df = silver_layer_flight_schedule_df.withColumn(day[:3], when(col("dayOfWeek").contains(day), 1).otherwise(0))

# COMMAND ----------

display(silver_layer_flight_schedule_df)

# COMMAND ----------

silver_layer_flight_schedule_df.count()

# COMMAND ----------

silver_layer_flight_schedule_df.write.saveAsTable("silver_lyr.flight_schedule",mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_lyr.flight_schedule

# COMMAND ----------

# MAGIC %sql
# MAGIC describe silver_lyr.flight_schedule;