# Databricks notebook source
bronze_layer_flight_schedule_df=spark.table("bronze_lyr.flight_schedule")

# COMMAND ----------

from pyspark.sql.functions import col, trim, length

bronze_layer_flight_schedule_filtered_df = bronze_layer_flight_schedule_df.where(
    (col("scheduledDepartureTime").isNotNull()) &
    (length(trim(col("scheduledDepartureTime"))) > 0) &
    (col("scheduledArrivalTime").isNotNull())  &
    (length(trim(col("scheduledArrivalTime")))  > 0)
)

# COMMAND ----------

bronze_layer_flight_schedule_filtered_df.count()

# COMMAND ----------

display(bronze_layer_flight_schedule_filtered_df)

# COMMAND ----------

#changing date columns
from pyspark.sql.functions import col,to_date

silver_layer_flight_schedule_df=bronze_layer_flight_schedule_filtered_df\
    .withColumn("validFrom",to_date(col("validFrom"),"dd-MM-yyyy"))\
    .withColumn("validTo",to_date(col("validTo"),"dd-MM-yyyy"))

# COMMAND ----------

from pyspark.sql.functions import split, explode, trim

# Split the string by comma and explode into rows
silver_layer_flight_schedule_df_days = silver_layer_flight_schedule_df.withColumn("day", explode(split("dayOfWeek", ","))) \
            .withColumn("day", trim(col("day"))) \
            .drop("dayOfWeek")

# COMMAND ----------

silver_layer_flight_schedule_df_days.count()

# COMMAND ----------

display(silver_layer_flight_schedule_df_days)

# COMMAND ----------

silver_layer_flight_schedule_df_days.write.saveAsTable("silver_lyr.flight_schedule",mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_lyr.flight_schedule

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select origin,destination,count(flightNumber) as flight_schedules from silver_lyr.flight_schedule group by origin,destination;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select origin,destination,count(day) as flight_schedules from silver_lyr.flight_schedule group by origin,destination ;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe silver_lyr.flight_schedule;