# Databricks notebook source
df=spark.table("bronze_lyr.airport_services")

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

new_df = spark.read.option("header", True) \
               .option("multiline", True) \
               .option("quote", '"') \
               .option("escape", '"') \
               .option("delimiter", ",") \
               .csv("/Volumes/workspace/default/dd_volume/airport_services.csv")

# COMMAND ----------

display(new_df)

# COMMAND ----------

new_df.count()

# COMMAND ----------

new_df.write.mode("overwrite").saveAsTable("bronze_lyr.airport_services")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze_lyr.airport_services;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_lyr.airport_services;