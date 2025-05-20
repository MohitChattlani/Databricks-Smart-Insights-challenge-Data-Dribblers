# Databricks notebook source
silver_layer_airport_service_df=spark.table("bronze_lyr.airport_services")

# COMMAND ----------

display(silver_layer_airport_service_df)

# COMMAND ----------

# Dropping empty columns
#df = silver_layer_airport_service_df.drop(['titleHindi', 'descriptionHindi'])

drop_df = silver_layer_airport_service_df.drop("titleHindi", "descriptionHindi")

display(drop_df)

# COMMAND ----------

#replacing missing values with null
missing_col_df = drop_df.fillna({
    "email": "Not Available",
    "phone": "Not Available",
    "website": "Not Available"
}) 

# COMMAND ----------

from pyspark.sql.functions import trim, regexp_replace,col

df = missing_col_df.withColumn("titleEnglish", trim(col("titleEnglish")))
df_replace = df.withColumn("descriptionEnglish", regexp_replace(col("descriptionEnglish"), r"\s+", " "))


# COMMAND ----------

from pyspark.sql.functions import to_date

df = df_replace.withColumn("last_updated", to_date(col("last_updated"), "yyyy-MM-dd"))


# COMMAND ----------

df.count()

# COMMAND ----------

# Parquet
df.write.saveAsTable("silver_lyr.airport_services",mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_lyr.airport_services

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lyr.airport_services;