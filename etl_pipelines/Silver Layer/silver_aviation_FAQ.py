# Databricks notebook source
bronze_layer_aviation_faq_df=spark.table("bronze_lyr.aviation_faq")

# COMMAND ----------

bronze_layer_aviation_faq_df.count()

# COMMAND ----------

display(bronze_layer_aviation_faq_df)

# COMMAND ----------

from pyspark.sql.functions import to_date

silver_layer_aviation_faq_df = bronze_layer_aviation_faq_df.withColumn("lastUpdated", to_date(bronze_layer_aviation_faq_df["lastUpdated"], "yyyy-MM-dd"))
display(silver_layer_aviation_faq_df)

# COMMAND ----------

silver_layer_aviation_faq_df.write.saveAsTable("silver_lyr.aviation_faq",mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_lyr.aviation_faq;