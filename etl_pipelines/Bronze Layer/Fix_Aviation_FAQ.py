# Databricks notebook source
df=spark.table("bronze_lyr.aviation_faq")

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
               .csv("/Volumes/workspace/default/dd_volume/aviation_FAQ.csv")

# COMMAND ----------

display(new_df)

# COMMAND ----------

new_df.count()

# COMMAND ----------

new_df.write.mode("overwrite").saveAsTable("bronze_lyr.aviation_faq")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from bronze_lyr.aviation_faq;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_lyr.aviation_faq;