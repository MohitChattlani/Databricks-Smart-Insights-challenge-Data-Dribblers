# Databricks notebook source
filename = dbutils.widgets.get("filename")
tablename = dbutils.widgets.get("tablename")

print(filename)
print(tablename)

# COMMAND ----------

file_path="/Volumes/workspace/default/dd_volume/{0}.csv".format(filename)
final_table_name="bronze_lyr.{0}".format(tablename)
print(file_path)
print(final_table_name)

# COMMAND ----------

df=spark.read.csv(file_path,header=True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.write.saveAsTable(final_table_name,mode="overwrite")

# COMMAND ----------

validation_query="SELECT count(*) FROM {0}".format(final_table_name)
print(validation_query)

# COMMAND ----------

# Use in SQL
df = spark.sql(validation_query)

df.show()