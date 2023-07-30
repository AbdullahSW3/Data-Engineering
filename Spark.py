# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

spark

# COMMAND ----------

# The applied options are for CSV files. For other file types, these will be ignored.
bank_data= spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/bank.csv")

display(bank_data)

# COMMAND ----------

# Create a view or table

temp_table_name = "bank_csv"

bank_data.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import avg
import pandas as pd
import numpy as np
from pandas import isna, isnull
import pyspark.sql.types as T

# COMMAND ----------

#gaining a better understanding of data
bank_data.describe().display()

# COMMAND ----------

#seeing the number of rows and columns in the dataset
print((bank_data.count(), len(bank_data.columns)))

# COMMAND ----------

#used printschema to see the schema of the data 
bank_data.printSchema()

# COMMAND ----------

#counting the number of data types
from collections import Counter
print(Counter((x[1]for x in bank_data.dtypes)))

# COMMAND ----------

#just to make sure the count was correct no need for this line other than just cheacking and making sure
bank_data.dtypes

# COMMAND ----------

#counting the number of nulls in each attribute 
null_count = bank_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in bank_data.columns])
null_count.show(vertical = True)

# COMMAND ----------

#counting the duplicated values if there is any 
uni_num= bank_data.select(bank_data.columns).distinct().count()
all_num = bank_data.select(bank_data.columns).count()

print((uni_num - all_num))

#displaying the duplicated values 
bank_data_duplicated = bank_data.exceptAll(bank_data.dropDuplicates())
bank_data_duplicated.display()

# COMMAND ----------

# getting the distinct values for the "job" column
bank_data.select("job").distinct().display()

# COMMAND ----------

# Filter rows where the "marital" column is "married" and the "job" column is "unemployed"
# or the "job" column is "unknown" and order it by "age"
bank_data.filter((F.col("marital") == "married") & \
((F.col("job") == "unemployed") | (F.col("job") == "unknown"))).orderBy("age").display()

# COMMAND ----------

# group the dataframe on the column "job" then count of each job category then rename the count column "count"
# and order the result in an ascending order
bank_data.groupBy("job").agg(F.count("job").alias("count")).orderBy("count").display()

# COMMAND ----------

# Filter rows where the "marital" column is "married" and the "job" column is NOT "unemployed"
# or the "job" column is NOT "unknown" and order it by "age"
bank_data_filter = bank_data.filter((F.col("marital") == "married") & \
((F.col("job") != "unemployed") | (F.col("job") != "unknown"))).orderBy("age")
bank_data_filter.display()

# COMMAND ----------

# group by "education" and count each category of "education" rename it to "count" and order by "count"
# to see the most likely eduation qualification that is needed to get a job and to get married 
bank_data_filter.groupBy("education").agg(F.count("education").alias("count")).orderBy("count").display()

# COMMAND ----------


