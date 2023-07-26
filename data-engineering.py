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
df = spark.read.format("csv") \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("sep", ",") \
  .load("/FileStore/tables/bank.csv")

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "bank_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import avg
import pandas as pd
from pandas import isna, isnull
import pyspark.sql.types as T

# COMMAND ----------

#no missing values are present
dfwindow= Window.partitionBy("job").orderBy("balance")
df.groupBy(F.col("job")).agg(F.avg("balance"), F.avg("age")).show()

# COMMAND ----------

#using the agg function to calculate more than one agg function
df.agg(F.max("balance"), F.avg("balance"),F.max("age"),F.avg("age")).display()

# COMMAND ----------

#pivoting the education column to further understand the data
df.groupBy("age").pivot("education").count().orderBy("age").display()

# COMMAND ----------

#pivot the marital status and counting each one of the the three values single married divorced and order it by the age then dispaly
df.groupBy("age").pivot("marital").count().orderBy("age").display()


# COMMAND ----------

#droping some columns that i dont find usfull

df.drop("defult", "day","duration","campagin")

# COMMAND ----------

#window function 
window = Window.partitionBy("job").orderBy("age")
df.withColumn("avg", F.avg("balance").over(window))\
.withColumn("sum", F.sum("balance").over(window)).display()

# COMMAND ----------

#rollup with agg
df.rollup("age","job","marital","balance").agg(F.avg("balance"), F.sum("balance")).orderBy("age").display()

# COMMAND ----------

#cube with agg
df.cube("age","job","marital","balance").agg(F.sum("balance"),F.avg("balance")).orderBy("age").display()#CHECK LATER!

# COMMAND ----------

#df.isna().sum()

# COMMAND ----------

#counting each null occurance of every attribute and there are no nulls in the dataframe 
#from pyspark.sql.functions import col, count, when <---- this can be left as comment since i added the F. to them seen in the above code
nCount = df.select([F.count(F.when(F.col(c).isNull(),c)).alias(c) for c in df.columns])
nCount.show(vertical = True)

# COMMAND ----------

#count the number of data types there are in my dataframe
from collections import Counter
print(Counter((x[1] for x in df.dtypes)))

# COMMAND ----------

df.dtypes

# COMMAND ----------

import numpy as np

# Get the columns and their data types as a list of tuples
catColumns = df.dtypes
print(catColumns)
# Use list comprehension to filter the columns with a string data type
catColumns = [col_name for col_name, data_type in catColumns if data_type == "string"]
print(catColumns)


for col in catColumns:
  if df.select(col).distinct().count() == 2:
    df= df.withColumn(col, (df[col]== "yes").cast("int"))

display(df)





# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# Create StringIndexer for each categorical column
indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="skip") for col in catColumns]

# Create a list of columns to one-hot encode (columns that have been indexed)
encodeCols = [indexer.getOutputCol() for indexer in indexers]

# Create OneHotEncoder for the indexed columns
encoder = OneHotEncoder(inputCols=encodeCols, outputCols=[col + "_encoded" for col in encodeCols])

# Define a pipeline to chain the transformations
pipeline = Pipeline(stages=indexers + [encoder])

# Fit the pipeline to the DataFrame and transform the data
encoded_df = pipeline.fit(df).transform(df)

# Show the resulting DataFrame with one-hot encoded columns
encoded_df.display()

# COMMAND ----------



# COMMAND ----------

#new dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `bank_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "bank_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------


