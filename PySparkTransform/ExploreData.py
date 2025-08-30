# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # ExploreData
# MAGIC 
# MAGIC ###Getting started with Databricks and March Madness.

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.marchmadstore.dfs.core.windows.net",
    dbutils.secrets.get(scope="MarchMadnessScope", key="marchmadstore-key"))

uri = "abfss://marchmadnessblob@marchmadstore.dfs.core.windows.net/"

# COMMAND ----------

dbutils.fs.ls(uri+"Kaggle")

# COMMAND ----------

results_df = spark.read.csv(uri+"Kaggle/MNCAATourneyCompactResults.csv", header=True)

print(type(results_df))


# COMMAND ----------

display(results_df)

print(results_df.dtypes)

# COMMAND ----------

import pandas as pd

results_pdf = results_df.toPandas()

print(type(results_pdf))


# COMMAND ----------

display(results_pdf)

print(results_pdf.dtypes)

# COMMAND ----------

results_df = results_df.withColumn("TotalPoints", results_df.WScore + results_df.LScore)

display(results_df)

# COMMAND ----------

results_pdf['TotalPoints']= results_pdf.WScore + results_pdf.LScore

display(results_pdf)

# COMMAND ----------

from pyspark.sql.types import IntegerType

results_df = results_df.withColumn("TotalPoints", results_df.WScore.cast(IntegerType()) + results_df.LScore.cast(IntegerType()))

display(results_df)

# COMMAND ----------

results_pdf['TotalPoints']= results_pdf.WScore.astype(int) + results_pdf.LScore.astype(int)

display(results_pdf)

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum

results_df = results_df.withColumn('WScoreInt', results_df.WScore.cast(IntegerType()))

wpoints_per_year_df = results_df.groupby('Season').agg(sum('WScoreInt'))

display(wpoints_per_year_df)

# COMMAND ----------

results_pdf['WScoreInt'] = results_pdf.WScore.astype(int)

wpoints_per_year = results_pdf.groupby('Season')['WScoreInt'].sum()

print(wpoints_per_year)


# COMMAND ----------

results_df.write.csv(uri+'KaggleOut2')
