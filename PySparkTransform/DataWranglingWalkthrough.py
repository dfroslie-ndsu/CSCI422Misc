# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Data Wrangling Walkthrough
# MAGIC
# MAGIC This notebook provides examples of several core PySpark operations using the Spark dataframe.  This will include:
# MAGIC - Reading data from CSV
# MAGIC - Basic dataframe metrics
# MAGIC - Joining data 
# MAGIC - Melting dataframes
# MAGIC - Filtering data
# MAGIC - Aggregation operations
# MAGIC - Saving data
# MAGIC
# MAGIC

# COMMAND ----------

# If using Azure Databricks, set Spark context for the storage account and the base URI.
# spark.conf.set(
#     "fs.azure.account.key.marchmadstore.dfs.core.windows.net",
#     dbutils.secrets.get(scope="MarchMadnessScope", key="marchmadstore-key"))

# uri = "abfss://datawranglingsample@marchmadstore.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading data
# MAGIC
# MAGIC This section will provide a couple of examples of reading data into a notebook.
# MAGIC
# MAGIC You'll need to upload the CSV data from the Data folder in your repo to your storage account or a Volumes section of the Databricks catalog.  The latter option is the only one for Databricks Free Edition.

# COMMAND ----------

# Read the Grades file using defaults and use the top row as header (not the default behavior)

# Azure Databricks - upload to storage account.
#grades_df = spark.read.csv(uri+"data/Grades.csv", header=True)  

# Databricks Free Edition - upload to a volume in the catalog.  
# df = (
#     spark.read.format("csv")
#     .option("header", True)
#     .load("/Volumes/<catalog_name>/<schema_name>/<volume_name>/<file_name>.csv")
# )

grades_df = (
    spark.read.format("csv")
    .option("header", True)
    .load("/Volumes/workspace/bronze_examples/landingzone/Grades.csv")
)

display(grades_df)



# COMMAND ----------

# Columns, number of rows and columns, data types.
print(grades_df.columns)  

print("Total columns:  ", len(grades_df.columns))
print("Total rows:  ", grades_df.count())   

print(grades_df.dtypes)



# COMMAND ----------

# Read the Students file utilizing schema on read and specifying more parameters.
# For more background on this, see https://sparkbyexamples.com/pyspark/pyspark-structtype-and-structfield/.

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([StructField("StudentID", IntegerType(), True), \
                    StructField("Major", StringType(), True), \
                    StructField("HomeState", StringType(), True) ])

# Azure Databricks
#students_df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"data/StudentInfo.csv")

students_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("schema", schema)
    .load("/Volumes/workspace/bronze_examples/landingzone/StudentInfo.csv")
)


display(students_df)


# COMMAND ----------

# Columns, number of rows and columns, data types.
print(students_df.columns)  

print("Total columns:  ", len(students_df.columns))
print("Total rows:  ", students_df.count())   

print(students_df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join the two data sets
# MAGIC
# MAGIC You should be familiar with the various types of joins.  We will use an inner join to get the student information together with the grades.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import IntegerType

# Convert the StudentID to an integer in grades to match students.
grades_df = grades_df.withColumn('StudentID', grades_df.StudentID.cast(IntegerType()))


# COMMAND ----------

grades_student_df = grades_df.join(students_df, on='StudentID', how='inner')

display(grades_student_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select a subset of the columns - only student info, tests, and final.

# COMMAND ----------

tests_df = grades_student_df.select('StudentID', 'Major', 'HomeState', 'Test1', 'Test2', 'Test3', 'Final')

display(tests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a new column based on the subset columns

# COMMAND ----------

from pyspark.sql.functions import col

tests_df = tests_df.withColumn(
    "TotalTestScore",
    col("Test1").cast("double") +
    col("Test2").cast("double") +
    col("Test3").cast("double") +
    col("Final").cast("double")
)

display(tests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Find the average grade based on HomeState using the new column.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import count,avg

homestate_avg_df = tests_df.groupby('HomeState').agg(count('StudentID').alias('TotalStudents'), avg('TotalTestScore').alias('AverageTotalTestScore'))

display(homestate_avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter the subset based on major and aggregate.

# COMMAND ----------

cs_tests_df = tests_df.filter(tests_df.Major == "Computer Science")
print(cs_tests_df.count())

homestate_cs_avg_df = cs_tests_df.groupby('HomeState').agg(count('StudentID').alias('TotalStudents'), avg('TotalTestScore').alias('AverageTotalTestScore'))

display(homestate_cs_avg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read the file with detail on the grade items.  "Melt" the grades dataframe to enable a join with the grade items.
# MAGIC

# COMMAND ----------

# Specify schema on read.
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, FloatType

schema = StructType([StructField("Item", StringType(), True), \
                    StructField("Type", StringType(), True), \
                    StructField("TotalPoints", IntegerType(), True), \
                    StructField("Topic", StringType(), True)])

# Azure Databricks
#items_df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"data/Items.csv")

# Databricks Free
items_df = (
    spark.read.format("csv")
    .option("header", True)
    .option("schema", schema)
    .load("/Volumes/workspace/bronze_examples/landingzone/Items.csv")
)

display(items_df)

print(items_df.dtypes)


# COMMAND ----------

display(grades_df)

# COMMAND ----------

# First, we'll drop the P/F assignments and melt the remaining columns.  

grades_score_df = grades_df.drop('SpecialEvent1', 'SpecialEvent2')

display(grades_score_df)

# COMMAND ----------

# Display the column list that I want to melt, starting with Quiz1 and ending with Final.
print(grades_score_df.columns[1:len(grades_score_df.columns)])

# COMMAND ----------

grades_scores_long_df = grades_score_df.melt(ids=['StudentID'],
                                             values=grades_score_df.columns[1:len(grades_score_df.columns)],
                                             variableColumnName = 'Item',
                                             valueColumnName = 'Score')
display(grades_scores_long_df)

# COMMAND ----------

# Next, we'll deal with the P/F columns separately.
grades_special_df = grades_df.select('StudentID', 'SpecialEvent1', 'SpecialEvent2')

display(grades_special_df)


# COMMAND ----------

# The target format is StudentID, Item, Grade

grades_special_long_df = grades_special_df.melt(ids=['StudentID'],
                                             values=grades_special_df.columns[1:len(grades_special_df.columns)],
                                             variableColumnName = 'Item',
                                             valueColumnName = 'Grade')

display(grades_special_long_df)

# COMMAND ----------

# Now that we're in long format, we can join with the items detail.
# We'll use a left outer join to ensure we don't miss any Item.

grades_special_scores_df = grades_special_long_df.join(items_df, on='Item', how='leftouter')

display(grades_special_scores_df)

# COMMAND ----------

# Now let's create the Score column.  We need a function to do that in PySpark.  Fortunately, it is a very simple function.
# Note - you could also use when/other statements.
def get_specialevent_score(grade, total_points) -> int:
    if grade=="P":
        return(total_points)
    else:
        return(0)

# COMMAND ----------

# Test the function.
print("Should be 10:  ", get_specialevent_score("P", 10))
print("Should be 0:  ", get_specialevent_score("F", 10))
print("Should be 0:  ", get_specialevent_score("Q", 10))

# COMMAND ----------

# The function needs to be a Spark UDF (User Defined Function) for it to be used across the cluster.
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

get_specialevent_score_udf = F.udf(get_specialevent_score, IntegerType())

grades_special_scores_map_df = grades_special_scores_df.withColumn('Score', 
                                               get_specialevent_score_udf(grades_special_scores_df.Grade, grades_special_scores_df.TotalPoints))

display(grades_special_scores_map_df)


# COMMAND ----------

# Reminder of the format of the dataframe to merge with.
display(grades_scores_long_df)
print(grades_scores_long_df)

# COMMAND ----------

# Finally, we can select the right columns to align the P/F dataframe with the other dataframe.  
special_scores_df = grades_special_scores_map_df.select('StudentID', 'Item', 'Score')

display(special_scores_df)
print(special_scores_df.dtypes)

# COMMAND ----------


# Need to align the Score data types and then merge.
grades_scores_long_df = grades_scores_long_df.withColumn('Score', grades_scores_long_df.Score.cast(IntegerType()))

all_long_df = grades_scores_long_df.union(special_scores_df)

print(all_long_df.count())   # Should be grades_df row count * (columns-1)
print(grades_df.count() * (len(grades_df.columns)-1))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Save the data in long format.
# MAGIC
# MAGIC We'll save in CSV, Parquet, and Delta formats.
# MAGIC

# COMMAND ----------

# Azure Databrics
# all_long_df.write.option('header',True).mode('overwrite').csv(uri+"output/CSVLong")
# all_long_df.write.option('header',True).mode('overwrite').parquet(uri+"output/ParquetLong")
# all_long_df.write.format("delta").mode('overwrite').save(uri+"output/DeltaLong")

# Databricks Free - just save to a delta lake table.
all_long_df.write.mode("overwrite").saveAsTable("bronze_examples.grades_long")


# COMMAND ----------

# MAGIC %md
# MAGIC # Analyze long format.
# MAGIC
# MAGIC Now that we're in long format, we can do a variety of analysis easily.

# COMMAND ----------

# Join the student and item data with the long format.
all_join_df = all_long_df.join(students_df, on='StudentID', how='inner')

all_join_df = all_join_df.join(items_df, on='Item', how='inner')

display(all_join_df)
                           
                              


# COMMAND ----------

# Calculate total score by student.
from pyspark.sql.functions import sum, desc

student_agg_df = all_join_df.groupby('StudentID', 'Major', 'HomeState').agg(sum('Score').alias('StudentTotalPoints'),
                                                                            sum('TotalPoints').alias('TotalPointsAvailable')).orderBy(desc('StudentTotalPoints'))

display(student_agg_df)

# COMMAND ----------

# Calculate average score by major.
display(student_agg_df.groupby('Major').agg(count('StudentID').alias('StudentCount'), avg('StudentTotalPoints').alias('AverageTotal')))

# COMMAND ----------

# Calculate total score by assignment type.
display(all_join_df.groupby('Type').agg(sum('Score').alias('ItemTotalPoints'),
                                                                            sum('TotalPoints').alias('TotalPointsAvailable')).orderBy(desc('ItemTotalPoints')))


# COMMAND ----------

# Find the students that are in the student list but don't have any grades.
# This is a good job for an anti-join.
students_notinclass_df = students_df.join(all_join_df, on='StudentID', how='anti')

display(students_notinclass_df)
