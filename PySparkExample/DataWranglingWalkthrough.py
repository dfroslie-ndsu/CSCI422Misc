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

# Set Spark context for the storage account and the base URI.
spark.conf.set(
    "fs.azure.account.key.marchmadstore.dfs.core.windows.net",
    dbutils.secrets.get(scope="MarchMadnessScope", key="marchmadstore-key"))

uri = "abfss://datawranglingsample@marchmadstore.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading data
# MAGIC
# MAGIC This section will provide a couple of examples of reading data into a notebook.
# MAGIC

# COMMAND ----------

# Read the Grades file using defaults and use the top row as header (not the default behavior)
grades_df = spark.read.csv(uri+"data/Grades.csv", header=True)
 
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
students_df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"data/StudentInfo.csv")

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

tests_df = tests_df.withColumn('TotalTestScore', tests_df.Test1+tests_df.Test2+tests_df.Test3+tests_df.Final)

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
items_df = spark.read.options(delimiter=',', header=True).schema(schema).csv(uri+"data/Items.csv")

display(items_df)

print(items_df.dtypes)


# COMMAND ----------

# R and Pandas have built in "melt" (wide to long) capabilities, but PySpark doesn't seem to have this.  
# Fortunately, the internet helps.  Here's a method along with the original source to perform the melt process.

# Function from https://www.taintedbits.com/2018/03/25/reshaping-dataframe-using-pivot-and-melt-in-apache-spark-and-pandas/
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
from typing import Iterable

def melt_df(
        df: DataFrame,
        id_vars: Iterable[str], value_vars: Iterable[str],
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

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

# The target format is StudentID, Item, Score
grades_scores_long_df = melt_df(grades_score_df,   # Data frame
                                ['StudentID'],     # Columns to keep
                                grades_score_df.columns[1:len(grades_score_df.columns)], # Columns to convert to long
                                'Item',            # Name of new column that used to be the column header.
                                'Score')             # Name of new column containing the value.

display(grades_scores_long_df)

# COMMAND ----------

# Next, we'll deal with the P/F columns separately.
grades_special_df = grades_df.select('StudentID', 'SpecialEvent1', 'SpecialEvent2')

display(grades_special_df)


# COMMAND ----------

# The target format is StudentID, Item, Grade
grades_special_long_df = melt_df(grades_special_df,   # Data frame
                                ['StudentID'],     # Columns to keep
                                grades_special_df.columns[1:len(grades_special_df.columns)], # Columns to convert to long
                                'Item',            # Name of new column that used to be the column header.
                                'Grade')             # Name of new column containing the value.

display(grades_special_long_df)

# COMMAND ----------

# Now that we're in long format, we can join with the items detail.
# We'll use a left outer join to ensure we don't miss any Item.

grades_special_scores_df = grades_special_long_df.join(items_df, on='Item', how='leftouter')

display(grades_special_scores_df)

# COMMAND ----------

# Now let's create the Score column.  We need a function to do that in PySpark.  Fortunately, it is a very simple function.
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
# MAGIC We'll save in both CSV and Parquet format.
# MAGIC

# COMMAND ----------

# coalesce(1) forces a single file (and a single thread)
all_long_df.coalesce(1).write.option('header',True).mode('overwrite').csv(uri+"output/CSVLong")
all_long_df.coalesce(1).write.option('header',True).mode('overwrite').parquet(uri+"output/ParquetLong")


# COMMAND ----------

all_long_df.write.format("delta").mode('overwrite').save(uri+"output/DeltaLong")

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
