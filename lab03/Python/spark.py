# ## Copying Files to HDFS

# This project includes a dataset describing on-time
# performance for flights departing New York City airports
# (EWR, JFK, and LGA) in the year 2013. This data was
# collected by the U.S. Department of Transportation. It
# is stored here in a comma-separated values (CSV) file
# named `flights.csv`.

# Copy this file to HDFS by running `hdfs dfs` commands in
# CDSW using shell magic:

# Delete the `flights` subdirectory and its contents in
# your home directory, in case it already exists:

!hdfs dfs -rm -r flights

# Create the `flights` subdirectory:

!hdfs dfs -mkdir flights

# Copy the file into it:

!hdfs dfs -put flights.csv flights/

# The file `flights.csv` is now stored in the subdirectory
# `flights` in your home directory in HDFS.


# ## Using Apache Spark 2 with PySpark

# CDSW provides a virtual gateway to the cluster, which
# you can use to run Apache Spark jobs using PySpark,
# Spark's Python API.

# Before you connect to Spark: If you are using a secure
# cluster with Kerberos authentication, you must first go
# to the Hadoop Authentication section of your CDSW user
# settings and enter your Kerberos principal and password.


# ### Connecting to Spark

# Spark SQL is Spark's module for working with structured
# data. PySpark is Spark's Python API. The `pyspark.sql`
# module exposes Spark SQL functionality to Python.

# Begin by importing `SparkSession`, PySpark's main entry
# point:

from pyspark.sql import SparkSession

# Then call the `getOrCreate()` method of
# `SparkSession.builder` to connect to Spark. This
# example connects to Spark on YARN and gives a name to
# the Spark application:

spark = SparkSession.builder \
  .master("yarn") \
  .appName("cdsw-training") \
  .getOrCreate()

# Now you can use the `SparkSession` named `spark` to read
# data into Spark.


# ### Reading Data

# Read the flights dataset from HDFS. This data is in CSV
# format and includes a header row. Spark can infer the
# schema automatically from the data:

flights = spark.read.csv("flights/", header=True, inferSchema=True)

# The result is a Spark DataFrame named `flights`.


# ### Inspecting Data

# Inspect the DataFrame to gain a basic understanding
# of its structure and contents.

# Print the number of rows:

flights.count()

# Print the schema:

flights.printSchema()

# Inspect one or more variables (columns):

flights.describe("arr_delay").show()
flights.describe("arr_delay", "dep_delay").show()

# Print the first five rows:

flights.limit(5).show()

# Or more concisely:

flights.show(5)

# Print the first 20 rows (the default number is 20):

flights.show()

# `show()` can cause rows to wrap onto multiple lines,
# making the output hard to read. To make the output
# more readable, use `toPandas()` to return a pandas
# DataFrame. For example, return the first five rows
# as a pandas DataFrame and display it:

flights_pd = flights.limit(5).toPandas()
flights_pd

# To display the pandas DataFrame in a scrollable
# grid, import pandas and set the pandas option
# `display.html.table_schema` to `True`:

import pandas as pd
pd.set_option("display.html.table_schema", True)

flights_pd

# Caution: When working with a large Spark DataFrame,
# limit the number of rows before returning a pandas
# DataFrame.


# ### Transforming Data

# Spark SQL provides a set of functions for manipulating
# Spark DataFrames. Each of these methods returns a
# new DataFrame.

# `select()` returns the specified columns:

flights.select("carrier").show()

# `distinct()` returns distinct rows:

flights.select("carrier").distinct().show()

# `filter()` (or its alias `where()`) returns rows that
# satisfy a Boolean expression.

# To disambiguate column names and literal strings,
# import and use the functions `col()` and `lit()`:

from pyspark.sql.functions import col, lit

flights.filter(col("dest") == lit("SFO")).show()

# `orderBy()` (or its alias `sort()`) returns rows
# arranged by the specified columns:

flights.orderBy("month", "day").show()

flights.orderBy("month", "day", ascending=False).show()

# `withColumn()` adds a new column or replaces an existing
# column using the specified expression:

flights \
  .withColumn("on_time", col("arr_delay") <= 0) \
  .show()

# To concatenate strings, import and use the function
# `concat()`:

from pyspark.sql.functions import concat

flights \
  .withColumn("flight_code", concat("carrier", "flight")) \
  .show()

# `agg()` performs aggregations using the specified
# expressions.

# Import and use aggregation functions such as `count()`,
# `countDistinct()`, `sum()`, and `mean()`:

from pyspark.sql.functions import count, countDistinct

flights.agg(count("*")).show()

flights.agg(countDistinct("carrier")).show()

# Use the `alias()` method to assign a name to name the
# resulting column:

flights \
  .agg(countDistinct("carrier").alias("num_carriers")) \
  .show()

# `groupBy()` groups data by the specified columns, so
# aggregations can be computed by group:

from pyspark.sql.functions import mean

flights \
  .groupBy("origin") \
  .agg( \
       count("*").alias("num_departures"), \
       mean("dep_delay").alias("avg_dep_delay") \
  ) \
  .show()

# You can chain together multiple DataFrame methods:

flights \
  .filter(col("dest") == lit("BOS")) \
  .groupBy("origin") \
  .agg( \
       count("*").alias("num_departures"), \
       mean("dep_delay").alias("avg_dep_delay") \
  ) \
  .orderBy("avg_dep_delay") \
  .show()


# ### Using SQL Queries

# Instead of using Spark DataFrame methods, you can
# use a SQL query to achieve the same result.

# First you must create a temporary view with the
# DataFrame you want to query:

flights.createOrReplaceTempView("flights")

# Then you can use SQL to query the DataFrame:

spark.sql("""
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'BOS'
  GROUP BY origin
  ORDER BY avg_dep_delay""").show()


# ### Visualizing Data from Spark

# You can create data visualizations in CDSW using Python
# plotting libraries such as Matplotlib.

# When using Matplotlib, you might need to first use this
# Jupyter magic command to ensure that the plots display
# properly in CDSW:

%matplotlib inline

# To visualize data from a Spark DataFrame with
# Matplotlib, you must first return the data as a pandas
# DataFrame.

# Caution: When working with a large Spark DataFrame,
# you might need to sample, filter, or aggregate before
# returning a pandas DataFrame.

# For example, you can select the departure delay and
# arrival delay columns from the `flights` dataset,
# randomly sample 5% of non-missing records, and return
# the result as a pandas DataFrame:

delays_sample_pd = flights \
  .select("dep_delay", "arr_delay") \
  .dropna() \
  .sample(withReplacement=False, fraction=0.05) \
  .toPandas()

# Then you can create a scatterplot showing the
# relationship between departure delay and arrival delay:

delays_sample_pd.plot.scatter(x="dep_delay", y="arr_delay")

# The scatterplot seems to show a positive linear
# association between departure delay and arrival delay.

# ### Cleanup

# Disconnect from Spark:

spark.stop()
