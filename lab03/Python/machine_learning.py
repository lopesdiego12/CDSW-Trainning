# ### Machine Learning with MLlib

!hdfs dfs -put flights.csv flights/

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

# ### Reading Data

# Read the flights dataset from HDFS. This data is in CSV
# format and includes a header row. Spark can infer the
# schema automatically from the data:

flights = spark.read.csv("flights/", header=True, inferSchema=True)

# MLlib is Spark's machine learning library.

# As an example, let's examine the relationship between
# departure delay and arrival delay using a linear
# regression model.

# First, create a Spark DataFrame with only the relevant
# columns and with missing values removed:

flights_to_model = flights \
  .select("dep_delay", "arr_delay") \
  .dropna()

# MLlib requires all predictor columns be combined into
# a single column of vectors. To do this, import and use
# the `VectorAssembler` feature transformer:

from pyspark.ml.feature import VectorAssembler

# In this example, there is only one predictor (input)
# variable: `dep_delay`.

assembler = VectorAssembler(inputCols=["dep_delay"], outputCol="features")

# Use the `VectorAssembler` to assemble the data:

flights_assembled = assembler.transform(flights_to_model)
flights_assembled.show(5)

# Randomly split the assembled data into a training
# sample (70% of records) and a test sample (30% of
# records):

(train, test) = flights_assembled.randomSplit([0.7, 0.3])

# Import and use `LinearRegression` to specify the linear
# regression model and fit it to the training sample:

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="arr_delay")

lr_model = lr.fit(train)

# Examine the model intercept and slope:

lr_model.intercept

lr_model.coefficients

# Evaluate the linear model on the test sample:

lr_summary = lr_model.evaluate(test)

# R-squared is the fraction of the variance in the test
# sample that is explained by the model:

lr_summary.r2


# ### Cleanup

# Disconnect from Spark:

spark.stop()
