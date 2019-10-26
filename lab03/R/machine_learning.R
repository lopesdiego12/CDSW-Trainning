# Copy the file into it:

system("hdfs dfs -put flights.csv flights/")

# ## Using Apache Spark 2 with sparklyr

# CDSW provides a virtual gateway to the cluster, which
# you can use to run Apache Spark jobs. Cloudera recommends
# using [sparklyr](https://spark.rstudio.com) as the R
# interface to Spark.

# Install the sparklyr package from CRAN (if it is not
# already installed). This might take several minutes:

if(!"sparklyr" %in% rownames(installed.packages())) {
  install.packages("sparklyr")
}

# Before you connect to Spark: If you are using a secure
# cluster with Kerberos authentication, you must first go
# to the Hadoop Authentication section of your CDSW user
# settings and enter your Kerberos principal and password.


# ### Connecting to Spark

# Begin by loading the sparklyr package:

library(sparklyr)

# Then call the `spark_connect()` function to connect to
# Spark. This example connects to Spark on YARN and gives
# a name to the Spark application:

spark <- spark_connect(
  master = "yarn",
  app_name = "cdsw-training"
)

# Now you can use the connection object named `spark` to
# read data into Spark.


# ### Reading Data

# Read the flights dataset from HDFS. This data is in CSV
# format and includes a header row. Spark can infer the
# schema automatically from the data:

flights <- spark_read_csv(
  sc = spark,
  name = "flights",
  path = "flights/",
  header = TRUE,
  infer_schema = TRUE
)

# ### Machine Learning with MLlib

# MLlib is Spark's machine learning library.

# As an example, let's examine the relationship between
# departure delay and arrival delay using a linear
# regression model.

# First, create a Spark DataFrame with only the relevant
# columns and with missing values removed:

flights_to_model <- flights %>%
  select(dep_delay, arr_delay) %>%
  na.omit()

# Randomly split the data into a training sample (70% of
# records) and a test sample (30% of records):

samples <- flights_to_model %>%
  sdf_partition(train = 0.7, test = 0.3)

# Specify the linear regression model and fit it to the
# training sample:

model <- samples$train %>%
  ml_linear_regression(arr_delay ~ dep_delay)

# Examine the model coefficients and other model summary
# information:

summary(model)

# Use the model to generate predictions for the test
# sample:

pred <- model %>%
  ml_predict(samples$test)

# Evaluate the model on the test sample by computing
# R-squared, which gives the fraction of the variance
# in the test sample that is explained by the model:

pred %>%
  summarise(r_squared = cor(arr_delay, prediction)^2)


# ### Cleanup

# Disconnect from Spark:

spark_disconnect(spark)
