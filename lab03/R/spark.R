# ## Copying Files to HDFS

# This project includes a dataset describing on-time
# performance for flights departing New York City airports
# (EWR, JFK, and LGA) in the year 2013. This data was
# collected by the U.S. Department of Transportation. It
# is stored here in a comma-separated values (CSV) file
# named `flights.csv`.

# Copy this file to HDFS by running `hdfs dfs` commands in
# CDSW using the R function `system()`:

# Delete the `flights` subdirectory and its contents in
# your home directory, in case it already exists:

system("hdfs dfs -rm -r flights")

# Create the `flights` subdirectory:

system("hdfs dfs -mkdir flights")

# Copy the file into it:

system("hdfs dfs -put flights.csv flights/")

# The file `flights.csv` is now stored in the subdirectory
# `flights` in your home directory in HDFS.


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

# The result is a Spark DataFrame named `flights`. Note
# that this is not an R data frame—it is a pointer to a
# Spark DataFrame.


# ### Inspecting Data

# Inspect the Spark DataFrame to gain a basic
# understanding of its structure and contents.

# To make the code more readable, the examples below use
# the pipe operator `%>%`.

# Print the number of rows:

flights %>% sdf_nrow()

# Print the column names:

flights %>% colnames()

# Print the first 10 rows of data, for as many columns
# as fit on the screen (this is the default behavior):

flights


# ### Transforming Data Using dplyr Verbs

# sparklyr works together with the popular R package
# [dplyr](http://dplyr.tidyverse.org). sparklyr enables
# you to use dplyr *verbs* to manipulate data with Spark.

# The main dplyr verbs are:
# * `select()` to select columns
# * `filter()` to filter rows
# * `arrange()` to order rows
# * `mutate()` to create new columns
# * `summarise()` to aggregate

# There are also some other, less important verbs, like
# `rename()` and `transmute()`, that are variations on
# the main verbs.

# In addition to verbs, dplyr also has the function
# `group_by()`, which allows you to perform operations by
# group.

# Load the dplyr package:

library(dplyr)

# `select()` returns the specified columns:

flights %>% select(carrier)

# `distinct()` works like `select()` but returns only
# distinct values:

flights %>% distinct(carrier)

# `filter()` returns rows that satisfy a Boolean
# expression:

flights %>% filter(dest == "SFO")

# `arrange()` returns rows arranged by the specified
# columns:

flights %>% arrange(month, day)

# The default sort order is ascending. Use the helper
# function `desc()` to sort by a column in descending
# order:

flights %>% arrange(desc(month), desc(day))

# `mutate()` adds new columns or replaces existing
# columns using the specified expressions:

flights %>% mutate(on_time = arr_delay <= 0)

flights %>% mutate(flight_code = paste0(carrier, flight))

# `summarise()` performs aggregations using the specified
# expressions.

# Use aggregation functions such as `n()`, `n_distinct()`,
# `sum()`, and `mean()`. With some of these functions,
# you must specify `na.rm = TRUE` to silence warnings
# about missing values:

flights %>% summarise(n = n())

flights %>%
  summarise(num_carriers = n_distinct(carrier))

# `group_by()` groups data by the specified columns, so
# aggregations can be computed by group:

flights %>%
  group_by(origin) %>%
  summarise(
    num_departures = n(),
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  )

# You can chain together multiple dplyr verbs:

flights %>%
  filter(dest == "BOS") %>%
  group_by(origin) %>%
  summarise(
    num_departures = n(),
    avg_dep_delay = mean(dep_delay, na.rm = TRUE)
  ) %>%
  arrange(avg_dep_delay)


# ### Using SQL Queries

# Instead of using dplyr verbs, you can use a SQL query
# to achieve the same result:

tbl(spark, sql("
  SELECT origin,
    COUNT(*) AS num_departures,
    AVG(dep_delay) AS avg_dep_delay
  FROM flights
  WHERE dest = 'BOS'
  GROUP BY origin
  ORDER BY avg_dep_delay"))


# ### Spark DataFrames Functions

# In addition to the dplyr verbs, there are also some
# other data manipulation functions you can use with
# sparklyr. For example:

# `na.omit()` filters out rows with missing values:

flights %>% na.omit()

# `sdf_sample()` returns a random sample of rows:

flights %>%
  sdf_sample(fraction = 0.05, replacement = FALSE)


# ### Visualizing Data from Spark

# You can create data visualizations in CDSW using R
# graphics packages such as ggplot2.

# To visualize data from a Spark DataFrame with ggplot2,
# you must first return the data as an R data frame. To
# do this, use the `collect()` function.

# Caution: When working with a large Spark DataFrame,
# you might need to sample, filter, or aggregate before
# using `collect()` to return an R data frame.

# For example, you can select the departure delay and
# arrival delay columns from the `flights` dataset,
# randomly sample 5% of non-missing records, and return
# the result as an R data frame:

delays_sample_df <- flights %>%
  select(dep_delay, arr_delay) %>%
  na.omit() %>%
  sdf_sample(fraction = 0.05, replacement = FALSE) %>%
  collect()

# Then you can create a scatterplot showing the
# relationship between departure delay and arrival delay:

library(ggplot2)

ggplot(delays_sample_df, aes(x=dep_delay, y=arr_delay)) +
  geom_point()

# The scatterplot seems to show a positive linear
# association between departure delay and arrival delay.

# ### Cleanup

# Disconnect from Spark:

spark_disconnect(spark)
