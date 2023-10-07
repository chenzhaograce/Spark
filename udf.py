from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
   .appName("UDF Example") \
   .getOrCreate()


# Sample data
data = [(1,), (2,), (3,), (4,)]
columns = ["number"]


# Create DataFrame
df = spark.createDataFrame(data, columns)


# Define custom function
def square(x):
   return x ** 2

# Register UDF
square_udf = udf(square, IntegerType())


# Use UDF in DataFrame operation
result_df = df.withColumn("number_squared", square_udf(df["number"]))


# Show the result
result_df.show()


# Stop the Spark session
spark.stop()

