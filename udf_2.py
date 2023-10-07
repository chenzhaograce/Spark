from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# Create a Spark session
spark = SparkSession.builder \
   .appName("Multi-Column UDF Example") \
   .getOrCreate()


# Sample data
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
columns = ["name", "age"]


# Create DataFrame
df = spark.createDataFrame(data, columns)


# Define custom function
def greet(name, age):
   return f"Hello, my name is {name} and I am {age} years old."


# Register UDF
greet_udf = udf(greet, StringType())


# Use UDF in DataFrame operation
result_df = df.withColumn("greeting", greet_udf(df["name"], df["age"]))


# Show the result
result_df.show()

# Show the full data of the DataFrame
result_df.show(truncate=False)
