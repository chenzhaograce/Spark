'''
User Defined Functions (UDFs) are typically applied to columns within a single DataFrame.
If you want to use values from multiple DataFrames in a UDF, you would generally perform a join operation first to create
a single DataFrame that contains all of the necessary fields. After the join, you can apply the UDF to the resulting DataFrame.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


# Create a Spark session
spark = SparkSession.builder \
   .appName("Multi-DataFrame UDF Example") \
   .getOrCreate()


# Sample data
data1 = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
data2 = [(1, 30), (2, 25), (3, 35)]
columns1 = ["name", "id"]
columns2 = ["id", "age"]


# Create DataFrames
df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)


# Define custom function
def greet(name, age):
   return f"Hello, my name is {name} and I am {age} years old."


# Register UDF
greet_udf = udf(greet, StringType())


# Perform join operation
joined_df = df1.join(df2, df1.id == df2.id)


# Use UDF in DataFrame operation
result_df = joined_df.withColumn("greeting", greet_udf(joined_df["name"], joined_df["age"]))


# Show the result
result_df.select("name", "age", "greeting").show(truncate=False)


# Stop the Spark session
spark.stop()
