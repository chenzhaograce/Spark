from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, IntegerType


# Create a Spark session
spark = SparkSession.builder \
   .appName("UDF and Explode Example") \
   .getOrCreate()


# Sample data
data = [("Alice", 3), ("Bob", 2), ("Charlie", 4)]
columns = ["name", "value"]


# Create DataFrame
df = spark.createDataFrame(data, columns)


# Define custom function to create an array [value, value*2, value*3]
def generate_array(value):
   return [value, value*2, value*3]


# Register UDF
array_udf = udf(generate_array, ArrayType(IntegerType()))


# Use UDF to generate array and then explode it
exploded_df = df.select("name", explode(array_udf(df["value"])).alias("exploded_value"))


# Show the result
exploded_df.show()


# Stop the Spark session
spark.stop()
