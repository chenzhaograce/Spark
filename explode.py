from pyspark.sql import SparkSession
from pyspark.sql.functions import explode


# Create a Spark session
spark = SparkSession.builder \
   .appName("Explode Example") \
   .getOrCreate()


# Sample data
data = [("Alice", [10, 20, 30]), ("Bob", [15, 25, 35]), ("Charlie", [20, 30, 40])]
columns = ["name", "values"]


# Create DataFrame
df = spark.createDataFrame(data, columns)


# Use explode function
exploded_df = df.select("name", explode(df["values"]).alias("value"))


# Show the result
df.show()
exploded_df.show()


# Stop the Spark session
spark.stop()
