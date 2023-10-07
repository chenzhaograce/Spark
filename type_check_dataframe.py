from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()


# Example DataFrame
data = [("Alice", 34), ("Bob", 45)]
columns = ["Name", "Age"]


df = spark.createDataFrame(data, columns)


# Print the schema
df.printSchema()
