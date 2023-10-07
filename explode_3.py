from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType


# Create a Spark session
spark = SparkSession.builder \
   .appName("UDF and Explode Example") \
   .getOrCreate()


# Sample data
data = [("Fruit", 100, "Apple", 50), ("Vehicle", 200, "Car", 150)]
columns = ["category", "category_count", "sub_category", "sub_category_count"]
# Create DataFrame
df = spark.createDataFrame(data, columns)


# Define a schema for the struct
schema = ArrayType(StructType([
   StructField("type", StringType(), True),
   StructField("count", IntegerType(), True)
]))


# Define custom function to create an array of structs
def generate_structs(category, category_count, sub_category, sub_category_count):
   return [(category, category_count), (sub_category, sub_category_count)]


# Register UDF
structs_udf = udf(generate_structs, schema)


# Use UDF to generate array of structs and then explode it
exploded_df = df.select(explode(structs_udf(
   df["category"], df["category_count"], df["sub_category"], df["sub_category_count"]
)).alias("exploded_struct"))


# Select struct fields as columns
result_df = exploded_df.select(
   "exploded_struct.type",
   "exploded_struct.count"
)


# Show the result
result_df.show()


# Stop the Spark session
spark.stop()
