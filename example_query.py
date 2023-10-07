from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import year, count, sum


# Create a Spark session
spark = SparkSession.builder \
   .appName("Sales Analysis") \
   .getOrCreate()


# Sample data
user_data = [(1, "Alice", "New York", "NY", "USA"),
            (2, "Bob", "Los Angeles", "CA", "USA")]


product_data = [(1, "Apple", "Electronics"),
               (2, "Zara", "Clothing")]


store_data = [(1, "New York", "USA"),
             (2, "Los Angeles", "USA")]


fact_data = [(1, 1, 1, "2023-01-10", 1, 3),
            (2, 2, 2, "2023-02-15", 2, 1),
            (3, 1, 2, "2022-03-10", 1, 2)]


# Define schemas with data types
user_schema = StructType([
   StructField("UserID", IntegerType()),
   StructField("Name", StringType()),
   StructField("City", StringType()),
   StructField("State", StringType()),
   StructField("Country", StringType())
])


product_schema = StructType([
   StructField("ProductID", IntegerType()),
   StructField("BranchName", StringType()),
   StructField("Category", StringType())
])


store_schema = StructType([
   StructField("StoreID", IntegerType()),
   StructField("City", StringType()),
   StructField("Country", StringType())
])


fact_schema = StructType([
   StructField("OrderID", IntegerType()),
   StructField("ProductID", IntegerType()),
   StructField("UserID", IntegerType()),
   StructField("DateTime", StringType()),  # You might want to parse this as DateType() in real use cases
   StructField("StoreID", IntegerType()),
   StructField("ItemCount", IntegerType())
])


# Create DataFrames
user_df = spark.createDataFrame(user_data, schema=user_schema)
product_df = spark.createDataFrame(product_data, schema=product_schema)
store_df = spark.createDataFrame(store_data, schema=store_schema)
fact_df = spark.createDataFrame(fact_data, schema=fact_schema)


# Perform join operations
joined_df = fact_df.join(product_df, fact_df.ProductID == product_df.ProductID, "inner") \
   .join(user_df, fact_df.UserID == user_df.UserID, "inner")


joined_df.show()
# Filter data for the year 2023 and calculate the sales count per branch
result_df = joined_df.filter((year("DateTime") == 2023) | (year("DateTime") == 2022)) \
   .groupBy("BranchName") \
   .agg(sum("ItemCount").alias("SalesCount"))


# Show the result
result_df.show()


# Stop the Spark session
spark.stop()

