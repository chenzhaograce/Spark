from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder \
   .appName("example") \
   .getOrCreate()


data = [("Alice", 34), ("Bob", 45)]
rdd = spark.sparkContext.parallelize(data)


df = spark.createDataFrame(rdd, ["name", 'age'])


df.show()
