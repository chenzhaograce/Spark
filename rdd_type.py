from pyspark import SparkContext


# Create a Spark context
sc = SparkContext("local", "Example")


# Example RDD
data = [("Alice", 34), ("Bob", 45)]
rdd = sc.parallelize(data)


# Get the first record
first_record = rdd.first()


# Check the type of each field
for idx, field in enumerate(first_record):
   print(f"Field {idx}: {type(field)}")
