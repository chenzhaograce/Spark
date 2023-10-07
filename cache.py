# Cache a read-only variable on each worker node instead of shipping a copy of it with tasks


from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


# Create a Spark session
spark = SparkSession.builder.appName("Broadcast Example").getOrCreate()


# Sample data
data_large = [(i, f"val_{i}") for i in range(1, 10001)]
data_small = [(i, f"val_{i}") for i in range(1, 101)]


# Create DataFrames
df_large = spark.createDataFrame(data_large, ["id", "value_large"])
df_small = spark.createDataFrame(data_small, ["id", "value_small"])


# Use broadcast join for optimizing join operation
# Broadcasting df_small as it is smaller
# The broadcast() function is used to broadcast the smaller DataFrame (df_small) to all worker nodes.
joined_df = df_large.join(broadcast(df_small), df_large.id == df_small.id)
# Show the result
joined_df.show()


# Stop the Spark session
spark.stop()




'''
advantage:
Larger DataFrame: It is not shuffled/moved across the network.
Each worker node performs the join operation locally using its own partition(s)
'''
