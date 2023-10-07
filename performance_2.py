'''
The repartition operation involves
 a full shuffle of the data, which means that all the data is redistributed
 across the network to create a new set of partitions.


Before performing a join, you might repartition on the join key to avoid shuffles during the join operation.


'''


from pyspark.sql import SparkSession


# Create a Spark session
spark = SparkSession.builder.appName("RepartitionJoinExample").getOrCreate()

# Create two DataFrames
data1 = [("Alice", 1), ("Bob", 2), ("Cathy", 3), ("David", 4)]
columns1 = ["name", "id"]
df1 = spark.createDataFrame(data1, columns1)


data2 = [(1, "F"), (2, "M"), (3, "F"), (4, "M")]
columns2 = ["id", "gender"]
df2 = spark.createDataFrame(data2, columns2)


# Repartition DataFrames on the join key "id"
df1_repartitioned = df1.repartition("id")
df2_repartitioned = df2.repartition("id")


# Perform join operation
joined_df = df1_repartitioned.join(df2_repartitioned, df1_repartitioned.id == df2_repartitioned.id)


# Show the result
joined_df.show()


# Stop the Spark session
spark.stop()


'''
repartitioning can minimize shuffling for the join, but the repartition operation itself involves a shuffle
and could be computationally expensive.
Always consider the trade-off, or you can test to ensure that the optimization is beneficial for your use case.
'''