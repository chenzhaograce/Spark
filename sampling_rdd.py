from pyspark import SparkContext


# Create a Spark context
sc = SparkContext("local", "Sampling Example")


# Create an RDD
data = sc.parallelize(range(1, 100))


# Perform sampling
sampled_rdd = data.sample(withReplacement=False, fraction=0.1, seed=42)


# Collect and print the sampled data
print(sampled_rdd.collect())