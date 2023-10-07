from pyspark import SparkContext


sc = SparkContext("local", "Example")
def square(x):
   return x ** 2
rdd = sc.parallelize([1, 2, 3, 4, 5])


squared_rdd = rdd.map(square)


print(squared_rdd.collect())