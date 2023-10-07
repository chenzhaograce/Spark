from pyspark import SparkContext
import requests
import re

# Create a SparkContext
sc = SparkContext("local", "Word Count App")
#sc.setLogLevel("INFO")  # Change "INFO" to "DEBUG" for even more detailed logging

# Fetch the text file from GitHub
url = "https://raw.githubusercontent.com/acgould/word-count/master/wordswithpunct.txt"
response = requests.get(url)
text_data = response.text.splitlines()

# Create an RDD from the text data
text_rdd = sc.parallelize(text_data)

# Define a function to clean and split lines into words
def clean_and_split(line):
    # Remove punctuation and convert to lowercase
    line = re.sub(r'[^\w\s]', '', line).lower()
    # Split the line into words
    return line.split()

# Clean and split the lines, then count the words
word_counts =text_rdd.flatMap(clean_and_split).countByValue()

# Print the word counts
for word, count in word_counts.items():
    print(f"{word}: {count}")

# Stop the SparkContext
sc.stop()
