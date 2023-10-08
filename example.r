# Load the sparklyr package
library(sparklyr)

spark_install()

# Connect to Spark
sc <- spark_connect(master = "local")

# Read data from a CSV file into a Spark DataFrame
df <- spark_read_csv(sc, "file:///Users/czzgrace/Documents/Spark/titanic_train.csv")

# Filter the DataFrame to only include rows where the age column is greater than 30
df_filtered <- filter(df, Age > 30)

# Retrieve the filtered DataFrame back into R
df_filtered_r <- collect(df_filtered)

# Print the first 10 rows of the filtered DataFrame
head(df_filtered_r)

# Disconnect from Spark
spark_disconnect(sc)
