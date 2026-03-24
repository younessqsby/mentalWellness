from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LiquorSalesProcessing") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.1.11:27017/mood_statistics_db.mood") \
    .getOrCreate()

# Define the HDFS path for the input file
hdfs_input_path = "/user/airflow/downloaded_datasets/big-sales-data/Sales_Data/Liquor_Sales.csv"

# Read the CSV file into a DataFrame
liquor_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(hdfs_input_path)

# Select necessary columns and rename them
transformed_df = liquor_df.select(
    col("Invoice/Item Number").alias("Id"),
    col("Date").alias("Date"),
    col("Zip Code").alias("Mood"),
    col("County Number").alias("Intensity"),
    col("State Bottle Retail").alias("Duration"),
    col("Bottles Sold").alias("Physical_Activity")
)

# Transform the values in the "Mood" column based on Zip Code
transformed_df = transformed_df.withColumn(
    "Mood",
    when(col("Mood") < 50600, "Happy")
    .when((col("Mood") >= 50600) & (col("Mood") < 51230), "Sad")
        .when((col("Mood") >= 51230) & (col("Mood") < 51856), "Anxious")
    .when((col("Mood") >= 51856) & (col("Mood") < 52474), "Angry")
    .otherwise("Calm")
)
# Add the "Physical_Activity" column based on Bottles Sold
transformed_df = transformed_df.withColumn(
    "Physical_Activity",
    when((col("Physical_Activity") >= 1) & (col("Physical_Activity") <= 10), "Active")
    .when((col("Physical_Activity") > 10) & (col("Physical_Activity") <= 50), "Mediocre")
    .otherwise("Nonactive")
)

# Save the transformed DataFrame to MongoDB
transformed_df.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("uri", "mongodb://192.168.1.11:27017/mood_statistics_db.mood") \
    .save()

# Stop the SparkSession
spark.stop()