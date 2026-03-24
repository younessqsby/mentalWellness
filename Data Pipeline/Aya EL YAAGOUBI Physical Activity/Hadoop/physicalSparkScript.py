from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, when, rand, expr, udf, to_timestamp
from pyspark.sql.types import DateType
from datetime import datetime

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("StockDataTransformation") \
    .config("spark.mongodb.output.uri", "mongodb://192.168.1.11:27017/stock_data_db.transformed_data") \
    .getOrCreate()

# Define HDFS path for input data
hdfs_input_path = "/user/airflow/physical/lt-1-minute-historical-stock-data-2003-2024/LT_1min_data.csv"

# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(hdfs_input_path)

# Drop 'Adj Close' and 'Volume'
df = df.drop("Adj Close", "Volume")


df = df.withColumn("Date", to_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ss"))

# Order data chronologically
df = df.orderBy("Date")
# Remove null values in 'Open', 'High', 'Low', and 'Close'
df = df.dropna(subset=["Open", "High", "Low", "Close"])

# Define transformation ranges
step_ranges = {'Lazy': (0, 1999), 'Normal': (2000, 4999), 'Active': (5000, 9999), 'Very Active': (10000, 20000)}
distance_ranges = {'Lazy': (0, 1), 'Normal': (1, 3), 'Active': (3, 5), 'Very Active': (5, 10)}
heart_rate_ranges = {'Lazy': (60, 80), 'Normal': (80, 100), 'Active': (100, 120), 'Very Active': (120, 140)}
calories_ranges = {'Lazy': (50, 200), 'Normal': (200, 400), 'Active': (400, 700), 'Very Active': (700, 1200)}

# Segment data into 4 equal parts
total_rows = df.count()
segment_size = total_rows // 4
profiles = list(step_ranges.keys())

# Apply transformations based on segments
for i, profile in enumerate(profiles):
    min_steps, max_steps = step_ranges[profile]
    min_distance, max_distance = distance_ranges[profile]
    min_hr, max_hr = heart_rate_ranges[profile]
    min_calories, max_calories = calories_ranges[profile]

    start_idx = i * segment_size
    end_idx = (i + 1) * segment_size if i < 3 else total_rows

    min_open = df.agg(min("Open")).first()[0]
    max_open = df.agg(max("Open")).first()[0]
    min_high = df.agg(min("High")).first()[0]
    max_high = df.agg(max("High")).first()[0]
min_low = df.agg(min("Low")).first()[0]
    max_low = df.agg(max("Low")).first()[0]
    min_close = df.agg(min("Close")).first()[0]
    max_close = df.agg(max("Close")).first()[0]

    if None in [min_open, max_open, min_high, max_high, min_low, max_low, min_close, max_close]:
        print("Warning: One of the columns has all null values. Skipping transformation.")
        continue

    df = df.withColumn(
        "Open",
        when(col("Open").between(min_open, max_open),
             (((col("Open") - min_open) / (max_open - min_open)) * (max_steps - min_steps) + min_steps) * rand(>
    )

    df = df.withColumn(
        "High",
        when(col("High").between(min_high, max_high),
             (((col("High") - min_high) / (max_high - min_high)) * (max_distance - min_distance) + min_distance>
    )

    df = df.withColumn(
        "Low",
        when(col("Low").between(min_low, max_low),
             expr(f"CAST((rand() * ({max_hr} - {min_hr}) + {min_hr}) AS INT)"))
    )

    df = df.withColumn(
        "Close",
        when(col("Close").between(min_close, max_close),
             (((col("Close") - min_close) / (max_close - min_close)) * (max_calories - min_calories) + min_calo>
    )

# Function to cyclically replace year in 'Date' column
def replace_year_cyclically(date):
    if date is None:
        return None
    year = date.year
    new_year = 2022 + ((year - 2003) % 3)  # Cycles between 2022, 2023, 2024
    try:
        new_date = date.replace(year=new_year)
    except ValueError:  # Handle leap year issue
        new_date = date.replace(year=new_year, day=28) if date.month == 2 and date.day == 29 else date.replace(>
    return new_date

# Register UDF
replace_year_udf = udf(replace_year_cyclically, DateType())

# Convert 'Date' column and apply transformation
df = df.withColumn("Date", col("Date").cast("date"))
df = df.withColumn("Date", replace_year_udf(col("Date")))

# Rename columns correctly
df = df.withColumnRenamed("Open", "Steps") \
       .withColumnRenamed("High", "Distance_km") \
       .withColumnRenamed("Low", "Heart_rate") \
       .withColumnRenamed("Close", "Calories")

# Save the transformed DataFrame to MongoDB
df.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("uri", "mongodb://192.168.1.11:27017/stock_data_db.transformed_data") \
    .save()

# Stop the SparkSession
spark.stop()
