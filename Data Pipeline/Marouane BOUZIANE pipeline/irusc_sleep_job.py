from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, expr, to_timestamp, input_file_name, regexp_extract, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
import uuid
from datetime import datetime, timedelta
import re
import numpy as np

def create_spark_session():
    return SparkSession.builder \
        .appName("Sleep Data Processor") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.1.11:27017/sleep_db") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()

def extract_number(text):
    """Extract the first number found in a text string."""
    if text is None:
        return None
    match = re.search(r'\d+(?:\.\d+)?', str(text))
    return float(match.group()) if match else None

def process_metadata(df):
    """Extract metadata from the Excel file."""
    # Convert to Pandas for easier metadata extraction
    pdf = df.toPandas()

    metadata = {
        "Id": str(uuid.uuid4()),
        "sex": None,
        "age": None,
        "height": None,
        "weight": None,
    }

    for index, row in pdf.iterrows():
        for i, cell in enumerate(row):
            if isinstance(cell, str):
                cell_clean = cell.strip().lower().replace(":", "")
                if cell_clean == "sex":
                    metadata["sex"] = str(row.iloc[i + 1]) if i + 1 < len(row) else None
                elif cell_clean == "age":
                    age_value = row.iloc[i + 1] if i + 1 < len(row) else None
                    metadata["age"] = float(age_value) if age_value is not None else None
                elif cell_clean == "height":
                    raw_height = pdf.iloc[index + 1].iloc[i] if index + 1 < len(pdf) else None
                    metadata["height"] = extract_number(raw_height)
                elif cell_clean == "weight":
                    raw_weight = pdf.iloc[index + 1].iloc[i] if index + 1 < len(pdf) else None
                    metadata["weight"] = extract_number(raw_weight)

    return metadata

def process_sleep_data(df, metadata):
    sleep_data = df.select(
        col("_c0").alias("Epoch"),
        col("_c1").alias("Stage"),
        col("_c2").alias("SpO2"),
        col("_c3").alias("HR")
    )

    # Filter out header rows and invalid stages
    valid_stages = {"W", "N1", "N2", "N3"}
    sleep_data = sleep_data.filter(col("Stage").isin(valid_stages))

    # Generate timestamps
    start_time = datetime.combine(datetime.today(), datetime.strptime("23:00:00", "%H:%M:%S").time())
    random_offset = np.random.randint(-60, 60)
    start_time = start_time + timedelta(minutes=random_offset)

    # Create timestamp generation function
    def generate_timestamp(epoch_number):
        return start_time + timedelta(seconds=30 * epoch_number)

    timestamp_udf = udf(generate_timestamp, TimestampType())

    # Add timestamps and other required columns
    sleep_data = sleep_data.withColumn("row_number", expr("row_number() over (order by 1)")) \
        .withColumn("timestamp", timestamp_udf(col("row_number"))) \
        .withColumn("duration", expr("30")) \
        .withColumn("Id", expr(f"'{metadata['Id']}'"))

    return sleep_data.select("Id", "timestamp", "duration", "Stage")

def process_single_file(spark, file_path):
    try:
        # Read Excel file
        df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "false") \
            .option("inferSchema", "true") \
            .load(file_path)

        # Process metadata
        metadata = process_metadata(df)
        print("Metadata for file:", file_path)
        print(metadata)

        # Create metadata DataFrame
        metadata_schema = StructType([
            StructField("Id", StringType(), False),
            StructField("sex", StringType(), True),
            StructField("age", FloatType(), True),
            StructField("height", FloatType(), True),
            StructField("weight", FloatType(), True)
        ])

        metadata_df = spark.createDataFrame([metadata], metadata_schema)
        sleep_data_df = process_sleep_data(df, metadata)

        return metadata_df, sleep_data_df
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None, None


def write_to_mongodb(df, collection_name):
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("collection", collection_name) \
        .save()

def main():
    spark = create_spark_session()

    # Initialize empty DataFrames for accumulating results
    metadata_df = None
    sleep_data_df = None

    # Process each file
    for subject in range(1, 101):
        file_path = f"/user/airflow/{subject}/{subject}_1.xlsx"
        try:
            current_metadata_df, current_sleep_df = process_single_file(spark, file_path)
            if current_metadata_df is not None and current_sleep_df is not None:
                # Union with accumulated results
                metadata_df = current_metadata_df if metadata_df is None else metadata_df.union(current_metadata_df)
                sleep_data_df = current_sleep_df if sleep_data_df is None else sleep_data_df.union(current_sleep_df)
                print(f"Successfully processed subject {subject}")
        except Exception as e:
            print(f"Error processing subject {subject}: {str(e)}")
            continue

    # Write results to MongoDB
    if metadata_df is not None and sleep_data_df is not None:
        try:
            print("Trying to save data in MongoDB")
            write_to_mongodb(metadata_df, "user_metadata")
            write_to_mongodb(sleep_data_df, "sleep_data")
            print("Successfully wrote results to MongoDB")
        except Exception as e:
            print(f"Error writing to MongoDB: {str(e)}")
    else:
        print("No data was processed successfully")

    spark.stop()

if __name__ == "__main__":
    main()
