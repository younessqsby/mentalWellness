from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import pyedflib
import uuid
from datetime import datetime, timedelta
import io
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("Sleep Data Processor") \
        .config("spark.mongodb.output.uri", "mongodb://192.168.1.11:27017/sleep_db") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .getOrCreate()



def list_hdfs_files(spark, hdfs_path):
    """Retrieve all file paths from HDFS directory using Hadoop API."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)

    if not fs.exists(path):
        print(f"Directory {hdfs_path} does not exist in HDFS.")
        return []

    status = fs.listStatus(path)
    return [file.getPath().toString() for file in status if not file.isDirectory()]


def process_hypnogram(spark, file_path):
    local_path = "/tmp/" + str(uuid.uuid4()) + ".edf"

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        src_path = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        dest_path = spark._jvm.org.apache.hadoop.fs.Path(local_path)

        # Check if the file exists in HDFS and download
        if fs.exists(src_path):
            fs.copyToLocalFile(src_path, dest_path)
            print("file copied...")
        else:
            print(f"File {file_path} does not exist in HDFS.")
            return None, []

        # Now process the file locally with pyedflib
        hypno_f = pyedflib.EdfReader(local_path)
        ann_signals = hypno_f.readAnnotations()

        data = []
        user_id = str(uuid.uuid4())
        start_time = datetime.combine(datetime.today(), datetime.min.time())


        valid_stages = {"W", "N1", "N2", "N3"}

        for onset, duration, stage in zip(*ann_signals):
            if 'Sleep stage' in stage:
                stage_label = stage.replace('Sleep stage ', '').strip()
                if stage_label in valid_stages:  # Only keep valid stages 
                    data.append({
                        "Id": user_id,
                        "timestamp": start_time + timedelta(seconds=onset),
                        "duration": float(duration),
                        "Stage": stage_label
                    })
        return user_id, data

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return None, []
    finally:
        # Clean up the local temporary file after processing
        if os.path.exists(local_path):
            os.remove(local_path)



def write_to_mongodb(spark, collection, data):
    if not data:
        print("No data to write, skipping MongoDB insertion.")
        return

    # Remove rows where any value is None by any chance :) 
    cleaned_data = [row for row in data if None not in row.values()]

    if not cleaned_data:
        print("All rows contained None values. No valid data to insert.")
        return

    if data:
        schema = StructType([
            StructField("Id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("duration", FloatType(), False),
            StructField("Stage", StringType(), False)
        ])

        df = spark.createDataFrame(data, schema)
        df.write.format("mongo").mode("append").option("collection", collection).save()

def main():
    spark = create_spark_session()
    hdfs_path = "/user/airflow/sleep_edfx"
    hypnogram_files = list_hdfs_files(spark, hdfs_path)

    for file_path in hypnogram_files:
        user_id, data = process_hypnogram(spark, file_path)

        if user_id:
         #   write_to_mongodb(spark, "user_metadata", [{"Id": user_id}])
            write_to_mongodb(spark, "sleep_data", data)

    spark.stop()

if __name__ == "__main__":
    main()                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               ~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ~                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ~                     