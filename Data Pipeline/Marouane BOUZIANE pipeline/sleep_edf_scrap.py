from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    "owner": "Admin",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 13),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "sleep_edfx_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    max_active_runs=1,
)

# Define paths
LOCAL_DIR = "/tmp/sleep_edfx"
HDFS_DIR = "/user/airflow/sleep_edfx"

# Step 1: Check Hadoop setup
check_hadoop = BashOperator(
    task_id="check_hadoop",
    bash_command="echo $PATH",
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HADOOP_HOME": "/opt/hadoop",
    },
    dag=dag,
)

# Step 2: Download Sleep-EDF dataset
download_data = BashOperator(
    task_id="download_sleep_edfx",
    bash_command=f"""
    mkdir -p {LOCAL_DIR} &&
    wget -qO- https://physionet.org/files/sleep-edfx/1.0.0/sleep-cassette/ |
    grep -oP '(?<=href=")[^"]*Hypnogram[^"]*' |
    while read file; do
        wget -N "https://physionet.org/files/sleep-edfx/1.0.0/sleep-cassette/$file" -P {LOCAL_DIR};
    done
    """,
    dag=dag,
)

# Step 3: Upload data to HDFS
upload_to_hdfs = BashOperator(
    task_id="upload_to_hdfs",
    bash_command=f"hdfs dfs -mkdir -p {HDFS_DIR} && hdfs dfs -put -f {LOCAL_DIR}/* {HDFS_DIR}",
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HADOOP_HOME": "/opt/hadoop",
        "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64/",
    },
    dag=dag,
)
# Step 4: Launch Spark Job

spark_command = """
export SPARK_HOME=/opt/spark && \
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && \
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/ && \
export PATH=/opt/spark/bin:/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH && \
source ~/.bashrc && \
/opt/spark/bin/spark-submit \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.crealytics:spark-excel_2.12:3.3.1_0.18.7 \
    --master yarn \
    /home/hduser/edf_sleep_job.py
"""

process_sleep_data = SSHOperator(
    task_id="process_sleep_data",
    ssh_conn_id="hadoop_ssh_connection",  # Must be defined in Airflow UI
    command=spark_command,
    dag=dag
)
# Step 4: Delete local files after upload
#delete_local_data = BashOperator(
#    task_id="delete_local_data",
#    bash_command=f"rm -rf {LOCAL_DIR}",
#    dag=dag,
#)

check_hadoop >> download_data >> upload_to_hdfs >> process_sleep_data