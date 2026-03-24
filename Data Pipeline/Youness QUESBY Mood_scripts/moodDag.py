from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

# DAG default arguments
default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'mood_processing_dag',  # DAG Name
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

# Task to check Hadoop version
hadoop_version = BashOperator(
    task_id="check_hadoop",
    bash_command="echo $PATH",
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HADOOP_HOME": "/opt/hadoop",
    },
    dag=dag,
)

# Step 1: Scraping the data
scrape_data = BashOperator(
    task_id='scrape_data',
    bash_command='python3 /home/toto/script.py',
    dag=dag,
)

# Step 2: Uploading scraped data to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command='hadoop fs -put /home/toto/downloaded_datasets /user/airflow',
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HADOOP_HOME": "/opt/hadoop",
        "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64/"
    },
    dag=dag,
)

# Step 3: Deleting the scraped data after uploading it to HDFS
delete_local_data = BashOperator(
    task_id='delete_local_data',
    bash_command='rm -rf /home/toto/downloaded_datasets',
    dag=dag,
)

# Step 4: Run Spark Job with MongoDB Connector
run_spark_job = SSHOperator(
    task_id='run_spark_job',
    ssh_conn_id='hadoop_ssh_connection',
    command='export HADOOP_CONF_DIR=/home/hduser/hadoop-3.3.6/etc/hadoop && \
    export YARN_CONF_DIR=/home/hduser/hadoop-3.3.6/etc/hadoop && \
    /opt/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
    --conf spark.yarn.submit.waitAppCompletion=true \
    /home/hduser/newSparkScript.py',
    execution_timeout=timedelta(seconds=7200),
    dag=dag,
)

# Set task dependencies
hadoop_version >> scrape_data >> upload_to_hdfs >> delete_local_data >> run_spark_job