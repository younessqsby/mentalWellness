from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'physical_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,  
    max_active_runs=1,
)

hadoop_version = BashOperator(
    task_id="check_hadoop",
    bash_command="echo $PATH",
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/>
        "HADOOP_HOME": "/opt/hadoop",
    },
    dag=dag,
)

# Step 1: Scraping the data
scrape_data = BashOperator(
    task_id='download_physical_data',
    bash_command='python3 /home/toto/script-PHY.py', 
    dag=dag,
)

# Step 2: Uploading scraped data to HDFS
upload_to_hdfs = BashOperator(
    task_id='upload_to_hdfs',
    bash_command='hadoop fs -put /home/toto/physical /user/airflow',
    env={
        "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/>
        "HADOOP_HOME": "/opt/hadoop",
        "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64/"
    },
dag=dag,
)

# Step 3: Delete local data after upload
delete_local_data = BashOperator(
    task_id='delete_local_data',
    bash_command='rm -rf /home/toto/physical',  
    dag=dag,
)

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
    /home/hduser/physicalSparkScript.py',
    execution_timeout=timedelta(seconds=7200),
    dag=dag,
)

# Set task dependencies
hadoop_version >> scrape_data >> upload_to_hdfs >> delete_local_data >> run_spa>

