from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


# DAG default arguments
default_args = {
    'owner': 'Admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
        'scrap_isruc_sleep_datasets',  # DAG Name
    default_args=default_args,
    description='A DAG to download and process multiple .rar files based on ISRUC-Sleep dataset',
    schedule_interval=None,  # Run manually
    start_date=datetime(2025, 1, 17),
    catchup=False,
    max_active_runs=1,
)

# List of subgroups and subjects
subgroups = {
   "subgroupI": list(range(1, 101)),  # Subgroup 1: Subjects 1-100
   "subgroupII": list(range(1, 9)),   # Subgroup 2: Subjects 1-8
   "subgroupIII": list(range(1, 11))  # Subgroup 3: Subjects 1-10
}

all_uploads_done = DummyOperator(task_id="all_uploads_done", dag=dag, trigger_rule=TriggerRule.ALL_DONE)

# Base URL for downloading files
base_url = "http://dataset.isr.uc.pt/ISRUC_Sleep"

def create_tasks(subgroup, subject):
    file_name = f"{subject}.rar"
    file_url = f"{base_url}/{subgroup}/{file_name}"
    local_path = f"~/temp_data/{subgroup}_{subject}.rar"
    extract_path = f"~/temp_data/extracted_{subgroup}_{subject}"

    # Step 1: Download the .rar file
    download_task = BashOperator(
        task_id=f'download_{subgroup}_{subject}',
        bash_command=f'wget -N -O {local_path} {file_url}',
        dag=dag,
    )

    # Step 2: Extract the .rar file (requires unrar installed)
    extract_task = BashOperator(
        task_id=f'extract_{subgroup}_{subject}',
        bash_command=f'unrar x {local_path} {extract_path}/',
        dag=dag,
    )

    # Step 3: Upload extracted files to HDFS
    upload_task = BashOperator(
        task_id=f'upload_{subgroup}_{subject}',
        bash_command=f'hadoop fs -put {extract_path}/* /user/airflow',
        env={
            "PATH": "/opt/hadoop/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "HADOOP_HOME": "/opt/hadoop",
            "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64/"
        },
        dag=dag,
    )

    # Step 4: Cleanup local files
    #cleanup_task = BashOperator(
        #task_id=f'cleanup_{subgroup}_{subject}',
        #bash_command=f'rm -rf {local_path} {extract_path}',
     #   dag=dag,
    #)

    # Set task dependencies
    download_task >> extract_task >> upload_task >> all_uploads_done

# Create tasks for all subgroups and subjects
for subgroup, subjects in subgroups.items():
    for subject in subjects:
        create_tasks(subgroup, subject)



spark_command = """
export SPARK_HOME=/opt/spark && \
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop && \
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/ && \
export PATH=/opt/spark/bin:/opt/hadoop/bin:$PATH && \
source ~/.bashrc && \
spark-submit \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,com.crealytics:spark-excel_2.12:3.3.1_0.18.7 \
    --master yarn \
    /home/hduser/irusc_sleep_job.py
"""


process_sleep_data = SSHOperator(
    task_id="process_sleep_data",
    ssh_conn_id="hadoop_ssh_connection",
    command=spark_command,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE  
)


all_uploads_done >> process_sleep_data
