from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.operators.emr_step_sensor import EmrStepSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_combined = DAG(
    'dag_combined',
    default_args=default_args,
    description='Combined DAG for Spark Jobs and EMR Steps',
    schedule_interval='@daily',  # Adjust the schedule interval as needed
)

# EMR Spark Step Configuration
emr_spark_step = {
    'Name': 'EMRSparkStep',
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['spark-submit', '/path/to/your/app_s3_to_ec2.jar']  # Replace with the actual path
    }
}

# EMR Add Steps Operator
emr_step_adder = EmrAddStepsOperator(
    task_id='add_emr_spark_step',
    job_flow_id='j-2A5H6N96VNGSN',  # Replace with your EMR cluster ID
    aws_conn_id='aws_default',
    steps=[emr_spark_step],
    dag=dag_combined
)

# EMR Step Sensor
emr_step_checker = EmrStepSensor(
    task_id='watch_emr_spark_step',
    job_flow_id='j-2A5H6N96VNGSN',  # Replace with your EMR cluster ID
    step_id="{{ task_instance.xcom_pull(task_ids='add_emr_spark_step')['StepIds'][0] }}",
    aws_conn_id='aws_default',
    dag=dag_combined
)

# Bash Operators for Local Spark Jobs
t1 = BashOperator(
    task_id='t1_s3_to_ec2',
    bash_command='spark-submit /path/to/your/app_s3_to_ec2.jar',  # Replace with the actual path
    dag=dag_combined,
)

t2 = BashOperator(
    task_id='t2_transform_and_save',
    bash_command='spark-submit /path/to/your/app_transform_and_save.jar',  # Replace with the actual path
    dag=dag_combined,
)

t3 = BashOperator(
    task_id='t3_export_to_s3',
    bash_command='spark-submit /path/to/your/app_export_to_s3.jar',  # Replace with the actual path
    dag=dag_combined,
)

# Set up dependencies
t1 >> t2 >> t3 >> emr_step_adder >> emr_step_checker