import os
import airflow
import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from util import email_sender

daily_schedule = "30 01 * * *"
start_schedule = datetime.strptime("2020-10-23","%Y-%m-%d")
# end_schedule = datetime.strptime("2020-10-21","%Y-%m-%d")
# start_schedule = airflow.utils.dates.days_ago(5)
ds = "{{ ds }}"

current_dir = os.path.dirname(os.path.abspath(__file__))
load_jar_location = os.path.join(current_dir, "lib", "aep-elt-data-erasure-assembly-0.1.0-SNAPSHOT.jar")

default_args = {
    'owner': 'data-platform, Sanjeev',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'sanjeev.kumar@telenor.se',
    'retries': 3,
    'retry_delay': timedelta(minutes=10)#,
    #'on_failure_callback': email_sender.task_failure_callback
}
enrich_black_list = DAG(
    'enrich-blacklist-v5',
    default_args=default_args,
    start_date=start_schedule,
    schedule_interval=daily_schedule,
    concurrency=4,
    max_active_runs=4,
    dagrun_timeout=timedelta(hours=4)
)

env = "{{ var.value.env }}"
refresh_table = """
        kinit -k -t {{ var.value.keytab }} {{ var.value.principal }}
        beeline -u 'jdbc:hive2://{{ var.value.hive_loadbalancer }}:10000/default;principal=hive/{{ var.value.hive_loadbalancer }}@TSE.AWS.CLOUD;auth-kerberos' -e "use {{ params.database }}; msck repair table {{ params.table }};"
        """

file_sensor = S3KeySensor(
    task_id="s3-check-blacklist",
    bucket_key="s3a://telenor-se-aep-{env}-operations/RAW/EXPORTS/BLACKLIST/ingestion_date={ds}/*".format(**locals()),
    wildcard_match=True,
    aws_conn_id='aws_default',
    mode="reschedule",
    poke_interval=300,
    dag=enrich_black_list
)


refresh_table = BashOperator(
    task_id="refresh-blacklist",
    bash_command=refresh_table,
    params={"database": "operations_matrix", "table": "blacklist_raw"},
    dag=enrich_black_list
)


enrich_blacklist = SparkSubmitOperator(
    task_id="enrich_blacklist",
    application=load_jar_location,
    conf={
        "spark.yarn.executor.memoryOverhead": "2G",
        "spark.driver.memory": "1G",
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        "spark.hadoop.hive.output.file.extension": "-enriched.parquet",
        "spark.yarn.appMasterEnv.current_rundate": ds
    },
    java_class="se.telenor.aep.dataplatform.EnrichBlacklist",
    name="Enrich_Blacklist (%s)" % ds,
    conn_id='spark_default',
    executor_cores=1,
    executor_memory="3G",
    num_executors=10,
    spark_binary="/usr/bin/spark2-submit",
    principal="{{ var.value.principal }}",
    keytab="{{ var.value.keytab }}",
    pool='default_pool',
    dag=enrich_black_list
)
file_sensor >> refresh_table >> enrich_blacklist
