import os
import airflow
import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.bash_operator import BashOperator
# from util import email_sender
import json
import glob

daily_schedule = "5 11 * * *"
start_schedule = datetime.strptime("2020-10-09","%Y-%m-%d")
# start_schedule = airflow.utils.dates.days_ago(5)
ds = "{{ ds }}"
prev_ds = "{{ prev_execution_date_success }}"

current_dir = os.path.dirname(os.path.abspath(__file__))
load_jar_location = os.path.join(current_dir, "lib", "aep-elt-data-erasure-assembly-0.1.0-SNAPSHOT.jar")


default_args = {
    'owner': 'data-platform, Sanjeev',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'sanjeev.kumar@telenor.se',
    'retries': 3,
    'retry_delay': timedelta(minutes=10)#,
    #'on_failure_callback': email_sender.task_failure_callback
}
load_to_access = DAG(
    'data-erasure-v5',
    default_args=default_args,
    start_date=start_schedule,
    schedule_interval=daily_schedule,
    concurrency=5,
    max_active_runs=5,
    dagrun_timeout=timedelta(days=1)
)

is_active_default = "false"
refresh_table = """
        kinit -k -t {{ var.value.keytab }} {{ var.value.principal }}
        impala-shell -i {{ var.value.impala_loadbalancer }} -q "use {{ params.database }}; invalidate metadata {{ params.table }};"
        """
# beeline -u 'jdbc:hive2://lb-impala-c03fa9db7669945b.elb.eu-north-1.amazonaws.com:21050/default;principal=impala/lb-impala-c03fa9db7669945b.elb.eu-north-1.amazonaws.com@TSE.AWS.CLOUD;auth-kerberos' -e "use {{ params.database_name }}; invalidate metadata {{ params.table_name }};"

def create_table_pipeline(schema, spec, pool):
    is_active = spec.get("is_active") or is_active_default
    if is_active == "true":
        env =  "{{ var.value.env }}"
        db = spec.get("db").lower()
        table = spec.get("table").lower()
        first_run = spec.get("first_run")
        join_query = spec.get("join_query_to_build_table_should_contain_blacklist_column")
        erasure_job = SparkSubmitOperator(
            task_id="data-erasure-%s" % table,
            application=load_jar_location,
            conf={
                "spark.executor.memoryOverhead": "2G",
                "spark.driver.memory": "2G",
                "spark.hadoop.hive.output.file.extension": "-erased-blacklist.parquet",
                "spark.sql.shuffle.partitions": "1500",
                "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "1",
                "spark.yarn.appMasterEnv.blacklist_base_filepath": "s3a://telenor-se-aep-{env}-operations/RAW/EXPORTS/BLACKLIST/".format(**locals()),
                "spark.yarn.appMasterEnv.current_rundate": ds,
                "spark.yarn.appMasterEnv.prev_success_rundate": prev_ds,
                "spark.yarn.appMasterEnv.erasure_db": db,
                "spark.yarn.appMasterEnv.erasure_table": table,
                "spark.yarn.appMasterEnv.join_query_to_build_table": join_query,
            },
            java_class="se.telenor.aep.dataplatform.DataErasure",
            name="Data-Erasure-%s (%s)" % (table, ds),
            conn_id='spark_default',
            executor_cores=1,
            executor_memory="4G",
            num_executors=10,
            spark_binary="/usr/bin/spark2-submit",
            principal="{{ var.value.principal }}",
            keytab="{{ var.value.keytab }}",
            application_args=[],
            pool=pool,
            dag=load_to_access
        )
        refresh_impala = BashOperator(
            task_id="refresh-%s" % table,
            bash_command=refresh_table,
            params={"database": db, "table": table},
            dag=load_to_access
        )
        erasure_job >> refresh_impala

# Read JSON File
def read_conf():
    json_files = glob.glob(os.path.join(current_dir, "config", 'data-erasure.json'))
    json_file = open(os.path.abspath(json_files[0]), encoding="utf-8")
    job_configurations = json.load(json_file)

    for schema, schema_spec in job_configurations.items():
        pool = schema_spec.get("pool")
        for spec in schema_spec.get("tables"):
            create_table_pipeline(schema.upper(), spec, pool)

read_conf()
