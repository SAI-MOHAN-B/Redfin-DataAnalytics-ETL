from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator  # type: ignore
from airflow.providers.amazon.aws.operators.emr import (EmrAddStepsOperator, EmrCreateJobFlowOperator,
                                                        EmrModifyClusterOperator, EmrTerminateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

JOB_FLOW_OVERRIDES = {
    "Name": "redfin_emr_cluster",
    "ReleaseLabel": "emr-6.13.0",
    "Applications": [{"Name": "Spark"}, {"Name": "JupyterEnterpriseGateway"}],
    "LogUri": "s3://redfin-data-engineering/emr-logs/",
    "VisibleToAllUsers": False,
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "Ec2SubnetId": "subnet-0cdd4446b2131a2ee",
        "Ec2KeyName": "data-project",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

default_args = {
    'owner': 'airflow',
    'depdends_on_past': False,
    'start_date': datetime(2023, 5, 27),
    'email': ['abc@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}
SPARK_STEPS_EXTRACTION = [
    {
        "Name": "Extract_Redfin_data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "s3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar",
            "Args": ["s3://redfin-data-engineering/scripts/ingest.sh"],
        },
    },
]

SPARK_STEPS_TRANSFORMATION = [{
    "Name": "Transform_Redfin_data",
    "ActionOnFailure": "CANCEL_AND_WAIT",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": ["spark-submit",
                 "s3://redfin-data-engineering/scripts/transform_redfin_data.py"
                 ],
    },
},
]
with DAG('redfin_analytics_spark_job_dag', default_args=default_args, catchup=False) as dag:
    start_pipeline = DummyOperator(task_id='task_start_pipeline')
    # Create an EMR Cluster
    create_emr_cluster = EmrCreateJobFlowOperator(task_id="task_create_emr_cluster",
                                                  job_flow_overrides=JOB_FLOW_OVERRIDES)

    is_emr_cluster_created = EmrJobFlowSensor(
        task_id='task_is_emr_cluster_created',
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster',key='return_value')}}",
        target_states={"WAITING"},
        timeout=3600,
        poke_interval=5,
        mode='poke'
    )

    # Add Your Steps to the EMR Cluster
    add_extraction_steps = EmrAddStepsOperator(
        task_id='task_add_extraction_steps',
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster',key='return_value')}}",
        steps=SPARK_STEPS_EXTRACTION
    )
    # Check whether the Extraction is completed or not
    is_extraction_completed = EmrStepSensor(
        task_id="task_is_extraction_completed",
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster',key='return_value')}}",
        step_id="{{task_instance.xcom_pull(task_ids='task_add_extraction_steps')[0]}}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=5,
    )
    # Check for the Transformation Steps
    add_transformation_step = EmrAddStepsOperator(
        task_id="task_add_transformation_step",
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster', key='return_value')}}",
        steps=SPARK_STEPS_TRANSFORMATION
    )
    is_transformation_completed = EmrStepSensor(
        task_id="task_is_transformation_completed",
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster',key='return_value')}}",
        step_id="{{task_instance.xcom_pull(task_ids='task_add_transformation_step')[0]}}",
        target_states={"COMPLETED"},
        timeout=3600,
        poke_interval=10,
    )

    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="task_remove_cluster",
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster',key='return_value')}}"
    )

    is_emr_cluster_terminated = EmrJobFlowSensor(
        task_id="task_is_emr_cluster_terminated",
        job_flow_id="{{task_instance.xcom_pull(task_ids='task_create_emr_cluster',key='return_value')}}",
        target_states={"TERMINATED"},
        timeout=3600,
        poke_interval=5,
        mode='poke'
    )
    end_pipeline = DummyOperator(task_id="task_end_pipeline")

    start_pipeline >> create_emr_cluster >> is_emr_cluster_created >> add_extraction_steps >> is_extraction_completed
    is_extraction_completed >> add_transformation_step >> is_transformation_completed >> remove_cluster
    remove_cluster >> is_emr_cluster_terminated >> end_pipeline


