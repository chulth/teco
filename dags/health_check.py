from airflow.models.dag import DAG
from airflow.utils.task_group import TaskGroup
from datetime import timedelta
from plugins.inventory_plug.inventory_operator import InventoryHttpOperator
from plugins.core_plug.core_operator import CoreHttpOperator


# [START howto_task_group]
with DAG(dag_id="health_check", start_date=timedelta(min=20), tags=["health_check"]) as dag:
    start = DummyOperator(task_id="start")

    # [START howto_task_group_section_1]
    with TaskGroup("get_token", tooltip="Tasks for get token") as get_token:
        get_core_token= CoreHttpOperator(
            task_id='get__token',
            endpoint='/authenticate',
            method='POST',
            response_filter=lambda response: response.json()['auth_token'],
            log_response=True

        )
    
        get_core_token
    # [END howto_task_group_section_1]


    