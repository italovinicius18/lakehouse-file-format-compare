from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime


@dag(
    dag_id="sisvan_full_pipeline",
    start_date=datetime(2025, 5, 12),
    tags=["sisvan", "full", "pipeline"],
)
def sisvan_full_pipeline():

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_bronze_to_silver",
        trigger_dag_id="bronze_to_silver",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_silver_to_gold",
        trigger_dag_id="silver_to_gold",
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_silver >> trigger_gold


sisvan_full_pipeline()
