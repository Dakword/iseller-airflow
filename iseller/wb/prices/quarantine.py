import json
from datetime import datetime, timedelta

from airflow.decorators import task, dag, task_group
from airflow.models import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from iseller.utils.etl_helpers import validate_json_schema, etl_transform_api_response, etl_hook_insert_data
from iseller.utils.tasks_factory import tf_create_table
from iseller.wb import Config
from iseller.wb.sellers import get_sellers_ids, get_seller_token
from iseller.wb.wb_tasks_factory import check_seller_token_task
from iseller.wbseller import PricesApi

DAG1_NAME = "wb_prices_quarantine"
DAG2_NAME = "wb_prices_quarantine_seller"
CONN_ID = Config.CONN_ID
TABLE = "prices_quarantine"
TABLE_CREATE_SQL = "./sql/prices_quarantine.sql"


@dag(
    DAG1_NAME,
    dag_display_name="WB Цены и скидки - Карантин цен 🚀",
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=40),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    },
    catchup=False,
    max_active_runs=1,
    tags=["wb", "prices", "quarantine"],
)
def dag1_tasks():
    sync_time = int(datetime.now().timestamp())

    create_table = tf_create_table(CONN_ID, {TABLE: TABLE_CREATE_SQL})

    for task_seller_id in get_sellers_ids():
        @task_group(group_id=f"seller_{task_seller_id}")
        def seller_tasks(seller_id, sync_time):
            start_download = TriggerDagRunOperator(
                task_id=f"seller_{seller_id}",
                task_display_name="🚀 Получить карантин цен поставщика",
                trigger_dag_id=DAG2_NAME,
                conf={
                    "seller_id": f"{seller_id}",
                    "limit": 1000,
                    "offset": 0,
                    "sync_time": sync_time,
                },
                wait_for_completion=False,
                trigger_rule=TriggerRule.NONE_FAILED,
            )

            check_token = check_seller_token_task(seller_id, PricesApi.NAME)

            check_token >> start_download

        create_table >> seller_tasks(task_seller_id, sync_time)


dag1_tasks()

# ----------------------------------------------------------------------------------------------------------------------

XCOM_TASK_ID = "extract"
XCOM_KEY = "next"


def trigger_params(**context):
    get_next = json.loads(context["context"]["ti"].xcom_pull(key=XCOM_KEY, task_ids=XCOM_TASK_ID, dag_id=DAG2_NAME))
    return {
        "seller_id": f"{get_next["seller_id"]}",
        "sync_time": get_next["sync_time"],
        "limit": get_next["limit"],
        "offset": get_next["offset"],
    }


@dag(
    DAG2_NAME,
    dag_display_name="WB Цены и скидки - Карантин цен поставщика 🖐️",
    default_args={
        "retries": 4,
        "retry_delay": timedelta(minutes=1)
    },
    params={
        "seller_id": Param(0, type="string", enum=list(map(lambda id: str(id), get_sellers_ids()))),
        "limit": Param(1000, type="integer"),
        "offset": Param(0, type="integer"),
        "sync_time": Param(int(datetime.now().timestamp()), type="integer"),
    },
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["wb", "prices", "quarantine", "sub"],
)
def dag2_tasks():
    @task
    def extract(**context) -> list:
        params = context["params"]
        api_response = PricesApi(token=get_seller_token(int(params["seller_id"]))).quarantine(
            offset=params["offset"],
            limit=params["limit"]
        )
        validate_json_schema(api_response, {
            "type": "object",
            "properties": {
                "data": {
                    "properties": {
                        "quarantineGoods": {
                            "type": "array",
                            "items": {"type": "object"},
                        }
                    },
                },
                "error": {"type": "boolean", "enum": [False]},
            }
        })

        prices = api_response["data"].get("quarantineGoods", []) if api_response["data"] else []

        if len(prices) == int(params["limit"]):
            context["ti"].xcom_push(key=XCOM_KEY, value=json.dumps({
                "get": True,
                "seller_id": params["seller_id"],
                "sync_time": int(params["sync_time"]),
                "limit": int(params["limit"]),
                "offset": (int(params["offset"]) + int(params["limit"])),
            }))
        else:
            context["ti"].xcom_push(key=XCOM_KEY, value=json.dumps({
                "get": False,
            }))

        return prices

    @task()
    def transform(api_response: tuple, **context) -> tuple:
        params = context["params"]
        return etl_transform_api_response(
            api_response, {
                "nm_id": lambda api: api.get("nmID"),
                "old_price": lambda api: api.get("oldPrice"),
                "new_price": lambda api: api.get("newPrice"),
                "old_discount": lambda api: api.get("oldDiscount"),
                "new_discount": lambda api: api.get("newDiscount"),
                "price_diff": lambda api: api.get("priceDiff"),
            }, {
                "seller_id": params["seller_id"],
                "sync_time": params["sync_time"],
            })

    @task
    def load(transformed: tuple):
        fields, transformed_data = transformed
        if len(transformed_data) > 0:
            etl_hook_insert_data(
                CONN_ID, TABLE,
                fields=fields, transformed_data=transformed_data,
                replace=True,
                replace_index="nm_id",
            )

    @task.branch(task_display_name="Запросить еще?")
    def check_stop(**context):
        get_next = json.loads(context["ti"].xcom_pull(key=XCOM_KEY, task_ids=XCOM_TASK_ID, dag_id=DAG2_NAME))
        return "self_trigger" if get_next["get"] else "stop_task"

    self_trigger = TriggerDagRunOperator(
        task_id="self_trigger",
        task_display_name="Загрузка следующей порции",
        trigger_dag_id=DAG2_NAME,
        conf=trigger_params,
        reset_dag_run=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        execution_date="{{ (macros.datetime.now() + macros.timedelta(seconds=1)).isoformat() }}",
    )

    @task(task_display_name="Загрузка завершена!")
    def stop_task(**context):
        params = context["params"]
        PostgresHook(postgres_conn_id=CONN_ID).run(
            f"DELETE FROM {TABLE} WHERE seller_id = {params["seller_id"]} AND sync_time <> {params["sync_time"]};"
        )



    load(transform(extract())) >> check_stop() >> [self_trigger, stop_task()]


dag2_tasks()
