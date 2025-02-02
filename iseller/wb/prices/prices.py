import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task, task_group, dag
from airflow.models import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from iseller.utils.etl_helpers import etl_hook_insert_data, validate_json_schema
from iseller.utils.tasks_factory import tf_create_table
from iseller.wb.config import Config
from iseller.wb.wb_tasks_factory import check_seller_token_task
from iseller.wb.sellers import get_sellers_ids, get_seller_token
from iseller.wbseller import PricesApi

DAG1_NAME = "wb_prices"
DAG2_NAME = "wb_prices_seller"
CONN_ID = Config.CONN_ID
TABLE = "prices"
CREATE_TABLE_SQL = "./sql/prices.sql"


@dag(DAG1_NAME,
     dag_display_name="WB Цены и скидки - Получение цен 🚀",
     start_date=days_ago(1),
     schedule_interval=timedelta(minutes=35),
     default_args={
         'retries': 2,
         'retry_delay': timedelta(minutes=5)
     },
     catchup=False,
     tags=['wb', 'prices'],
     max_active_runs=1,
     )
def dag1_tasks():
    sync_time = int(datetime.now().timestamp())
    create_tables = tf_create_table(CONN_ID, {TABLE: CREATE_TABLE_SQL})

    for task_seller_id in get_sellers_ids():
        @task_group(group_id=f"seller_{task_seller_id}")
        def seller_tasks_group(seller_id):
            check_token = check_seller_token_task(seller_id, PricesApi.NAME)

            trigger = TriggerDagRunOperator(
                task_id="get_prices",
                task_display_name="🚀 Получение цен поставщика",
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

            check_token >> trigger

        create_tables >> seller_tasks_group(task_seller_id)


dag1_tasks()

# ----------------------------------------------------------------------------------------------------------------------

XCOM_TASK_ID = "extract"
XCOM_KEY = "next"


def getnextparams(**context):
    getnext = json.loads(context["context"]['ti'].xcom_pull(key='next', task_ids='extract', dag_id=DAG2_NAME))
    return {
        "seller_id": f"{getnext["seller_id"]}",
        "limit": getnext["limit"],
        "offset": getnext["offset"],
        "sync_time": getnext["sync_time"],
    }


@dag(
    DAG2_NAME,
    dag_display_name="WB Цены и скидки - Получение цен поставщика ️️ 🖐️",
    default_args={
        'retries': 4,
        'retry_delay': timedelta(minutes=1)
    },
    params={
        "seller_id": Param(0, type="string", enum=list(map(lambda id: str(id), get_sellers_ids()))),
        "limit": Param(200, type="integer"),
        "offset": Param(0, type="integer"),
        "sync_time": Param(int(datetime.now().timestamp()), type="integer"),
    },
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['wb', 'prices', 'sub'],
)
def dag2_tasks():
    @task
    def extract(**context) -> list:
        params = context['params']
        api_response = PricesApi(token=get_seller_token(int(params['seller_id']))).supplier_prices(
            offset=params['offset'],
            limit=params['limit']
        )
        validate_json_schema(api_response, {
            "type": "object",
            "properties": {
                "data": {
                    "properties": {
                        "listGoods": {
                            "type": "array",
                            "items": {"type": "object"},
                        }
                    }
                }
            }
        })

        prices = api_response["data"]["listGoods"] if "listGoods" in api_response["data"] else []

        context["ti"].xcom_push(key=XCOM_KEY, value=json.dumps({
            "get": len(prices) == int(params['limit']),
            "seller_id": params["seller_id"],
            "offset": (int(params['offset']) + int(params['limit'])),
            "limit": int(params["limit"]),
            "sync_time": int(params["sync_time"]),
        }))

        return prices

    @task()
    def transform(api_response: list, **context) -> tuple:
        params = context['params']
        data = []
        for item in api_response:
            for size in item["sizes"]:
                data.append([
                    int(params['seller_id']), params['sync_time'],
                    item["nmID"],
                    size["sizeID"],
                    item["vendorCode"],
                    "" if size["techSizeName"] == "0" else size["techSizeName"],
                    size["price"],
                    item["discount"],
                    size["discountedPrice"],
                    item["clubDiscount"],
                    size["clubDiscountedPrice"],
                    item["editableSizePrice"],
                ])
        return (
            [
                "seller_id", "sync_time", "nm_id", "chrt_id", "vendor_code", "tech_size",
                "price", "discount", "sale_price", "club_discount", "club_price",
                "size_price_editable",
            ],
            data
        )

    @task
    def load(transformed_data: tuple):
        fields, data = transformed_data
        if len(transformed_data) > 0:
            etl_hook_insert_data(
                CONN_ID, TABLE, fields=fields, transformed_data=data,
                replace=True,
                replace_index=['nm_id', 'chrt_id'],
            )

    @task.branch(task_display_name="Загрузить еще?")
    def check_stop(**context):
        get_next = json.loads(context['ti'].xcom_pull(key=XCOM_KEY, task_ids=XCOM_TASK_ID, dag_id=DAG2_NAME))
        return 'self_trigger' if get_next['get'] else 'stop_task'

    self_trigger = TriggerDagRunOperator(
        task_id="self_trigger",
        task_display_name="Загрузка следующей порции",
        trigger_dag_id=DAG2_NAME,
        conf=getnextparams,
        reset_dag_run=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        execution_date="{{ (macros.datetime.now() + macros.timedelta(seconds=2)).isoformat() }}",
    )

    @task(task_display_name="Загрузка завершена!")
    def stop_task(**context):
        params = context["params"]
        PostgresHook(postgres_conn_id=CONN_ID).run(
            f"DELETE FROM {TABLE} WHERE seller_id = {params["seller_id"]} AND sync_time <> {params["sync_time"]};"
        )

    load(
        transform(
            extract()
        )
    ) >> check_stop() >> [self_trigger, stop_task()]


dag2_tasks()
