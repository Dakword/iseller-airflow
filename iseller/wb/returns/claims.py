import json
from datetime import timedelta

from airflow.decorators import task, task_group, dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param
from airflow.utils.dates import days_ago

from iseller.utils.etl_helpers import validate_json_schema, etl_hook_insert_data, etl_transform_api_response
from iseller.wb import Config
from iseller.wb.wb_tasks_factory import check_seller_token_task
from iseller.utils.tasks_factory import tf_create_table
from iseller.wb.sellers import get_sellers_ids, get_seller_token
from iseller.wbseller import ReturnsApi

DAG1_NAME = "wb_returns_claim"
DAG2_NAME = "wb_returns_claim_seller"
CONN_ID = Config.CONN_ID
TABLE = "returns"
TABLE_CREATE_SQL = "./sql/returns.sql"
STAGE = {
    "ARCHIVED": "archived",
    "NEW": "new",
}


@dag(
    DAG1_NAME,
    dag_display_name="WB Возвраты - Заявки на возврат 🚀",
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=80),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10)
    },
    catchup=False,
    max_active_runs=1,
    tags=["wb", "returns", "claim"],
)
def dag1_tasks():
    create_table = tf_create_table(CONN_ID, {TABLE: TABLE_CREATE_SQL})

    for seller_id in get_sellers_ids():
        @task_group(group_id=f"seller_{seller_id}")
        def seller_tasks_group():
            check_token = check_seller_token_task(seller_id, ReturnsApi.NAME)

            tasks = map(lambda stage: {
                "task_id": stage,
                "conf": {
                    "download": STAGE[stage],
                    "seller_id": f"{seller_id}",
                    "limit": 200,
                    "offset": 0,
                },
            }, STAGE.keys())

            download_archived, download_new = map(lambda task: TriggerDagRunOperator(
                task_id=task["task_id"],
                conf=task["conf"],
                trigger_dag_id=DAG2_NAME,
                wait_for_completion=True,
                trigger_rule=TriggerRule.NONE_FAILED,
            ), tasks)

            check_token >> download_archived >> download_new

        create_table >> seller_tasks_group()


dag1_tasks()

# ----------------------------------------------------------------------------------------------------------------------

XCOM_TASK_ID = "extract_api_data"
XCOM_KEY = "next"


def trigger_params(**context):
    get_next = json.loads(context["context"]["ti"].xcom_pull(key=XCOM_KEY, task_ids=XCOM_TASK_ID, dag_id=DAG2_NAME))
    return {
        "download": get_next["download"],
        "seller_id": f"{get_next["seller_id"]}",
        "limit": get_next["limit"],
        "offset": int(get_next["offset"]) + int(get_next["limit"]),
    }


@dag(
    DAG2_NAME,
    dag_display_name="WB Возвраты - Заявки на возврат поставщика 🖐️",
    default_args={
        "retries": 4,
        "retry_delay": timedelta(minutes=1)
    },
    params={
        "download": Param(STAGE.get("ARCHIVED"), enum=[STAGE.get("ARCHIVED"), STAGE.get("NEW")]),
        "seller_id": Param(0, type="string", enum=list(map(lambda id: str(id), get_sellers_ids()))),
        "limit": Param(200, type="integer"),
        "offset": Param(0, type="integer"),
    },
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["wb", "returns", "claim", "sub"],
)
def dag2_tasks():
    @task
    def extract_api_data(**context) -> list:
        params = context["params"]
        api_response = {"claims": []}

        if params["download"] == STAGE.get("ARCHIVED"):
            api_response = ReturnsApi(token=get_seller_token(int(params["seller_id"]))).archived_claims(
                offset=params["offset"],
                limit=params["limit"]
            )
        elif params["download"] == STAGE.get("NEW"):
            api_response = ReturnsApi(token=get_seller_token(int(params["seller_id"]))).claims(
                offset=params["offset"],
                limit=params["limit"]
            )

        validate_json_schema(api_response, {
            "type": "object",
            "properties": {
                "claims": {
                    "type": "array",
                    "items": {"type": "object"},
                },
            }
        })

        claims = api_response.get("claims", [])

        if len(claims) == int(params["limit"]):
            context["ti"].xcom_push(key=XCOM_KEY, value=json.dumps({
                "get": True,
                "download": params["download"],
                "seller_id": int(params["seller_id"]),
                "limit": int(params["limit"]),
                "offset": int(params["offset"]),
            }))
        else:
            context["ti"].xcom_push(key=XCOM_KEY, value=json.dumps({
                "get": False,
            }))

        return claims

    @task
    def transform_extracted(api_response: list, **context) -> tuple:
        seller_id = context["params"]["seller_id"]
        return etl_transform_api_response(
            api_response, {
                "id": lambda api: api.get("id"),
                "nm_id": lambda api: api.get("nm_id"),
                "date": lambda api: api.get("dt").replace("T", " "),
                "order_date": lambda api: api.get("order_dt").replace("T", " "),
                "update_date": lambda api: api.get("dt_update").replace("T", " "),
                "source": lambda api: api.get("claim_type"),
                "status": lambda api: api.get("status"),
                "product_status": lambda api: api.get("status_ex"),
                "product_name": lambda api: api.get("imt_name"),
                "user_comment": lambda api: api.get("user_comment"),
                "photos": lambda api: json.dumps(api["photos"]) if api.get("photos") else None,
                "videos": lambda api: json.dumps(api["video_paths"]) if api.get("video_paths") else None,
                "user_price": lambda api: api.get("price"),
                "actions": lambda api: json.dumps(api.get("actions")),
                "srid": lambda api: api.get("srid"),
                "wb_comment": lambda api: api.get("wb_comment"),
            }, {
                "seller_id": seller_id
            })

    @task
    def load_data(transformed: tuple):
        fields, transformed_data = transformed
        if len(transformed_data) > 0:
            etl_hook_insert_data(
                CONN_ID, TABLE,
                fields=fields, transformed_data=transformed_data,
                replace=True,
                replace_index="id",
            )

    @task.branch(task_display_name="Запросить еще?")
    def check_stop(**context):
        get_next = json.loads(context["ti"].xcom_pull(key=XCOM_KEY, task_ids=XCOM_TASK_ID, dag_id=DAG2_NAME))
        return "self_trigger" if get_next["get"] else "stop_task"

    @task(task_display_name="Загрузка завершена!")
    def stop_task():
        pass

    self_trigger = TriggerDagRunOperator(
        task_id="self_trigger",
        task_display_name="Загрузка следующей порции",
        trigger_dag_id=DAG2_NAME,
        conf=trigger_params,
        reset_dag_run=True,
        trigger_rule=TriggerRule.NONE_FAILED,
        execution_date="{{ (macros.datetime.now() + macros.timedelta(seconds=3)).isoformat() }}",
    )

    etl = load_data(transform_extracted(extract_api_data()))

    etl >> check_stop() >> [self_trigger, stop_task()]


dag2_tasks()
