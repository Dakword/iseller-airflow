from datetime import timedelta

from airflow.decorators import task, dag, task_group
from airflow.utils.dates import days_ago

from iseller.utils.etl_helpers import etl_hook_insert_data, validate_json_schema, etl_transform_api_response
from iseller.utils.tasks_factory import tf_create_table
from iseller.wb import Config
from iseller.wb.sellers import get_seller_token, get_sellers_ids
from iseller.wb.wb_tasks_factory import check_seller_token_task
from iseller.wbseller import AnalyticsApi

DAG_NAME = "wb_analytics_cards_shadowed"
CONN_ID = Config.CONN_ID
TABLE = "cards_shadowed"
SQL_CREATE_TABLE = "./sql/cards_shadowed.sql"


@dag(DAG_NAME,
     dag_display_name="WB Аналитика - Скрытые карточки",
     start_date=days_ago(1),
     schedule_interval=timedelta(minutes=140),
     default_args={
         'retries': 2,
         'retry_delay': timedelta(minutes=15)
     },
     catchup=False,
     max_active_runs=1,
     tags=['wb', 'analytics', 'cards', 'shadowed'],
     )
def dag_tasks():
    create_table_if_not_exists = tf_create_table(CONN_ID, {TABLE: SQL_CREATE_TABLE})

    for task_seller_id in get_sellers_ids():
        @task_group(group_id=f"seller_{task_seller_id}")
        def seller_tasks():
            check_token = check_seller_token_task(task_seller_id, AnalyticsApi.NAME)

            @task_group(group_id=f"ETL")
            def etl_tasks(seller_id):
                @task
                def extract_api_data() -> list:
                    api_response = AnalyticsApi(get_seller_token(seller_id)).shadowed_cards()
                    validate_json_schema(api_response, {
                        "type": "object",
                        "properties": {
                            "report": {
                                "type": "array",
                                "items": {"type": "object"},
                            },
                        }
                    })
                    return api_response.get("report", [])

                @task
                def transform_extracted(api_response: list) -> tuple:
                    return etl_transform_api_response(
                        api_response, {
                            "nm_id": lambda api: api.get("nmId"),
                            "vendor_code": '{{ api.get("vendorCode") }}',
                            "brand": '{{ api.get("brand") }}',
                            "title": '{{ api.get("title") }}',
                            "rating": lambda api: api.get("nmRating"),
                        }, {
                            "seller_id": seller_id,
                        })

                @task
                def load_data(transformed: tuple):
                    fields, transformed_data = transformed
                    etl_hook_insert_data(
                        conn_id=CONN_ID, table=TABLE,
                        fields=fields, transformed_data=transformed_data,
                        sql_before_insert=f"DELETE FROM {TABLE} WHERE seller_id={seller_id};"
                    )

                load_data(transform_extracted(extract_api_data()))

            check_token >> etl_tasks(task_seller_id)

        create_table_if_not_exists >> seller_tasks()


dag_tasks()
