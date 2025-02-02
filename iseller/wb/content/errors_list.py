from airflow.decorators import dag, task_group
from datetime import timedelta

from airflow.utils.dates import days_ago

from iseller.wb import Config
from iseller.wb.content.tasks.seller_errors import SellerErrorsTaskGroup
from iseller.wb.sellers import get_sellers_ids, get_seller_token
from iseller.wbseller import ContentApi
from iseller.wb.wb_tasks_factory import check_seller_token_task
from iseller.utils.tasks_factory import tf_create_table

DAG1_NAME = "wb_content_errors"
DAG2_NAME = "wb_content_errors_seller"
CONN_ID = Config.CONN_ID
TABLE = "cards_errors"
SQL_CREATE_TABLE = "./sql/cards_errors.sql"


@dag(
    DAG1_NAME,
    dag_display_name="WB Контент - Карточки с ошибками",
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=4),
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False,
    max_active_runs=1,
    tags=['wb', 'content', 'errors'],
)
def tasks():
    create_table_if_not_exists = tf_create_table(CONN_ID, {TABLE: SQL_CREATE_TABLE})

    for task_seller_id in get_sellers_ids():
        @task_group(group_id=f"seller_{task_seller_id}")
        def seller_tasks(seller_id):
            check_token = check_seller_token_task(seller_id, ContentApi.NAME)

            tasks_group = SellerErrorsTaskGroup(
                group_id=f"etl_{seller_id}",
                conn_id=CONN_ID,
                table=TABLE,
                seller_id=seller_id,
            )

            check_token >> tasks_group

        create_table_if_not_exists >> seller_tasks(task_seller_id)


tasks()
