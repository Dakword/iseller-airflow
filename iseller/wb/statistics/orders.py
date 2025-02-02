from datetime import timedelta

from airflow.decorators import dag, task_group
from airflow.utils.dates import days_ago

from iseller.utils.tasks_factory import tf_create_table
from iseller.wb.sellers import get_sellers_ids
from iseller.wb.statistics.tasks.seller_orders import SellerOrdersTaskGroup
from iseller.wb.wb_tasks_factory import check_seller_token_task
from iseller.wb import Config
from iseller.wbseller import ReturnsApi, StatisticsApi

DAG_NAME = "wb_statistics_orders"
CONN_ID = Config.CONN_ID
TABLE = "orders"
TABLE_CREATE_SQL = "./sql/orders.sql"


@dag(
    DAG_NAME,
    dag_display_name="WB Статистика - Отчет \"Заказы\"",
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=30),
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False,
    tags=['wb', 'statistic', 'orders'],
)
def dag_tasks():
    create_table = tf_create_table(CONN_ID, {TABLE: TABLE_CREATE_SQL})

    for task_seller_id in get_sellers_ids():
        @task_group(group_id=f"seller_{task_seller_id}")
        def seller_tasks(seller_id):
            check_token = check_seller_token_task(seller_id, StatisticsApi.NAME)

            seller_etl = SellerOrdersTaskGroup(
                group_id="ETL",
                conn_id=CONN_ID,
                table=TABLE,
                seller_id=seller_id,
            )

            check_token >> seller_etl

        create_table >> seller_tasks(task_seller_id)


dag_tasks()
