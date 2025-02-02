from airflow.models import MappedOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


def tf_create_table(conn_id: str, params: dict) -> MappedOperator:
    def mapped(pair):
        return {
            "sql": pair[1],
            "params": {
                "table": pair[0],
            }
        }

    if len(params) == 1:
        param = mapped(params.popitem())
        return SQLExecuteQueryOperator(
            task_id="create_table_if_not_exists",
            task_display_name="Проверка таблицы БД",
            conn_id=conn_id,
            **param
        )
    else:
        return SQLExecuteQueryOperator.partial(
            task_id="create_table_if_not_exists",
            task_display_name="Проверка таблиц БД",
            conn_id=conn_id,
        ).expand_kwargs(list(map(mapped, params.items())))
