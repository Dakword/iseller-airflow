import json

from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from iseller.utils.etl_helpers import validate_json_schema, etl_transform_api_response, etl_hook_insert_data
from iseller.wb.sellers import get_seller_token
from iseller.wbseller.content import ContentApi


class SellerErrorsTaskGroup(TaskGroup):

    def __init__(self, group_id: str, conn_id: str, seller_id: int, table: str, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        @task(task_group=self)
        def extract() -> list:
            api_response = ContentApi(get_seller_token(seller_id)).errors_list()
            validate_json_schema(api_response, {
                "type": "object",
                "properties": {
                    "data": {
                        "type": "array",
                        "items": {"type": "object"},
                    },
                    "error": {"type": "boolean", "enum": [False]},
                }
            })
            return api_response.get("data")

        @task(task_group=self)
        def transform(api_response: list) -> tuple:
            return etl_transform_api_response(
                api_response, {
                    "vendor_code": "{{ api.get('vendorCode') }}",
                    "object_id": "{{ api.get('objectID') }}",
                    "object_name": "{{ api.get('object') }}",
                    "updated_at": lambda api: api.get("updateAt").replace("T", " "),
                    "errors": lambda api: json.dumps(api["errors"], ensure_ascii=False),
                }, {
                    "seller_id": seller_id,
                }
            )

        @task(task_group=self)
        def load(data: tuple):
            fields, transformed_data = data

            if len(transformed_data) > 0:
                etl_hook_insert_data(
                    table=table, conn_id=conn_id,
                    fields=fields, transformed_data=transformed_data,
                    sql_before_insert=f"DELETE FROM {table} WHERE seller_id={seller_id};"
                )

        load(transform(extract()))
