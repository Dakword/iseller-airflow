from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task

from iseller.utils.etl_helpers import validate_json_schema, etl_transform_api_response, etl_hook_insert_data
from iseller.wb.sellers import get_seller_token
from iseller.wbseller import StatisticsApi


class SellerOrdersTaskGroup(TaskGroup):

    def __init__(self, group_id: str, conn_id: str, seller_id: int, table: str, **kwargs):
        super().__init__(group_id=group_id, **kwargs)

        @task(task_group=self)
        def get_lastchange_date() -> tuple | None:
            return PostgresHook(postgres_conn_id=conn_id).get_first(
                f"SELECT last_change_date FROM {table} WHERE seller_id = {seller_id} ORDER BY last_change_date DESC LIMIT 1;"
            )

        @task(task_group=self)
        def extract(last_change_date: tuple | None) -> list:
            date_from = last_change_date[0].strftime('%Y-%m-%dT%H:%M:%S') if last_change_date else '2020-01-01T00:00:00'

            api_response = StatisticsApi(get_seller_token(seller_id)).supplier_orders(date_from)

            validate_json_schema(api_response, {
                "type": "array",
                "items": {"type": "object"},
            })
            return api_response

        @task(task_group=self)
        def transform(api_response: list) -> tuple:
            return etl_transform_api_response(
                api_response, {
                    "srid": "{{ api.get('srid') }}",
                    "g_number": lambda api: api.get("gNumber"),
                    "nm_id": lambda api: api.get("nmId"),
                    "order_type": "{{ api.get('orderType') }}",
                    "date": lambda api: api.get("date").replace("T", " "),
                    "last_change_date": lambda api: api.get("lastChangeDate").replace("T", " "),
                    "warehouse_name": "{{ api.get('warehouseName') }}",
                    "warehouse_type": "{{ api.get('warehouseType') }}",
                    "vendor_code": "{{ api.get('supplierArticle') }}",
                    "barcode": "{{ api.get('barcode') }}",
                    "tech_size": lambda api: api["techSize"] if api.get("techSize") != "0" else None,
                    "category": "{{ api.get('category') }}",
                    "subject": "{{ api.get('subject') }}",
                    "brand": "{{ api.get('brand') }}",
                    "income_id": lambda api: api["incomeID"] if api.get("incomeID") else None,
                    "is_supply": lambda api: api.get("isSupply", False),
                    "is_realization": lambda api: api.get("isRealization", False),
                    "is_cancel": lambda api: api.get("isCancel", False),
                    "cancel_date": lambda api: api["cancelDate"][:10] if api.get("isCancel", False) else None,
                    "sticker": lambda api: api["sticker"] if api.get("sticker") else None,
                    "country": "{{ api.get('countryName') }}",
                    "okrug": lambda api: api["oblastOkrugName"] if api.get("oblastOkrugName") else None,
                    "region": "{{ api.get('regionName') }}",
                    "price": lambda api: api.get('totalPrice'),
                    "discount": lambda api: api.get("discountPercent"),
                    "sale_price": lambda api: api.get("priceWithDisc"),
                    "spp": lambda api: api.get("spp"),
                    "retail_price": lambda api: api.get("finishedPrice"),
                }, {
                    "seller_id": seller_id,
                }
            )

        @task(task_group=self)
        def load(transformed_data: tuple):
            fields, data = transformed_data

            etl_hook_insert_data(
                conn_id, table, fields, data,
                replace=True,
                replace_index='g_number',
            )

        #
        load(
            transform(
                extract(
                    get_lastchange_date()
                )
            )
        )
