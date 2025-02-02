import jsonschema
from airflow.providers.postgres.hooks.postgres import PostgresHook
from jinja2 import Template


def etl_hook_insert_data(conn_id: str, table: str, fields: list = [], transformed_data: list = [],
                         sql_before_insert: str | None = None, sql_after_insert: str | None = None,
                         **kwargs
                         ):
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)

    if sql_before_insert is not None:
        postgres_hook.run(sql_before_insert)

    if len(transformed_data) > 0:
        chunk_size = 1000
        for i in range(0, len(transformed_data), chunk_size):
            postgres_hook.insert_rows(
                table=table,
                target_fields=fields,
                rows=transformed_data[i:i + chunk_size],
                **kwargs
            )

    if sql_after_insert is not None:
        postgres_hook.run(sql_after_insert)


def etl_transform_api_response(api_response: list, schema: dict, addons: dict = {}) -> tuple:
    return (
        # fields list
        list(schema.keys()) + list(addons.keys()),
        # data list
        list(map(
            lambda values: values + list(addons.values()),
            [[col(row) if callable(col) else Template(col).render(api=row) for col in schema.values()] for row in
             api_response]
        ))
    )


def validate_json_schema(api_response, schema: dict):
    jsonschema.validate(api_response, schema)
