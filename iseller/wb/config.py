import json
from os import getenv

from airflow import settings
from airflow.models import Connection, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONNECTION_ID = "iseller_wildberries"
VAR_SELLERS = "iseller_wildberries_sellers"


class Config():
    CONN_ID = CONNECTION_ID
    SELLERS = Variable.get(VAR_SELLERS, [], deserialize_json=True)


def _init_vars():
    if Variable.get(VAR_SELLERS, False, deserialize_json=True) is False:
        print(f"Create variable {VAR_SELLERS}")
        Variable.set(VAR_SELLERS, json.loads(getenv("ISELLER_WB_JSON_SELLERS", '''[{
            "id": 123456,
            "name": "Shop name",
            "token": "eyJhbGciOiJFUzI1NiIsImt..."
        }]''')), serialize_json=True)
        print(f"Variable \"{VAR_SELLERS}\" created.")
    else:
        print(f"Variable \"{VAR_SELLERS}\" exist.")

def _init_connections():
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == CONNECTION_ID).first()
    if conn_name is None:
        conn = Connection(
            conn_id=CONNECTION_ID,
            conn_type="postgres",
            host=getenv("ISELLER_WB_DB_HOST", "host.docker.internal"),
            login=getenv("ISELLER_WB_DB_USER", "root"),
            password=getenv("ISELLER_WB_DB_PASSWORD", "password"),
            port=getenv("ISELLER_WB_DB_PORT", 5432),
            schema=getenv("ISELLER_WB_DB", "wildberries_api")
        )
        session.add(conn)
        session.commit()
        print(f"Connection \"{CONNECTION_ID}\" created.")
        schema = conn.schema
    else:
        print(f"Connection \"{CONNECTION_ID}\" exist.")
        schema = conn_name.schema

    print(f"Check schema \"{schema}\" exist.")
    PostgresHook(postgres_conn_id=CONNECTION_ID).run(sql=f"CREATE SCHEMA IF NOT EXISTS {schema}")


if __name__ == "__main__":
    _init_vars()
    _init_connections()
