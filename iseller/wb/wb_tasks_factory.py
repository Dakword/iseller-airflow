from airflow.operators.python import ShortCircuitOperator

from .sellers import validate_seller_token


def check_seller_token_task(seller_id: int | None, scope: str) -> ShortCircuitOperator:
    return ShortCircuitOperator(
        task_id="check_token",
        task_display_name="Проверка токена",
        python_callable=validate_seller_token,
        op_args=[seller_id, scope],
    )
