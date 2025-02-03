import logging
from time import sleep
from typing import Optional, Dict, Literal
import requests
from requests.exceptions import JSONDecodeError

from .api_token import ApiToken


class APIError(Exception):
    def __init__(self, status_code: int, error: str, detail: str = ""):
        self.status_code = status_code
        self.error = error
        self.detail = detail
        super().__init__(f"{status_code}: {error}. {detail}".strip())


class Api:
    NAME: str = "none"
    locale: str = "ru"
    retries: int = 5
    retry_delay: int = 12

    def __init__(self, token):
        self.token: ApiToken = ApiToken(token)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def api_name(self) -> str:
        return self.NAME

    def api_access(self) -> bool:
        return self.token.has_scope(self.NAME)

    def get(self, url: str, params: Optional[Dict] = None):
        return self.__request("GET", url, params)

    def post(self, url: str, params: Optional[Dict] = None, data: Optional[Dict] = None):
        return self.__request("POST", url, params, json=data)

    def __request(
            self,
            method: Literal["GET", "POST"],
            url: str,
            params: Optional[Dict] = None,
            json: Optional[Dict] = None
    ):
        params = params or {}
        json = json or {}

        for attempt in range(1, self.retries + 1):
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json
            )

            logging.info(f"Attempt {attempt}/{self.retries} | {method} {response.url}")

            if response.status_code == 429 and attempt < self.retries:
                delay = self.__get_retry_delay(response)
                logging.warning(f"429 Too Many Requests. Retrying in {delay}s")
                sleep(delay)
                continue

            if not response.ok:
                self.__handle_error(response)

            return response.json()

        raise APIError(429, "Max retries exceeded", "Too Many Requests")

    def __get_retry_delay(self, response: requests.Response) -> int:
        retry_after = response.headers.get("X-Ratelimit-Retry", 0)
        return int(retry_after) if retry_after else self.retry_delay

    def __handle_error(self, response: requests.Response) -> None:
        try:
            response_json = response.json()
        except JSONDecodeError:
            response_json = {"message": "Invalid JSON response", "detail": response.text}

        error_info = {
            "status_code": response.status_code,
            "error": self.__extract_error_message(response_json),
            "detail": response_json.get("detail", "")
        }

        logging.error(f"API Error: {error_info}")
        raise APIError(**error_info)

    def __extract_error_message(self, response_json: Dict) -> str:
        error_fields = [
            ("message", None),
            ("statusText", None)
            ("errors", lambda x: "; ".join(x)),
            ("error", "errorText"),
        ]
        for field, processor in error_fields:
            if field in response_json:
                value = response_json[field]
                if callable(processor):
                    return processor(value)
                elif value is not None and value in response_json:
                    return response_json[value]
                return value
        return "Unknown error"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
