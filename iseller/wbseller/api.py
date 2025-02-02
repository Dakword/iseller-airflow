import logging
from time import sleep

import requests

from .api_token import ApiToken


class Api:
    NAME: str = "none"
    locale: str = "ru"
    retries: int = 3
    retry_delay: int = 10

    def __init__(self, token):
        self.token: ApiToken = ApiToken(token)
        self.__headers: dict = {
            "Authorization": token,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def api_name(self) -> str:
        return self.NAME

    def api_access(self) -> bool:
        return self.token.has_scope(self.NAME)

    def get(self, url: str, params: dict = {}):
        return self.__request(url, params)

    def post(self, url: str, params: dict = {}, data: dict = {}):
        return self.__request(url, params, data, "POST")

    def __request(self, url: str, params: dict = {}, data: dict = {}, method: str = "GET"):
        if method not in ("GET", "POST"):
            raise Exception("Unknown method")

        for retry in range(self.retries):
            if method == "GET":
                response = requests.get(url, params=params, headers=self.__headers)
            elif method == "POST":
                response = requests.post(url, params=params, json=data, headers=self.__headers)

            logging.info(f"URL: {response.url}")
            logging.info(f"Attempt: {retry + 1}/{self.retries}")

            if response.status_code == 429 and retry < self.retries:
                logging.info("429: Too Many Requests")
                logging.info(f"Repeat after {self.retry_delay} seconds.")
                sleep(self.retry_delay)
            else:
                break

        self.__check_response_status(response)
        return response.json()

    def __check_response_status(self, response: requests.Response):
        if response.status_code == 200:
            logging.info("200: OK")
        else:
            try:
                response_json = response.json()
            except Exception:
                response_json = {
                    "statusText": "Error decoding the json response",
                    "detail": response.content
                }

            error = "Wildberries API Error"
            if response.status_code == 400 and "message" in response_json: error = response_json["message"]
            if "error" in response_json:      error = response_json["error"]["errorText"]
            if "errors" in response_json:     error = "; ".join(response_json["errors"])
            if "statusText" in response_json: error = response_json["statusText"]

            detail = ""
            if "detail" in response_json:     detail = response_json["detail"]

            raise Exception(f"{response.status_code}: {error}. {detail}")
