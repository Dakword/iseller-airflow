import json
from base64 import b64decode
from datetime import datetime


class ApiToken():
    __api_scope: dict = {
        "common": 0,
        "tariffs": 0,
        "content": 1,
        "analytics": 2,
        "calendar": 3,
        "prices": 3,
        "marketplace": 4,
        "statistics": 5,
        "adv": 6,
        "feedbacks": 7,
        "questions": 7,
        "recommends": 8,
        "chat": 9,
        "supplies": 10,
        "returns": 11,
        "documents": 12,
    }
    __token: str
    __payload: dict

    def __init__(self, token: str):
        self.__token = token
        alg, payload, signature = token.split(".")
        payload += "=" * (-len(payload) % 4)
        self.__payload = json.loads(b64decode(payload).decode("utf-8"))

    def __str__(self):
        return self.__token

    def payload(self, item=None) -> dict:
        return self.__payload if item is None else self.__payload.get(item)

    def is_expired(self) -> bool:
        return datetime.now().timestamp() > self.payload("exp")

    def has_scope(self, api: str) -> bool:
        flag_position = self.__api_scope.get(api) if api in self.__api_scope else None
        if flag_position is None:
            return False
        return self.__is_flag_set(flag_position)

    def __is_flag_set(self, position: int) -> bool:
        return bool(self.payload("s") & (0b1 << position))
