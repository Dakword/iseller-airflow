from .api import Api

BASE_URL = "https://supplies-api.wildberries.ru"


class SuppliesApi(Api):
    NAME = "supplies"

    def warehouses(self):
        return self.get(f'{BASE_URL}/api/v1/warehouses')
