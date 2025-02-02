from .api import Api

BASE_URL = "https://discounts-prices-api.wildberries.ru"


class PricesApi(Api):
    NAME = "prices"

    def supplier_prices(self, offset: int = 0, limit: int = 1000, nm_id: int = None):
        params = {
            "limit": limit,
            "offset": offset,
        }
        if nm_id: params["filterNmID"] = nm_id
        return self.get(f'{BASE_URL}/api/v2/list/goods/filter', params)

    def quarantine(self, offset: int = 0, limit: int = 1000):
        return self.get(f'{BASE_URL}/api/v2/quarantine/goods', {
            "limit": limit,
            "offset": offset,
        })
