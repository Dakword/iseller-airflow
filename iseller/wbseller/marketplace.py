from .api import Api

BASE_URL = "https://marketplace-api.wildberries.ru"


class MarketplaceApi(Api):
    NAME = "marketplace"

    def offices(self):
        return self.get(f'{BASE_URL}/api/v3/offices')

    def warehouses_list(self):
        return self.get(f'{BASE_URL}/api/v3/warehouses')

    def new_orders(self):
        return self.get(f'{BASE_URL}/api/v3/orders/new')

    def orders_statuses(self, orders: list):
        return self.post(f'{BASE_URL}/api/v3/orders/status', data={
            "orders": orders
        })
