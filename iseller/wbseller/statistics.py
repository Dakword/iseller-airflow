from .api import Api

BASE_URL = "https://statistics-api.wildberries.ru"


class StatisticsApi(Api):
    NAME = "statistics"

    def supplier_incomes(self, date_from):
        return self.get(f'{BASE_URL}/api/v1/supplier/incomes', {
            "dateFrom": date_from
        })

    def supplier_stocks(self, date_from, flag=0):
        return self.get(f'{BASE_URL}/api/v1/supplier/stocks', {
            "dateFrom": date_from,
            "flag": flag
        })

    def supplier_orders(self, date_from, flag=0):
        return self.get(f'{BASE_URL}/api/v1/supplier/orders', {
            "dateFrom": date_from,
            "flag": flag
        })

    def supplier_sales(self, date_from, flag=0):
        return self.get(f'{BASE_URL}/api/v1/supplier/sales', {
            "dateFrom": date_from,
            "flag": flag
        })
