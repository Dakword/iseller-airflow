from .api import Api

BASE_URL = "https://seller-analytics-api.wildberries.ru"


class AnalyticsApi(Api):
    NAME = "analytics"

    def blocked_cards(self):
        return self.get(f'{BASE_URL}/api/v1/analytics/banned-products/blocked', {
            "sort": "nmId",
            "order": "asc",
        })

    def shadowed_cards(self):
        return self.get(f'{BASE_URL}/api/v1/analytics/banned-products/shadowed', {
            "sort": "nmId",
            "order": "asc",
        })
