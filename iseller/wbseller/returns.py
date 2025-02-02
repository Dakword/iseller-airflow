from .api import Api

BASE_URL = "https://returns-api.wildberries.ru"


class ReturnsApi(Api):
    NAME = "returns"

    def claims(self, offset: int = 0, limit: int = 200):
        return self.get(f'{BASE_URL}/api/v1/claims', {
            "is_archive": 'false',
            "offset": offset,
            "limit": limit
        })

    def archived_claims(self, offset: int = 0, limit: int = 200):
        return self.get(f'{BASE_URL}/api/v1/claims', {
            "is_archive": 'true',
            "offset": offset,
            "limit": limit
        })

    def nm_claims(self, nm_id: int, offset: int = 0, limit: int = 200):
        return self.get(f'{BASE_URL}/api/v1/claims', {
            "is_archive": 'false',
            "nm_id": nm_id,
            "offset": offset,
            "limit": limit
        })

    def archived_nm_claims(self, nm_id: int, offset: int = 0, limit: int = 200):
        return self.get(f'{BASE_URL}/api/v1/claims', {
            "is_archive": 'true',
            "nm_id": nm_id,
            "offset": offset,
            "limit": limit
        })

    def claim(self, id: str, offset: int = 0, limit: int = 200):
        return self.get(f'{BASE_URL}/api/v1/claims', {
            "is_archive": 'false',
            "id": id,
            "offset": offset,
            "limit": limit
        })

    def archived_claim(self, id: str, offset: int = 0, limit: int = 200):
        return self.get(f'{BASE_URL}/api/v1/claims', {
            "is_archive": 'true',
            "id": id,
            "offset": offset,
            "limit": limit
        })
