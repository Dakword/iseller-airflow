from .api import Api

BASE_URL = "https://content-api.wildberries.ru"


class ContentApi(Api):
    NAME = "content"

    def categories(self):
        return self.get(f'{BASE_URL}/content/v2/object/parent/all', {
            "locale": self.locale
        })

    def subjects(self, offset: int = 0, limit: int = 1000):
        return self.get(f'{BASE_URL}/content/v2/object/all', {
            "locale": self.locale,
            "offset": offset,
            "limit": limit,
        })

    def subject_characteristics(self, subject_id: int):
        return self.get(f'{BASE_URL}/content/v2/object/charcs/{subject_id}', {
            "locale": self.locale,
        })

    def errors_list(self):
        return self.get(f'{BASE_URL}/content/v2/cards/error/list', {
            "locale": self.locale
        })

    def trash_list(self, cursor_nm_id: int | None = None, cursor_trashed_at: str | None = None,
                   search: str | None = None, limit: int = 100):
        cursor = {
            "limit": limit
        }
        if cursor_nm_id is not None: cursor["nmID"] = cursor_nm_id
        if cursor_trashed_at is not None: cursor["trashedAt"] = cursor_trashed_at
        settings = {
            "cursor": cursor,
        }
        if search is not None:
            settings["filter"] = {
                "textSearch": search
            }
        return self.post(f'{BASE_URL}/content/v2/get/cards/trash', {
            "locale": self.locale
        }, {
            "settings": settings
        })

    def cards_list(self, cursor_nm_id: int | None = None, cursor_updated_at: str | None = None, limit: int = 100):
        cursor = {
            "limit": limit
        }
        if cursor_nm_id is not None: cursor["nmID"] = cursor_nm_id
        if cursor_updated_at is not None: cursor["updatedAt"] = cursor_updated_at

        settings = {
            "sort": { "ascending": True},
            "cursor": cursor,
            "filter": {
                "withPhoto": -1
            }
        }
        return self.post(f'{BASE_URL}/content/v2/get/cards/list', {
            "locale": self.locale
        }, {
            "settings": settings
        })
