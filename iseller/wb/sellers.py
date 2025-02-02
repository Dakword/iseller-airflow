import random

from iseller.wbseller import ApiToken

from .config import Config


def get_sellers_ids() -> list:
    return list(map(lambda seller: seller.get("id"), Config.SELLERS))


def get_tokens() -> list:
    return list(map(lambda seller: seller.get("token"), Config.SELLERS))


def get_seller_token(seller_id: int) -> str | None:
    for seller in Config.SELLERS:
        if seller.get("id") == seller_id:
            return seller.get("token")
    return None


def get_random_seller_id(scope: str | None = None) -> int | None:
    return random.choice(
        [int(seller.get("id")) for seller in Config.SELLERS if validate_seller_token(seller.get("id"), scope)]
    )


def get_random_token(scope: str | None = None) -> str | None:
    tokens = get_tokens()
    if scope is None:
        return random.choice(
            [token for token in tokens if not ApiToken(token).is_expired()]
        )
    else:
        return random.choice(
            [token for token in tokens if ApiToken(token).has_scope(scope) and not ApiToken(token).is_expired()]
        )


def validate_seller_token(seller_id: int | None, scope: str | None = None) -> bool:
    if seller_id is None: return False
    token = ApiToken(get_seller_token(seller_id))
    if scope is None:
        return not token.is_expired()
    else:
        return token.has_scope(scope) and not token.is_expired()


def get_seller_id_by_token(token: str) -> int | None:
    for seller in Config.SELLERS:
        if seller.get("token") == token:
            return int(seller.get("id"))
    return None
