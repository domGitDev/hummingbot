from datetime import datetime
import hashlib
import hmac
from typing import (
    Any,
    Dict
)
from collections import OrderedDict


class DexfinAuth:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key: str = api_key
        self.secret_key: str = secret_key
        self.partner_id: str = "Hummingbot"
        self.partner_key: str = "8fb50686-81a8-408a-901c-07c5ac5bd758"

    @staticmethod
    def keysort(dictionary: Dict[str, str]) -> Dict[str, str]:
        return OrderedDict(sorted(dictionary.items(), key=lambda t: t[0]))

    def add_auth_to_params(self,
                           method: str,
                           path_url: str,
                           args: Dict[str, Any] = None,
                           partner_header: bool = False) -> Dict[str, Any]:

        now = datetime.utcnow()
        timestamp = int(datetime.timestamp(now) * 1e3)
        request = {
            "X-Auth-Apikey": self.api_key,
            "X-Auth-Nonce": str(timestamp),
            "Content-Type": "application/json"
        }

        signature = hmac.new(
            self.secret_key.encode("utf-8"),
            f"{timestamp}{self.api_key}".encode("utf-8"),
            hashlib.sha256).hexdigest()

        request["X-Auth-Signature"] = signature
        return request
