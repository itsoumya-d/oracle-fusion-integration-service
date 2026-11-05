from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx


class OracleError(Exception):
    pass


@dataclass
class OracleConfig:
    base_url: str
    token_url: str
    client_id: str
    client_secret: str
    scope: str = "urn:opc:resource:consumer::all"


class OracleFusionClient:
    def __init__(self, config: OracleConfig, transport: httpx.BaseTransport | None = None, sleep=time.sleep) -> None:
        self.config = config
        self.sleep = sleep
        self._client = httpx.Client(base_url=config.base_url, transport=transport, timeout=20)
        self._token = ""
        self._token_expires_at = 0.0

    def close(self) -> None:
        self._client.close()

    def _token_valid(self) -> bool:
        return bool(self._token) and time.monotonic() < self._token_expires_at

    def authenticate(self) -> str:
        if self._token_valid():
            return self._token
        response = self._client.post(
            self.config.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "scope": self.config.scope,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if response.status_code >= 400:
            raise OracleError(f"token request failed: {response.status_code}")
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise OracleError("token response missing access_token")
        expires_in = int(payload.get("expires_in", 3600))
        self._token = token
        self._token_expires_at = time.monotonic() + max(expires_in - 60, 30)
        return token

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None, idempotency_key: str = "") -> dict[str, Any]:
        token = self.authenticate()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key
        response = self._client.request(method, path, json=payload, headers=headers)
        if response.status_code in {429, 500, 502, 503, 504}:
            raise OracleError(f"transient Oracle error: {response.status_code}")
        if response.status_code >= 400:
            raise OracleError(f"Oracle rejected request: {response.status_code} {response.text[:300]}")
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise OracleError("Oracle returned non-JSON response") from exc

    def create_supplier(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return self.request("POST", "/fscmRestApi/resources/latest/suppliers", payload, idempotency_key)

    def create_purchase_order(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return self.request("POST", "/fscmRestApi/resources/latest/purchaseOrders", payload, idempotency_key)

    def create_invoice(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return self.request("POST", "/fscmRestApi/resources/latest/invoices", payload, idempotency_key)

    def create_receipt(self, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        return self.request("POST", "/fscmRestApi/resources/latest/receivingReceipts", payload, idempotency_key)
