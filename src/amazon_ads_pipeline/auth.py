"""Login with Amazon (LWA) access tokens, minted from the client's long-lived refresh token."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Protocol

import httpx


class AuthError(RuntimeError):
    """LWA refused to issue an access token."""

    def __init__(self, status_code: int, error: str, description: str) -> None:
        super().__init__(f"Login with Amazon returned {status_code} {error}: {description}")
        self.status_code = status_code
        self.error = error

    @property
    def transient(self) -> bool:
        return self.status_code == 429 or self.status_code >= 500


class TokenProvider(Protocol):
    def token(self) -> str: ...

    def invalidate(self) -> None: ...


class LwaTokenProvider:
    """Exchanges the refresh token for access tokens and caches each until shortly before expiry.

    Access tokens live about an hour (`expires_in`). A token is renewed `refresh_margin`
    seconds early, or immediately after the API answers 401.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        token_url: str,
        http: httpx.Client,
        clock: Callable[[], float] = time.monotonic,
        refresh_margin: float = 60.0,
    ) -> None:
        self._form = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
        self._token_url = token_url
        self._http = http
        self._clock = clock
        self._margin = refresh_margin
        self._token: str | None = None
        self._expires_at = 0.0

    def token(self) -> str:
        if self._token is None or self._clock() >= self._expires_at - self._margin:
            self._refresh()
        assert self._token is not None
        return self._token

    def invalidate(self) -> None:
        self._token = None

    def _refresh(self) -> None:
        response = self._http.post(self._token_url, data=self._form)
        if response.status_code != 200:
            try:
                payload = response.json()
            except ValueError:
                payload = {}
            raise AuthError(
                response.status_code,
                payload.get("error", "unknown_error"),
                payload.get("error_description", response.text[:200]),
            )
        payload = response.json()
        self._token = payload["access_token"]
        self._expires_at = self._clock() + float(payload.get("expires_in", 3600))
