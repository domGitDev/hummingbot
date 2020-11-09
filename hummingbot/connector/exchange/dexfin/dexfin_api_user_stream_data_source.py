#!/usr/bin/env python

import asyncio
import logging
import time
from typing import (
    AsyncIterable,
    Dict,
    Optional
)
import ujson
import websockets

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.connector.exchange.dexfin.dexfin_auth import DexfinAuth
from hummingbot.logger import HummingbotLogger

DEXFIN_API_ENDPOINT = "https://api.dexfin.com"
USER_STREAM_ENDPOINT = "wss://test.dexfin.dev/api/v2/ranger/public"

DEXFIN_PRIVATE_TOPICS = [
    "order",
    "trade",
]


class DexfinAPIUserStreamDataSource(UserStreamTrackerDataSource):

    PING_TIMEOUT = 50.0

    _kausds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._kausds_logger is None:
            cls._kausds_logger = logging.getLogger(__name__)
        return cls._kausds_logger

    def __init__(self, dexfin_auth: DexfinAuth):
        self._current_listen_key = None
        self._current_endpoint = None
        super().__init__()
        self._dexfin_auth: DexfinAuth = dexfin_auth
        self._last_recv_time: float = 0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    async def get_listen_key(self):
        pass

    async def _subscribe_topic(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            subscribe_request = {"event": "subscribe", "streams": DEXFIN_PRIVATE_TOPICS}
            await ws.send(ujson.dumps(subscribe_request))
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    async def get_ws_connection(self) -> websockets.WebSocketClientProtocol:
        ws_path: str = "&".join([f"stream={priv_topic}" for priv_topic in DEXFIN_PRIVATE_TOPICS])
        stream_url: str = f"{USER_STREAM_ENDPOINT}?{ws_path}"
        headers = self._dexfin_auth.add_auth_to_params("get", "")
        logging.info(f"Connecting to PRIVATE {stream_url}")

        # Create the WS connection.
        return websockets.connect(stream_url, ping_interval=40, ping_timeout=self.PING_TIMEOUT, extra_headers=headers)

    async def _inner_messages(self, ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            while True:
                msg: str = await ws.recv()
                self._last_recv_time = time.time()
                yield msg
        finally:
            await ws.close()
            self._current_listen_key = None

    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        ws = None
        while True:
            try:
                async with (await self.get_ws_connection()) as ws:
                    # await self._subscribe_topic(DEXFIN_PRIVATE_TOPICS)
                    async for msg in self._inner_messages(ws):
                        decoded: Dict[str, any] = ujson.loads(msg)
                        output.put_nowait(decoded)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error while maintaining the user event listen key. Retrying after "
                                    "5 seconds...", exc_info=False)
                self._current_listen_key = None
                if ws:
                    await ws.close()
                await asyncio.sleep(5)
