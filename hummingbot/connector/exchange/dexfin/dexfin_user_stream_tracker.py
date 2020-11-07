#!/usr/bin/env python

import asyncio
import logging
from typing import (
    Optional
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.logger import HummingbotLogger
from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.utils.async_utils import (
    safe_ensure_future,
    safe_gather,
)
from hummingbot.connector.exchange.dexfin.dexfin_api_user_stream_data_source import DexfinAPIUserStreamDataSource
from hummingbot.connector.exchange.dexfin.dexfin_auth import DexfinAuth
# from dexfin.client import Client as DexfinClient


class DexfinUserStreamTracker(UserStreamTracker):
    _kust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._kust_logger is None:
            cls._kust_logger = logging.getLogger(__name__)
        return cls._kust_logger

    def __init__(self,
                 dexfin_auth: Optional[DexfinAuth] = None):
        super().__init__()
        self._dexfin_client: DexfinAuth = dexfin_auth
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if not self._data_source:
            self._data_source = DexfinAPIUserStreamDataSource(dexfin_auth=self._dexfin_client)
        return self._data_source

    @property
    def exchange_name(self) -> str:
        return "dexfin"

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(
            self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream)
        )
        await safe_gather(self._user_stream_tracking_task)
