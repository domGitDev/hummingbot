#!/usr/bin/env python

import asyncio
import ujson
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
from decimal import Decimal
import requests
import cachetools.func
import time
import websockets
from websockets.exceptions import ConnectionClosed
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.dexfin.dexfin_order_book import DexfinOrderBook
from hummingbot.connector.exchange.dexfin.dexfin_utils import convert_to_exchange_trading_pair

SNAPSHOT_REST_URL = "https://test.dexfin.dev/api/v2/peatio/public/markets/{0}/depth"
DIFF_STREAM_URL = "wss://test.dexfin.dev/api/v2/ranger/public/"
TICKER_PRICE_CHANGE_URL = "https://test.dexfin.dev/api/v2/peatio/public/markets"
EXCHANGE_INFO_URL = "https://test.dexfin.dev/api/v2/peatio/public/markets"


class DexfinAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _kaobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._kaobds_logger is None:
            cls._kaobds_logger = logging.getLogger(__name__)
        return cls._kaobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        results = dict()
        async with aiohttp.ClientSession() as client:
            resp = await client.get(f"{TICKER_PRICE_CHANGE_URL}/tickers")
            resp_json = await resp.json()
            for trading_pair in trading_pairs:
                if trading_pair not in resp_json:
                    continue
                market = convert_to_exchange_trading_pair(trading_pair)
                resp_record = resp_json[market]["tikcer"]
                results[trading_pair] = float(resp_record["last"])
        return results

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        from hummingbot.connector.exchange.dexfin.dexfin_utils import convert_to_exchange_trading_pair

        market = convert_to_exchange_trading_pair(trading_pair)
        resp = requests.get(url=f"{TICKER_PRICE_CHANGE_URL}/{market}/tickers")
        record = resp.json()
        result = Decimal(record["ticker"]["avg_price"])
        return result if result else None

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            from hummingbot.connector.exchange.dexfin.dexfin_utils import convert_from_exchange_trading_pair
            async with aiohttp.ClientSession() as client:
                async with client.get(EXCHANGE_INFO_URL, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        raw_trading_pairs = [d["id"] for d in data if d["state"] == "enabled"]
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in raw_trading_pairs:
                            converted_trading_pair: Optional[str] = \
                                convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list

        except Exception:
            # Do nothing if the request fails -- there will be no autocomplete for dexfin trading pairs
            pass
        return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        params: Dict = {"limit": str(limit)} if limit != 0 else {}
        market = convert_to_exchange_trading_pair(trading_pair)
        async with client.get(SNAPSHOT_REST_URL.format(market), params=params) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching Dexfin market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()

            # Need to add the symbol into the snapshot message for the Kafka message queue.
            # Because otherwise, there'd be no way for the receiver to know which market the
            # snapshot belongs to.

            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        market = convert_to_exchange_trading_pair(trading_pair)
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, market, 1000)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = DexfinOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        # Terminate the recv() loop as soon as the next message timed out, so the outer loop can reconnect.
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                converted_pairs = []
                for trading_pair in self._trading_pairs:
                    converted_pairs.append(convert_to_exchange_trading_pair(trading_pair).lower())

                ws_path: str = "&".join([f"stream={trading_pair}.trades" for trading_pair in converted_pairs])
                stream_url: str = f"{DIFF_STREAM_URL}?{ws_path}"
                logging.info(f"Connecting to {stream_url}")

                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        market, topic = None, None
                        for item in msg.items():
                            if "trades" in item[0]:
                                market, topic = item[0].split('.')
                                break
                        for trade in msg[f"{market}.{topic}"][topic]:
                            trade_msg: OrderBookMessage = DexfinOrderBook.trade_message_from_exchange(
                                trade, metadata={"trading_pair": market})
                            output.put_nowait(trade_msg)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                converted_pairs = []
                for trading_pair in self._trading_pairs:
                    converted_pairs.append(convert_to_exchange_trading_pair(trading_pair).lower())

                ws_path: str = "&".join([f"stream={trading_pair}.ob-inc" for trading_pair in converted_pairs])
                stream_url: str = f"{DIFF_STREAM_URL}?{ws_path}"
                async with websockets.connect(stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    async for raw_msg in self._inner_messages(ws):
                        msg = ujson.loads(raw_msg)
                        market, topic = None, None
                        for item in msg.items():
                            if "ob-inc" in item[0]:
                                market, topic = item[0].split('.')
                                break
                        if market is None or topic is None:
                            continue
                        if topic and 'ob-snap' not in topic:
                            continue
                        obook = msg[f"{market}.{topic}"]
                        order_book_message: OrderBookMessage = DexfinOrderBook.diff_message_from_exchange(
                            obook, time.time(), metadata={"trading_pair": market})
                        output.put_nowait(order_book_message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with aiohttp.ClientSession() as client:
                    for trading_pair in self._trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = DexfinOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
