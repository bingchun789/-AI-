import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fetch_binance_ai_select import fetch_dataset, fetch_rendered_signal_lists
from strategy_registry import (
    STRATEGY_CONFIG_FILE,
    STRATEGY_STATUS_FILE,
    get_strategy_config,
    write_strategy_status,
)


BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_TESTNET_BASE_URL = "https://demo-fapi.binance.com"

LONG = "LONG"
SHORT = "SHORT"

LONG_STRATEGY_ID = "ai_select_futures_long"
SHORT_STRATEGY_ID = "ai_select_futures_short"


def load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ[key.strip()] = value.strip().strip("'").strip('"')


def get_env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def normalize_asset(raw_asset: str, base_asset: str | None) -> str:
    if base_asset:
        return base_asset
    if "@" in raw_asset:
        return raw_asset.split("@", 1)[0]
    if "_" in raw_asset:
        return raw_asset.split("_", 1)[0]
    return raw_asset


def http_request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: int = 30,
) -> Any:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            **(headers or {}),
        },
        data=body,
        method=method,
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            payload = {"status": exc.code, "body": raw_body}
        raise RuntimeError(f"HTTP {exc.code} {payload}") from exc


def http_get_json(url: str, timeout: int = 30) -> Any:
    return http_request_json(url, timeout=timeout)


@dataclass
class BotConfig:
    score_threshold: float
    negative_score_threshold: float
    interval: str
    poll_interval_seconds: int
    dry_run: bool
    quote_asset: str
    usdt_per_trade: float
    max_new_positions_per_cycle: int
    max_total_open_positions: int
    max_long_open_positions: int
    max_short_open_positions: int
    enable_min_signal_count_filter: bool
    min_signal_count_to_open: int
    cooldown_minutes: int
    required_margin_mode: str
    skip_if_margin_mode_unavailable: bool
    leverage: int
    min_quote_volume_24h_usdt: float
    enable_margin_usage_cap: bool
    max_margin_usage_pct: float
    enable_volatility_filter: bool
    volatility_interval: str
    volatility_lookback_bars: int
    max_single_bar_range_pct: float
    enable_funding_rate_filter: bool
    max_abs_funding_rate_pct: float
    enable_correlation_filter: bool
    correlation_interval: str
    correlation_lookback_bars: int
    correlation_threshold: float
    enable_trend_confirmation: bool
    trend_interval: str
    trend_ma_period: int
    trend_fallback_intervals: tuple[str, ...]
    enable_time_exit: bool
    max_hold_hours: int
    time_exit_min_pnl_pct: float
    enable_stop_loss: bool
    stop_loss_pct: float
    enable_profit_lock: bool
    profit_lock_tiers: str
    enable_profit_protection: bool
    profit_protection_activate_pct: float
    profit_protection_trail_pct: float
    enable_signal_drop_guard: bool
    signal_drop_guard_ratio: float
    signal_drop_guard_min_candidates: int
    signal_lost_exit_confirm_rounds: int
    estimated_taker_fee_rate: float
    strategy_config_file: Path
    strategy_status_file: Path
    state_file: Path
    positive_snapshot_file: Path
    negative_snapshot_file: Path
    log_file: Path


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"positions": {}, "history": []}
        return json.loads(self.path.read_text(encoding="utf-8-sig"))

    def save(self, state: dict[str, Any]) -> None:
        self.path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
        )


class BinanceFuturesCatalog:
    def __init__(self, exchange_info_url: str) -> None:
        self.exchange_info_url = exchange_info_url
        self._symbols_by_base_asset: dict[str, dict[str, Any]] | None = None

    def refresh(self) -> None:
        payload = http_get_json(self.exchange_info_url)
        symbols = {}
        for item in payload.get("symbols", []):
            if item.get("status") != "TRADING":
                continue
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            symbols[item["baseAsset"]] = item
        self._symbols_by_base_asset = symbols

    def get_contract(self, base_asset: str) -> dict[str, Any] | None:
        if self._symbols_by_base_asset is None:
            self.refresh()
        assert self._symbols_by_base_asset is not None
        return self._symbols_by_base_asset.get(base_asset)


def position_key(asset: str, side: str) -> str:
    return f"{asset}:{side}"


def enter_action(side: str) -> str:
    return "enter_long" if side == LONG else "enter_short"


def exit_action(side: str) -> str:
    return "exit_long" if side == LONG else "exit_short"


def signal_enter_reason(side: str) -> str:
    return "strong_positive_signal" if side == LONG else "strong_negative_signal"


def signal_hold_reason(side: str) -> str:
    return "still_strong_positive" if side == LONG else "still_strong_negative"


def side_from_position(position: dict[str, Any]) -> str:
    side = position.get("side")
    if side in {LONG, SHORT}:
        return side
    return LONG


def live_side_from_amount(position_amt_raw: str | Decimal | int | float) -> str | None:
    amount = Decimal(str(position_amt_raw))
    if amount > 0:
        return LONG
    if amount < 0:
        return SHORT
    return None


def migrate_state(state: dict[str, Any]) -> dict[str, Any]:
    positions = state.get("positions", {})
    migrated_positions: dict[str, Any] = {}
    for key, position in positions.items():
        if not isinstance(position, dict):
            continue
        asset = position.get("asset") or key.split(":", 1)[0]
        side = side_from_position(position)
        strategy_id = position.get("strategyId")
        if strategy_id not in {LONG_STRATEGY_ID, SHORT_STRATEGY_ID}:
            strategy_id = LONG_STRATEGY_ID if side == LONG else SHORT_STRATEGY_ID
        migrated_positions[position_key(asset, side)] = {
            **position,
            "asset": asset,
            "side": side,
            "strategyId": strategy_id,
            "signalLostRounds": int(position.get("signalLostRounds", 0) or 0),
            "minPnlPct": position.get(
                "minPnlPct",
                position.get("lastPnlPct", "0"),
            ),
        }
    state["positions"] = migrated_positions

    migrated_history = []
    for event in state.get("history", []):
        if not isinstance(event, dict):
            continue
        action = event.get("action")
        side = event.get("side")
        if side not in {LONG, SHORT}:
            if action in {"enter_short", "exit_short"}:
                side = SHORT
            else:
                side = LONG
        strategy_id = event.get("strategyId")
        if strategy_id not in {LONG_STRATEGY_ID, SHORT_STRATEGY_ID}:
            strategy_id = LONG_STRATEGY_ID if side == LONG else SHORT_STRATEGY_ID
        migrated_history.append({**event, "side": side, "strategyId": strategy_id})
    state["history"] = migrated_history
    return state


class BrokerAdapter:
    name = "base"

    def exchange_info_url(self) -> str:
        return BINANCE_FUTURES_EXCHANGE_INFO_URL

    def supported_margin_modes(self, contract_symbol: str) -> set[str]:
        return {"CROSS", "ISOLATED"}

    def get_mark_price(self, contract_symbol: str) -> Decimal:
        payload = http_get_json(
            f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={contract_symbol}"
        )
        return Decimal(str(payload["markPrice"]))

    def get_mark_prices(self, contract_symbols: list[str]) -> dict[str, Decimal]:
        payload = http_get_json("https://fapi.binance.com/fapi/v1/premiumIndex")
        return {
            item["symbol"]: Decimal(str(item["markPrice"]))
            for item in payload
            if item.get("symbol") in contract_symbols
        }

    def get_quote_volume_24h(self, contract_symbol: str) -> Decimal:
        payload = http_get_json(
            f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={contract_symbol}"
        )
        return Decimal(str(payload.get("quoteVolume", "0")))

    def get_last_funding_rate_pct(self, contract_symbol: str) -> Decimal:
        payload = http_get_json(
            f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={contract_symbol}"
        )
        return Decimal(str(payload.get("lastFundingRate", "0"))) * Decimal("100")

    def get_klines(
        self, contract_symbol: str, interval: str, limit: int
    ) -> list[list[Any]]:
        return http_get_json(
            f"https://fapi.binance.com/fapi/v1/klines?symbol={contract_symbol}&interval={interval}&limit={limit}"
        )

    def get_account_snapshot(self) -> dict[str, Any] | None:
        return None

    def get_income_history(
        self,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        income_type: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return []

    def get_user_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        return []

    def get_force_orders(
        self,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return []

    def get_all_orders(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        return []

    def has_open_position(self, state: dict[str, Any], asset: str, side: str) -> bool:
        return position_key(asset, side) in state.get("positions", {})

    def get_live_position(
        self, contract_symbol: str, side: str | None = None
    ) -> dict[str, Any] | None:
        return None

    def ensure_stop_loss(
        self,
        *,
        contract_symbol: str,
        side: str,
        position: dict[str, Any],
        stop_loss_pct: float,
        dry_run: bool,
    ) -> dict[str, Any]:
        stop_price = calculate_stop_loss_price(
            position,
            Decimal(str(stop_loss_pct)),
        )
        return {
            "orderId": None,
            "status": "DRY_RUN_PROTECTED" if dry_run else "STOP_LOSS_UNAVAILABLE",
            "configured": stop_price is not None,
            "stopPrice": format_decimal_value(stop_price),
            "stopLossPct": format_decimal_value(stop_loss_pct),
        }

    def close_position(
        self,
        *,
        contract_symbol: str,
        asset: str,
        side: str,
        position: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def place_long_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def place_short_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        raise NotImplementedError


class MockBrokerAdapter(BrokerAdapter):
    name = "mock"

    def __init__(self, forced_margin_modes: set[str] | None = None) -> None:
        self._forced_margin_modes = forced_margin_modes or {"CROSS", "ISOLATED"}

    def supported_margin_modes(self, contract_symbol: str) -> set[str]:
        return self._forced_margin_modes

    def _build_open_result(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
        status_if_filled: str,
    ) -> dict[str, Any]:
        order_id = f"mock-{int(time.time() * 1000)}-{contract_symbol}"
        status = "DRY_RUN_ACCEPTED" if dry_run else status_if_filled
        return {
            "orderId": order_id,
            "status": status,
            "contractSymbol": contract_symbol,
            "asset": asset,
            "notionalUsdt": notional_usdt,
            "entryPrice": format(self.get_mark_price(contract_symbol), "f"),
            "quantity": None,
            "metadata": metadata,
        }

    def place_long_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        return self._build_open_result(
            contract_symbol=contract_symbol,
            asset=asset,
            notional_usdt=notional_usdt,
            metadata=metadata,
            dry_run=dry_run,
            status_if_filled="MOCK_FILLED",
        )

    def place_short_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        return self._build_open_result(
            contract_symbol=contract_symbol,
            asset=asset,
            notional_usdt=notional_usdt,
            metadata=metadata,
            dry_run=dry_run,
            status_if_filled="MOCK_FILLED",
        )

    def close_position(
        self,
        *,
        contract_symbol: str,
        asset: str,
        side: str,
        position: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        order_id = f"mock-close-{int(time.time() * 1000)}-{contract_symbol}"
        status = "DRY_RUN_CLOSE_ACCEPTED" if dry_run else "MOCK_CLOSED"
        return {
            "orderId": order_id,
            "status": status,
            "contractSymbol": contract_symbol,
            "asset": asset,
            "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
            "confirmedClosed": True,
            "closedAtMs": int(time.time() * 1000),
            "closeRetryCount": 0,
        }

    def ensure_stop_loss(
        self,
        *,
        contract_symbol: str,
        side: str,
        position: dict[str, Any],
        stop_loss_pct: float,
        dry_run: bool,
    ) -> dict[str, Any]:
        stop_price = calculate_stop_loss_price(
            position,
            Decimal(str(stop_loss_pct)),
        )
        return {
            "orderId": f"mock-stop-{int(time.time() * 1000)}-{contract_symbol}",
            "status": "DRY_RUN_PROTECTED" if dry_run else "MOCK_PROTECTED",
            "configured": stop_price is not None,
            "stopPrice": format_decimal_value(stop_price),
            "stopLossPct": format_decimal_value(stop_loss_pct),
        }


class BinanceTestnetBrokerAdapter(BrokerAdapter):
    name = "binance_testnet"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        leverage: int,
        required_margin_mode: str,
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.leverage = leverage
        self.required_margin_mode = required_margin_mode.upper()
        self.base_url = BINANCE_TESTNET_BASE_URL
        self._exchange_info: dict[str, Any] | None = None
        self._price_cache: dict[str, Decimal] = {}
        self._quote_volume_cache: dict[str, Decimal] = {}
        self._funding_rate_cache: dict[str, Decimal] = {}
        self._kline_cache: dict[tuple[str, str, int], list[list[Any]]] = {}
        self._time_offset_ms = 0
        self._position_risks: dict[str, dict[str, Any]] | None = None

    def exchange_info_url(self) -> str:
        return f"{self.base_url}/fapi/v1/exchangeInfo"

    def _sync_server_time(self) -> None:
        payload = http_get_json(f"{self.base_url}/fapi/v1/time")
        server_time = int(payload["serverTime"])
        local_time = int(time.time() * 1000)
        self._time_offset_ms = server_time - local_time

    def _signed_request(self, method: str, path: str, params: dict[str, Any]) -> Any:
        signed_params = {
            **params,
            "recvWindow": 60000,
            "timestamp": int(time.time() * 1000) + self._time_offset_ms,
        }
        query = urlencode(signed_params, doseq=True)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed_query = f"{query}&signature={signature}"
        url = f"{self.base_url}{path}"
        body = None
        if method.upper() in {"GET", "DELETE"}:
            url = f"{url}?{signed_query}"
        else:
            body = signed_query.encode("utf-8")
        try:
            return http_request_json(
                url,
                method=method,
                headers={
                    "X-MBX-APIKEY": self.api_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                body=body,
            )
        except RuntimeError as exc:
            if "-1021" not in str(exc):
                raise
            self._sync_server_time()
            return self._signed_request(method, path, params)

    def _public_get(self, path: str, params: dict[str, Any]) -> Any:
        query = urlencode(params, doseq=True)
        return http_get_json(f"{self.base_url}{path}?{query}")

    def _get_exchange_info(self) -> dict[str, Any]:
        if self._exchange_info is None:
            self._exchange_info = http_get_json(self.exchange_info_url())
        return self._exchange_info

    def _get_symbol_meta(self, contract_symbol: str) -> dict[str, Any]:
        for item in self._get_exchange_info().get("symbols", []):
            if item.get("symbol") == contract_symbol:
                return item
        raise ValueError(f"Contract metadata not found for {contract_symbol}")

    def _get_price(self, contract_symbol: str) -> Decimal:
        if contract_symbol not in self._price_cache:
            payload = self._public_get("/fapi/v1/ticker/price", {"symbol": contract_symbol})
            self._price_cache[contract_symbol] = Decimal(str(payload["price"]))
        return self._price_cache[contract_symbol]

    def get_mark_price(self, contract_symbol: str) -> Decimal:
        payload = self._public_get("/fapi/v1/premiumIndex", {"symbol": contract_symbol})
        return Decimal(str(payload["markPrice"]))

    def get_mark_prices(self, contract_symbols: list[str]) -> dict[str, Decimal]:
        payload = http_get_json(f"{self.base_url}/fapi/v1/premiumIndex")
        return {
            item["symbol"]: Decimal(str(item["markPrice"]))
            for item in payload
            if item.get("symbol") in contract_symbols
        }

    def get_quote_volume_24h(self, contract_symbol: str) -> Decimal:
        if contract_symbol not in self._quote_volume_cache:
            payload = self._public_get("/fapi/v1/ticker/24hr", {"symbol": contract_symbol})
            self._quote_volume_cache[contract_symbol] = Decimal(
                str(payload.get("quoteVolume", "0"))
            )
        return self._quote_volume_cache[contract_symbol]

    def get_last_funding_rate_pct(self, contract_symbol: str) -> Decimal:
        if contract_symbol not in self._funding_rate_cache:
            payload = self._public_get("/fapi/v1/premiumIndex", {"symbol": contract_symbol})
            self._funding_rate_cache[contract_symbol] = (
                Decimal(str(payload.get("lastFundingRate", "0"))) * Decimal("100")
            )
        return self._funding_rate_cache[contract_symbol]

    def get_klines(
        self, contract_symbol: str, interval: str, limit: int
    ) -> list[list[Any]]:
        key = (contract_symbol, interval, limit)
        if key not in self._kline_cache:
            self._kline_cache[key] = self._public_get(
                "/fapi/v1/klines",
                {"symbol": contract_symbol, "interval": interval, "limit": limit},
            )
        return self._kline_cache[key]

    def _refresh_position_risks(self) -> dict[str, dict[str, Any]]:
        if self._position_risks is None:
            payload_v2 = self._signed_request("GET", "/fapi/v2/positionRisk", {})
            payload_v3 = self._signed_request("GET", "/fapi/v3/positionRisk", {})
            payload_v2_by_symbol = {item["symbol"]: item for item in payload_v2}
            merged_payload = [
                {**payload_v2_by_symbol.get(item["symbol"], {}), **item}
                for item in payload_v3
            ]
            self._position_risks = {item["symbol"]: item for item in merged_payload}
        return self._position_risks

    def _quantize_quantity(self, contract_symbol: str, notional_usdt: float) -> Decimal:
        meta = self._get_symbol_meta(contract_symbol)
        filters = meta.get("filters", [])
        lot_size = next(
            item for item in filters if item.get("filterType") == "LOT_SIZE"
        )
        market_lot_size = next(
            (item for item in filters if item.get("filterType") == "MARKET_LOT_SIZE"),
            None,
        )
        qty_filter = market_lot_size or lot_size
        step_size = Decimal(str(qty_filter["stepSize"]))
        min_qty = Decimal(str(qty_filter["minQty"]))
        max_qty_raw = qty_filter.get("maxQty")
        max_qty = (
            Decimal(str(max_qty_raw))
            if max_qty_raw not in (None, "", "0", 0)
            else None
        )
        price = self._get_price(contract_symbol)
        raw_qty = Decimal(str(notional_usdt)) / price
        steps = (raw_qty / step_size).quantize(Decimal("1"), rounding=ROUND_DOWN)
        quantity = steps * step_size

        min_notional = None
        for item in meta.get("filters", []):
            if item.get("filterType") == "NOTIONAL" and item.get("minNotional") not in (None, ""):
                min_notional = Decimal(str(item["minNotional"]))
                break
            if item.get("filterType") == "MIN_NOTIONAL" and item.get("notional") not in (None, ""):
                min_notional = Decimal(str(item["notional"]))
                break

        if min_notional is not None and quantity * price < min_notional:
            min_steps = (min_notional / price / step_size).quantize(
                Decimal("1"), rounding=ROUND_DOWN
            )
            if min_steps * step_size * price < min_notional:
                min_steps += 1
            quantity = min_steps * step_size

        if max_qty is not None and quantity > max_qty:
            logging.info(
                "quantity_capped_by_exchange symbol=%s requestedQty=%s maxQty=%s",
                contract_symbol,
                format(quantity, "f"),
                format(max_qty, "f"),
            )
            quantity = max_qty

        if quantity < min_qty:
            raise ValueError(
                f"{contract_symbol} quantity {quantity} is below minQty {min_qty}. "
                "Increase USDT_PER_TRADE."
            )
        if min_notional is not None and quantity * price < min_notional:
            raise ValueError(
                f"{contract_symbol} maxQty-capped quantity {quantity} cannot satisfy minNotional {min_notional}. "
                "Increase USDT_PER_TRADE or skip this symbol."
            )
        return quantity.normalize()

    def _ensure_margin_and_leverage(self, contract_symbol: str) -> None:
        margin_type = "ISOLATED" if self.required_margin_mode == "ISOLATED" else "CROSSED"
        try:
            self._signed_request(
                "POST",
                "/fapi/v1/marginType",
                {"symbol": contract_symbol, "marginType": margin_type},
            )
        except Exception:
            logging.info("margin_type_unchanged_or_failed symbol=%s", contract_symbol)

        try:
            self._signed_request(
                "POST",
                "/fapi/v1/leverage",
                {"symbol": contract_symbol, "leverage": self.leverage},
            )
        except Exception:
            logging.info("leverage_unchanged_or_failed symbol=%s", contract_symbol)

    def _quantize_price(
        self,
        contract_symbol: str,
        raw_price: Decimal,
        *,
        rounding: str,
    ) -> Decimal:
        meta = self._get_symbol_meta(contract_symbol)
        price_filter = next(
            item for item in meta.get("filters", []) if item.get("filterType") == "PRICE_FILTER"
        )
        tick_size = Decimal(str(price_filter["tickSize"]))
        if tick_size == Decimal("0"):
            return raw_price
        steps = (raw_price / tick_size).quantize(Decimal("1"), rounding=rounding)
        price = steps * tick_size
        if price <= Decimal("0"):
            raise ValueError(
                f"{contract_symbol} stop price {price} is invalid for stop loss placement."
            )
        return price.normalize()

    def _list_open_orders(self, contract_symbol: str) -> list[dict[str, Any]]:
        payload = self._signed_request(
            "GET",
            "/fapi/v1/openOrders",
            {"symbol": contract_symbol},
        )
        return payload if isinstance(payload, list) else []

    def _list_open_algo_orders(self, contract_symbol: str) -> list[dict[str, Any]]:
        payload = self._signed_request(
            "GET",
            "/fapi/v1/openAlgoOrders",
            {
                "symbol": contract_symbol,
                "algoType": "CONDITIONAL",
            },
        )
        return payload if isinstance(payload, list) else []

    def _protective_stop_trigger_price(self, order: dict[str, Any]) -> Decimal | None:
        return (
            decimal_or_none(order.get("triggerPrice"))
            or decimal_or_none(order.get("stopPrice"))
            or decimal_or_none(order.get("activatePrice"))
        )

    def _is_protective_stop_order(self, order: dict[str, Any], side: str) -> bool:
        order_type = str(
            order.get("type") or order.get("origType") or order.get("orderType") or ""
        ).upper()
        if "STOP" not in order_type:
            return False
        close_position = str(order.get("closePosition") or "").strip().lower() == "true"
        if not close_position:
            return False
        return str(order.get("side") or "").upper() == side

    def _cancel_protective_stop_orders(self, contract_symbol: str, side: str) -> None:
        order_side = "SELL" if side == LONG else "BUY"
        for order in self._list_open_orders(contract_symbol):
            if not self._is_protective_stop_order(order, order_side):
                continue
            try:
                self._signed_request(
                    "DELETE",
                    "/fapi/v1/order",
                    {
                        "symbol": contract_symbol,
                        "orderId": order.get("orderId"),
                    },
                )
            except Exception as exc:
                logging.warning(
                    "stop_loss_cancel_failed symbol=%s side=%s orderId=%s error=%s",
                    contract_symbol,
                    side,
                    order.get("orderId"),
                    exc,
                )
        for order in self._list_open_algo_orders(contract_symbol):
            if not self._is_protective_stop_order(order, order_side):
                continue
            algo_id = order.get("algoId")
            client_algo_id = order.get("clientAlgoId")
            if algo_id in (None, "") and client_algo_id in (None, ""):
                logging.warning(
                    "stop_loss_cancel_missing_algo_id symbol=%s side=%s payload=%s",
                    contract_symbol,
                    side,
                    order,
                )
                continue
            params: dict[str, Any] = {"symbol": contract_symbol}
            if algo_id not in (None, ""):
                params["algoId"] = algo_id
            else:
                params["clientAlgoId"] = client_algo_id
            try:
                self._signed_request(
                    "DELETE",
                    "/fapi/v1/algoOrder",
                    params,
                )
            except Exception as exc:
                logging.warning(
                    "stop_loss_algo_cancel_failed symbol=%s side=%s algoId=%s clientAlgoId=%s error=%s",
                    contract_symbol,
                    side,
                    algo_id,
                    client_algo_id,
                    exc,
                )

    def supported_margin_modes(self, contract_symbol: str) -> set[str]:
        return {"CROSS", "ISOLATED"}

    def has_open_position(self, state: dict[str, Any], asset: str, side: str) -> bool:
        item = self._refresh_position_risks().get(f"{asset}USDT")
        if not item:
            return False
        return live_side_from_amount(item.get("positionAmt", "0")) == side

    def get_live_position(
        self, contract_symbol: str, side: str | None = None
    ) -> dict[str, Any] | None:
        item = self._refresh_position_risks().get(contract_symbol)
        if not item:
            return None
        live_side = live_side_from_amount(item.get("positionAmt", "0"))
        if live_side is None:
            return None
        if side and live_side != side:
            return None
        return item

    def _place_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
        order_side: str,
    ) -> dict[str, Any]:
        self._ensure_margin_and_leverage(contract_symbol)
        quantity = self._quantize_quantity(contract_symbol, notional_usdt)
        mark_price = self.get_mark_price(contract_symbol)
        path = "/fapi/v1/order/test" if dry_run else "/fapi/v1/order"
        result = self._signed_request(
            "POST",
            path,
            {
                "symbol": contract_symbol,
                "side": order_side,
                "type": "MARKET",
                "quantity": format(quantity, "f"),
                "newOrderRespType": "RESULT",
            },
        )
        self._position_risks = None
        entry_price = decimal_or_none(result.get("avgPrice")) or decimal_or_none(
            result.get("price")
        )
        if entry_price in (None, Decimal("0")):
            entry_price = mark_price
        actual_notional_usdt = float(quantity * entry_price)
        return {
            "orderId": result.get("orderId") or f"test-{int(time.time() * 1000)}",
            "status": result.get("status") or ("TESTNET_ACCEPTED" if dry_run else "FILLED"),
            "contractSymbol": contract_symbol,
            "asset": asset,
            "notionalUsdt": actual_notional_usdt,
            "quantity": format(quantity, "f"),
            "entryPrice": format(entry_price, "f"),
            "metadata": metadata,
        }

    def place_long_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        return self._place_market_order(
            contract_symbol=contract_symbol,
            asset=asset,
            notional_usdt=notional_usdt,
            metadata=metadata,
            dry_run=dry_run,
            order_side="BUY",
        )

    def place_short_market_order(
        self,
        *,
        contract_symbol: str,
        asset: str,
        notional_usdt: float,
        metadata: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        return self._place_market_order(
            contract_symbol=contract_symbol,
            asset=asset,
            notional_usdt=notional_usdt,
            metadata=metadata,
            dry_run=dry_run,
            order_side="SELL",
        )

    def close_position(
        self,
        *,
        contract_symbol: str,
        asset: str,
        side: str,
        position: dict[str, Any],
        dry_run: bool,
    ) -> dict[str, Any]:
        live_position = self.get_live_position(contract_symbol, side)
        if not live_position:
            return {
                "orderId": f"test-close-{int(time.time() * 1000)}",
                "status": "NO_POSITION",
                "contractSymbol": contract_symbol,
                "asset": asset,
                "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                "confirmedClosed": True,
                "closedAtMs": int(time.time() * 1000),
                "closeRetryCount": 0,
            }

        position_amt = Decimal(str(live_position.get("positionAmt", "0")))
        if position_amt == Decimal("0"):
            return {
                "orderId": f"test-close-{int(time.time() * 1000)}",
                "status": "NO_POSITION",
                "contractSymbol": contract_symbol,
                "asset": asset,
                "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                "confirmedClosed": True,
                "closedAtMs": int(time.time() * 1000),
                "closeRetryCount": 0,
            }

        order_side = "SELL" if position_amt > 0 else "BUY"
        quantity = abs(position_amt)
        path = "/fapi/v1/order/test" if dry_run else "/fapi/v1/order"
        retry_count = 0
        if not dry_run:
            try:
                self._cancel_protective_stop_orders(contract_symbol, side)
            except Exception as exc:
                logging.warning(
                    "stop_loss_cancel_before_close_failed symbol=%s side=%s error=%s",
                    contract_symbol,
                    side,
                    exc,
                )

        def submit_close_market(close_qty: Decimal) -> dict[str, Any]:
            nonlocal retry_count
            last_exc: Exception | None = None
            for attempt in range(3):
                try:
                    return self._signed_request(
                        "POST",
                        path,
                        {
                            "symbol": contract_symbol,
                            "side": order_side,
                            "type": "MARKET",
                            "quantity": format(close_qty, "f"),
                            "reduceOnly": "true",
                            "newOrderRespType": "RESULT",
                        },
                    )
                except RuntimeError as exc:
                    last_exc = exc
                    if "-4131" not in str(exc):
                        raise
                    retry_count += 1
                    logging.warning(
                        "close_market_retry symbol=%s side=%s qty=%s attempt=%s error=%s",
                        contract_symbol,
                        side,
                        format(close_qty, "f"),
                        attempt + 1,
                        exc,
                    )
                    time.sleep(1.0)
            raise last_exc if last_exc is not None else RuntimeError("close_market_failed")

        def submit_close_limit_ioc(close_qty: Decimal) -> dict[str, Any]:
            nonlocal retry_count
            retry_count += 1
            logging.warning(
                "close_market_fallback_limit_ioc symbol=%s side=%s qty=%s",
                contract_symbol,
                side,
                format(close_qty, "f"),
            )
            return self._signed_request(
                "POST",
                path,
                {
                    "symbol": contract_symbol,
                    "side": order_side,
                    "type": "LIMIT",
                    "timeInForce": "IOC",
                    "quantity": format(close_qty, "f"),
                    "reduceOnly": "true",
                    "priceMatch": "OPPONENT",
                    "newOrderRespType": "RESULT",
                },
            )

        try:
            result = submit_close_market(quantity)
        except Exception as exc:
            if "-4131" in str(exc):
                try:
                    result = submit_close_limit_ioc(quantity)
                except Exception as fallback_exc:
                    return {
                        "orderId": f"failed-close-{int(time.time() * 1000)}",
                        "status": "CLOSE_REJECTED",
                        "contractSymbol": contract_symbol,
                        "asset": asset,
                        "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                        "confirmedClosed": False,
                        "closedAtMs": int(time.time() * 1000),
                        "closeRetryCount": retry_count,
                        "quantity": format(quantity, "f"),
                        "error": str(fallback_exc),
                    }
            else:
                return {
                    "orderId": f"failed-close-{int(time.time() * 1000)}",
                    "status": "CLOSE_REJECTED",
                    "contractSymbol": contract_symbol,
                    "asset": asset,
                    "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                    "confirmedClosed": False,
                    "closedAtMs": int(time.time() * 1000),
                    "closeRetryCount": retry_count,
                    "quantity": format(quantity, "f"),
                    "error": str(exc),
                }
        self._position_risks = None
        confirmed_closed = True
        if not dry_run:
            for _ in range(2):
                remaining_position = self.get_live_position(contract_symbol, side)
                if remaining_position is None:
                    confirmed_closed = True
                    break
                remaining_amt = abs(
                    Decimal(str(remaining_position.get("positionAmt", "0")))
                )
                if remaining_amt == Decimal("0"):
                    confirmed_closed = True
                    break
                confirmed_closed = False
                try:
                    result = submit_close_market(remaining_amt)
                except Exception as exc:
                    if "-4131" in str(exc):
                        try:
                            result = submit_close_limit_ioc(remaining_amt)
                        except Exception as fallback_exc:
                            return {
                                "orderId": result.get("orderId")
                                or f"failed-close-{int(time.time() * 1000)}",
                                "status": "CLOSE_REJECTED",
                                "contractSymbol": contract_symbol,
                                "asset": asset,
                                "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                                "confirmedClosed": False,
                                "closedAtMs": int(time.time() * 1000),
                                "closeRetryCount": retry_count,
                                "quantity": format(remaining_amt, "f"),
                                "error": str(fallback_exc),
                            }
                    else:
                        return {
                            "orderId": result.get("orderId")
                            or f"failed-close-{int(time.time() * 1000)}",
                            "status": "CLOSE_REJECTED",
                            "contractSymbol": contract_symbol,
                            "asset": asset,
                            "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                            "confirmedClosed": False,
                            "closedAtMs": int(time.time() * 1000),
                            "closeRetryCount": retry_count,
                            "quantity": format(remaining_amt, "f"),
                            "error": str(exc),
                        }
                self._position_risks = None
                time.sleep(0.5)
            else:
                remaining_position = self.get_live_position(contract_symbol, side)
                confirmed_closed = remaining_position is None
        return {
            "orderId": result.get("orderId") or f"test-close-{int(time.time() * 1000)}",
            "status": result.get("status") or ("TESTNET_CLOSE_ACCEPTED" if dry_run else "FILLED"),
            "contractSymbol": contract_symbol,
            "asset": asset,
            "exitPrice": str(result.get("avgPrice") or result.get("price") or self.get_mark_price(contract_symbol)),
            "confirmedClosed": confirmed_closed,
            "closedAtMs": result.get("updateTime")
            or result.get("transactTime")
            or int(time.time() * 1000),
            "closeRetryCount": retry_count,
            "quantity": format(quantity, "f"),
        }

    def ensure_stop_loss(
        self,
        *,
        contract_symbol: str,
        side: str,
        position: dict[str, Any],
        stop_loss_pct: float,
        dry_run: bool,
    ) -> dict[str, Any]:
        stop_price = calculate_stop_loss_price(
            position,
            Decimal(str(stop_loss_pct)),
        )
        if stop_price is None:
            raise ValueError(
                f"Unable to calculate stop loss price for {contract_symbol} {side}."
            )
        stop_price = self._quantize_price(
            contract_symbol,
            stop_price,
            rounding=ROUND_DOWN if side == LONG else ROUND_UP,
        )
        order_side = "SELL" if side == LONG else "BUY"
        if dry_run:
            return {
                "orderId": f"dry-stop-{int(time.time() * 1000)}",
                "status": "DRY_RUN_PROTECTED",
                "configured": True,
                "stopPrice": format(stop_price, "f"),
                "stopLossPct": format_decimal_value(stop_loss_pct),
            }

        protective_orders = [
            order
            for order in [
                *self._list_open_orders(contract_symbol),
                *self._list_open_algo_orders(contract_symbol),
            ]
            if self._is_protective_stop_order(order, order_side)
        ]
        if len(protective_orders) == 1:
            existing_stop_price = self._protective_stop_trigger_price(protective_orders[0])
            if existing_stop_price == stop_price:
                return {
                    "orderId": protective_orders[0].get("orderId")
                    or protective_orders[0].get("algoId")
                    or protective_orders[0].get("clientAlgoId"),
                    "status": "ALREADY_PROTECTED",
                    "configured": True,
                    "stopPrice": format(stop_price, "f"),
                    "stopLossPct": format_decimal_value(stop_loss_pct),
                }

        self._cancel_protective_stop_orders(contract_symbol, side)
        result = self._signed_request(
            "POST",
            "/fapi/v1/algoOrder",
            {
                "algoType": "CONDITIONAL",
                "symbol": contract_symbol,
                "side": order_side,
                "type": "STOP_MARKET",
                "triggerPrice": format(stop_price, "f"),
                "closePosition": "true",
                "workingType": "MARK_PRICE",
            },
        )
        return {
            "orderId": result.get("orderId") or result.get("algoId") or result.get("clientAlgoId"),
            "status": result.get("algoStatus") or result.get("status") or "STOP_MARKET_PLACED",
            "configured": True,
            "stopPrice": format(stop_price, "f"),
            "stopLossPct": format_decimal_value(stop_loss_pct),
        }

    def get_account_snapshot(self) -> dict[str, Any] | None:
        with ThreadPoolExecutor(max_workers=3) as executor:
            account_future = executor.submit(
                self._signed_request, "GET", "/fapi/v3/account", {}
            )
            positions_v3_future = executor.submit(
                self._signed_request, "GET", "/fapi/v3/positionRisk", {}
            )
            positions_v2_future = executor.submit(
                self._signed_request, "GET", "/fapi/v2/positionRisk", {}
            )
            account = account_future.result()
            positions_v3 = positions_v3_future.result()
            positions_v2 = positions_v2_future.result()
        positions_v2_by_symbol = {item["symbol"]: item for item in positions_v2}
        positions = [
            {**positions_v2_by_symbol.get(item["symbol"], {}), **item}
            for item in positions_v3
        ]
        self._position_risks = {item["symbol"]: item for item in positions}
        active_positions = [
            item
            for item in positions
            if Decimal(str(item.get("positionAmt", "0"))) != Decimal("0")
        ]
        return {
            "source": "binance_testnet",
            "totalWalletBalance": account.get("totalWalletBalance"),
            "availableBalance": account.get("availableBalance"),
            "totalUnrealizedProfit": account.get("totalUnrealizedProfit"),
            "totalMarginBalance": account.get("totalMarginBalance"),
            "totalInitialMargin": account.get("totalInitialMargin"),
            "totalMaintMargin": account.get("totalMaintMargin"),
            "positionCount": len(active_positions),
            "positions": active_positions,
        }

    def get_income_history(
        self,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        income_type: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        if income_type:
            params["incomeType"] = income_type
        return self._signed_request("GET", "/fapi/v1/income", params)

    def get_user_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._signed_request("GET", "/fapi/v1/userTrades", params)

    def get_force_orders(
        self,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._signed_request("GET", "/fapi/v1/forceOrders", params)

    def get_all_orders(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if start_time_ms is not None:
            params["startTime"] = start_time_ms
        if end_time_ms is not None:
            params["endTime"] = end_time_ms
        return self._signed_request("GET", "/fapi/v1/allOrders", params)


def select_broker_adapter() -> BrokerAdapter:
    adapter_name = os.getenv("BROKER_ADAPTER", "mock").strip().lower()
    margin_modes = {
        part.strip().upper()
        for part in os.getenv("BROKER_MARGIN_MODES", "CROSS,ISOLATED").split(",")
        if part.strip()
    }
    if adapter_name == "mock":
        return MockBrokerAdapter(forced_margin_modes=margin_modes)
    if adapter_name == "binance_testnet":
        api_key = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        if not api_key or not api_secret:
            raise ValueError(
                "BROKER_ADAPTER=binance_testnet requires BINANCE_API_KEY and BINANCE_API_SECRET."
            )
        leverage = int(os.getenv("LEVERAGE", "2"))
        required_margin_mode = os.getenv("REQUIRED_MARGIN_MODE", "CROSS").upper()
        return BinanceTestnetBrokerAdapter(
            api_key=api_key,
            api_secret=api_secret,
            leverage=leverage,
            required_margin_mode=required_margin_mode,
        )
    raise ValueError(
        f"Unsupported BROKER_ADAPTER={adapter_name}. "
        "Use mock or binance_testnet."
    )


def get_score(item: dict[str, Any]) -> float:
    metrics = item.get("metrics", {})
    score = metrics.get("sentiment_score", {}).get("value", "0")
    return float(score)


def get_score_label(item: dict[str, Any]) -> str:
    return str(
        item.get("metrics", {})
        .get("sentiment_score", {})
        .get("valueLabel", "")
    ).strip()


def is_strong_positive(item: dict[str, Any], threshold: float) -> bool:
    label = get_score_label(item)
    if label:
        return label.lower() == "strong positive"
    return get_score(item) >= threshold


def is_strong_negative(item: dict[str, Any], threshold: float) -> bool:
    label = get_score_label(item)
    if label:
        return label.lower() == "strong negative"
    return get_score(item) <= threshold


def fetch_signal_assets(
    config: BotConfig,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (strong_positive, strong_negative) candidate lists."""
    try:
        rendered_payload = fetch_rendered_signal_lists(config.interval)
        if rendered_payload.get("ok"):
            data = rendered_payload.get("json", {}).get("data", {})
            positive = data.get("positiveItems", []) or []
            negative = data.get("negativeItems", []) or []
            if positive or negative:
                positive.sort(key=lambda item: (item.get("rank") or 999999, -get_score(item)))
                negative.sort(key=lambda item: (-(item.get("rank") or 0), get_score(item)))
                return positive, negative
    except Exception as exc:
        logging.warning("rendered_signal_fetch_failed: %s", exc)

    payload = fetch_dataset("assets", config.interval)
    if not payload.get("ok"):
        raise RuntimeError(f"AI Select request failed: {payload.get('status')}")

    items = payload.get("json", {}).get("data", {}).get("items", [])
    positive = [item for item in items if is_strong_positive(item, config.score_threshold)]
    negative = [item for item in items if is_strong_negative(item, config.negative_score_threshold)]
    positive.sort(key=lambda item: (item.get("rank") or 999999, -get_score(item)))
    negative.sort(key=lambda item: (-(item.get("rank") or 0), get_score(item)))
    return positive, negative


def is_in_cooldown(
    state: dict[str, Any], asset: str, side: str, cooldown_minutes: int
) -> bool:
    now = time.time()
    target_action = exit_action(side)
    for event in reversed(state.get("history", [])):
        if event.get("asset") != asset:
            continue
        if event.get("action") != target_action:
            continue
        cooldown_until_override = event.get("cooldownUntilOverride")
        if cooldown_until_override not in (None, ""):
            return now < float(cooldown_until_override)
        timestamp = event.get("timestamp", 0)
        return now - timestamp < cooldown_minutes * 60
    return False


def infer_position_quantity(position: dict[str, Any]) -> Decimal | None:
    quantity_raw = position.get("quantity")
    if quantity_raw not in (None, ""):
        return abs(Decimal(str(quantity_raw)))

    entry_price_raw = position.get("entryPrice")
    notional_raw = position.get("notionalUsdt")
    if entry_price_raw in (None, "") or notional_raw in (None, ""):
        return None

    entry_price = Decimal(str(entry_price_raw))
    if entry_price == Decimal("0"):
        return None
    return Decimal(str(notional_raw)) / entry_price


def calculate_realized_pnl(position: dict[str, Any], exit_price_raw: str | None) -> Decimal | None:
    entry_price_raw = position.get("entryPrice")
    if entry_price_raw in (None, "") or exit_price_raw in (None, ""):
        return None

    quantity = infer_position_quantity(position)
    if quantity is None:
        return None

    side = side_from_position(position)
    entry_price = Decimal(str(entry_price_raw))
    exit_price = Decimal(str(exit_price_raw))
    if side == SHORT:
        return (entry_price - exit_price) * quantity
    return (exit_price - entry_price) * quantity


def calculate_unrealized_pnl(
    position: dict[str, Any], mark_price_raw: str | None
) -> Decimal | None:
    entry_price_raw = position.get("entryPrice")
    if entry_price_raw in (None, "") or mark_price_raw in (None, ""):
        return None

    quantity = infer_position_quantity(position)
    if quantity is None:
        return None

    side = side_from_position(position)
    entry_price = Decimal(str(entry_price_raw))
    mark_price = Decimal(str(mark_price_raw))
    if side == SHORT:
        return (entry_price - mark_price) * quantity
    return (mark_price - entry_price) * quantity


def extract_api_return_basis(position: dict[str, Any]) -> Decimal | None:
    for key in ("positionInitialMargin", "initialMargin", "isolatedWallet", "isolatedMargin"):
        raw_value = position.get(key)
        if raw_value in (None, "", 0, "0"):
            continue
        basis = abs(Decimal(str(raw_value)))
        if basis != Decimal("0"):
            return basis
    return None


def calculate_unrealized_pnl_pct(
    position: dict[str, Any], mark_price_raw: str | None
) -> Decimal | None:
    unrealized_pnl_raw = position.get("unRealizedProfit")
    if unrealized_pnl_raw in (None, ""):
        unrealized_pnl_raw = position.get("unrealizedProfit")
    if unrealized_pnl_raw not in (None, ""):
        unrealized_pnl = Decimal(str(unrealized_pnl_raw))
    else:
        unrealized_pnl = calculate_unrealized_pnl(position, mark_price_raw)
        if unrealized_pnl is None:
            return None

    return_basis_raw = position.get("returnBasisUsdt")
    if return_basis_raw not in (None, "", 0, "0"):
        return_basis = abs(Decimal(str(return_basis_raw)))
        if return_basis != Decimal("0"):
            return (unrealized_pnl / return_basis) * Decimal("100")

    api_return_basis = extract_api_return_basis(position)
    if api_return_basis is None:
        return None
    return (unrealized_pnl / api_return_basis) * Decimal("100")


def position_age_hours(position: dict[str, Any]) -> float | None:
    opened_at = position.get("openedAt")
    if opened_at in (None, ""):
        return None
    return max(0.0, (time.time() - float(opened_at)) / 3600)


def infer_return_basis_usdt(position: dict[str, Any]) -> Decimal | None:
    return_basis_raw = position.get("returnBasisUsdt")
    if return_basis_raw not in (None, "", 0, "0"):
        return_basis = abs(Decimal(str(return_basis_raw)))
        if return_basis != Decimal("0"):
            return return_basis

    api_return_basis = extract_api_return_basis(position)
    if api_return_basis is not None:
        return api_return_basis

    notional = decimal_or_none(position.get("notionalUsdt"))
    leverage = decimal_or_none(position.get("leverage"))
    if notional is not None and notional != Decimal("0"):
        if leverage is not None and leverage > Decimal("0"):
            estimated_margin = abs(notional) / leverage
            if estimated_margin != Decimal("0"):
                return estimated_margin
        return abs(notional)

    quantity = infer_position_quantity(position)
    entry_price = decimal_or_none(position.get("entryPrice"))
    if quantity is not None and entry_price is not None and entry_price != Decimal("0"):
        entry_notional = abs(quantity * entry_price)
        if leverage is not None and leverage > Decimal("0"):
            estimated_margin = entry_notional / leverage
            if estimated_margin != Decimal("0"):
                return estimated_margin
        return entry_notional
    return None


def calculate_stop_loss_price(
    position: dict[str, Any],
    stop_loss_pct: Decimal,
) -> Decimal | None:
    if stop_loss_pct <= Decimal("0"):
        return None
    quantity = infer_position_quantity(position)
    entry_price = decimal_or_none(position.get("entryPrice"))
    return_basis = infer_return_basis_usdt(position)
    if quantity is None or quantity == Decimal("0") or entry_price is None or return_basis is None:
        return None

    max_loss_usdt = (return_basis * stop_loss_pct) / Decimal("100")
    price_delta = max_loss_usdt / quantity
    if side_from_position(position) == SHORT:
        stop_price = entry_price + price_delta
    else:
        stop_price = entry_price - price_delta
    if stop_price <= Decimal("0"):
        return None
    return stop_price


def should_trigger_stop_loss(
    *,
    config: BotConfig,
    current_pnl_pct: Decimal | None,
) -> bool:
    if not config.enable_stop_loss:
        return False
    if current_pnl_pct is None:
        return False
    return current_pnl_pct <= -Decimal(str(config.stop_loss_pct))


def parse_profit_lock_tiers(raw_value: str) -> list[tuple[Decimal, Decimal]]:
    tiers: list[tuple[Decimal, Decimal]] = []
    for part in (raw_value or "").split(","):
        chunk = part.strip()
        if not chunk or ":" not in chunk:
            continue
        activate_raw, lock_raw = chunk.split(":", 1)
        try:
            activate_pct = Decimal(activate_raw.strip())
            lock_pct = Decimal(lock_raw.strip())
        except Exception:
            continue
        if activate_pct <= Decimal("0") or lock_pct < Decimal("0"):
            continue
        tiers.append((activate_pct, lock_pct))
    tiers.sort(key=lambda item: item[0])
    return tiers


def should_trigger_profit_lock(
    *,
    config: BotConfig,
    position: dict[str, Any],
    current_pnl_pct: Decimal | None,
    peak_pnl_pct: Decimal | None,
) -> bool:
    if not config.enable_profit_lock:
        return False
    if current_pnl_pct is None or peak_pnl_pct is None:
        return False
    active_lock_pct = None
    for activate_pct, lock_pct in parse_profit_lock_tiers(config.profit_lock_tiers):
        if peak_pnl_pct >= activate_pct:
            active_lock_pct = lock_pct
    if active_lock_pct is None:
        return False
    return current_pnl_pct <= active_lock_pct


def should_trigger_time_exit(
    *,
    config: BotConfig,
    position: dict[str, Any],
    current_pnl_pct: Decimal | None,
) -> bool:
    if not config.enable_time_exit:
        return False
    age_hours = position_age_hours(position)
    if age_hours is None or age_hours < config.max_hold_hours:
        return False
    if current_pnl_pct is None:
        return True
    return current_pnl_pct <= Decimal(str(config.time_exit_min_pnl_pct))


def count_total_open_positions(state: dict[str, Any]) -> int:
    return sum(1 for _ in state.get("positions", {}).values())


def kline_closes(klines: list[list[Any]]) -> list[Decimal]:
    return [Decimal(str(item[4])) for item in klines if len(item) > 4]


def candle_max_range_pct(klines: list[list[Any]]) -> Decimal:
    max_range = Decimal("0")
    for item in klines:
        if len(item) < 4:
            continue
        high = Decimal(str(item[2]))
        low = Decimal(str(item[3]))
        open_price = Decimal(str(item[1]))
        if open_price == Decimal("0"):
            continue
        range_pct = ((high - low) / open_price) * Decimal("100")
        if range_pct > max_range:
            max_range = range_pct
    return max_range


def simple_ma(closes: list[Decimal], period: int) -> Decimal | None:
    if len(closes) < period or period <= 0:
        return None
    window = closes[-period:]
    return sum(window, Decimal("0")) / Decimal(str(period))


def normalize_intervals(raw_value: str) -> tuple[str, ...]:
    return tuple(
        interval.strip()
        for interval in raw_value.split(",")
        if interval.strip()
    )


def resolve_trend_confirmation(
    *,
    broker: BrokerAdapter,
    contract_symbol: str,
    config: BotConfig,
) -> dict[str, Any] | None:
    lookback = max(config.trend_ma_period, 21)
    intervals: list[str] = [config.trend_interval]
    for interval in config.trend_fallback_intervals:
        if interval not in intervals:
            intervals.append(interval)
    for interval in intervals:
        trend_klines = broker.get_klines(contract_symbol, interval, lookback)
        closes = kline_closes(trend_klines)
        ma_value = simple_ma(closes, config.trend_ma_period)
        current_close = closes[-1] if closes else None
        if ma_value is None or current_close is None:
            continue
        return {
            "interval": interval,
            "ma": ma_value,
            "close": current_close,
            "isFallback": interval != config.trend_interval,
        }
    return None


def returns_from_closes(closes: list[Decimal]) -> list[float]:
    returns: list[float] = []
    for prev, curr in zip(closes, closes[1:]):
        if prev == Decimal("0"):
            continue
        returns.append(float((curr - prev) / prev))
    return returns


def pearson_corr(xs: list[float], ys: list[float]) -> float | None:
    length = min(len(xs), len(ys))
    if length < 3:
        return None
    xs = xs[-length:]
    ys = ys[-length:]
    mean_x = sum(xs) / length
    mean_y = sum(ys) / length
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    denom_x = sum((x - mean_x) ** 2 for x in xs) ** 0.5
    denom_y = sum((y - mean_y) ** 2 for y in ys) ** 0.5
    if denom_x == 0 or denom_y == 0:
        return None
    return numerator / (denom_x * denom_y)


def current_margin_usage_pct(account_snapshot: dict[str, Any] | None) -> Decimal | None:
    if not account_snapshot:
        return None
    initial_margin_raw = account_snapshot.get("totalInitialMargin")
    margin_balance_raw = account_snapshot.get("totalMarginBalance")
    if initial_margin_raw in (None, "") or margin_balance_raw in (None, "", "0", 0):
        return None
    initial_margin = Decimal(str(initial_margin_raw))
    margin_balance = Decimal(str(margin_balance_raw))
    if margin_balance == Decimal("0"):
        return None
    return (initial_margin / margin_balance) * Decimal("100")


def update_profit_tracking(
    position: dict[str, Any], current_pnl_pct: Decimal | None
) -> dict[str, Decimal | None]:
    result = {"current": current_pnl_pct, "peak": None, "drawdown": None, "trough": None}
    if current_pnl_pct is None:
        return result

    previous_peak = Decimal(str(position.get("maxProfitPct", "0") or "0"))
    peak = previous_peak
    if current_pnl_pct > peak:
        peak = current_pnl_pct
        position["maxProfitPct"] = format(peak, "f")
        position["maxProfitAt"] = time.time()
    elif "maxProfitPct" not in position:
        position["maxProfitPct"] = format(peak, "f")

    previous_trough_raw = position.get("minPnlPct")
    if previous_trough_raw in (None, ""):
        trough = current_pnl_pct
        position["minPnlPct"] = format(trough, "f")
        position["minPnlAt"] = time.time()
    else:
        trough = Decimal(str(previous_trough_raw))
        if current_pnl_pct < trough:
            trough = current_pnl_pct
            position["minPnlPct"] = format(trough, "f")
            position["minPnlAt"] = time.time()

    position["lastPnlPct"] = format(current_pnl_pct, "f")
    position["lastPnlCheckAt"] = time.time()
    result["peak"] = peak
    result["drawdown"] = peak - current_pnl_pct
    result["trough"] = trough
    return result


def should_trigger_profit_protection(
    *,
    config: BotConfig,
    position: dict[str, Any],
    current_pnl_pct: Decimal | None,
    peak_pnl_pct: Decimal | None,
) -> bool:
    if not config.enable_profit_protection:
        return False
    if current_pnl_pct is None or peak_pnl_pct is None:
        return False

    activate_pct = Decimal(str(config.profit_protection_activate_pct))
    trail_pct = Decimal(str(config.profit_protection_trail_pct))
    if peak_pnl_pct < activate_pct:
        return False
    if peak_pnl_pct <= Decimal("0"):
        return False

    drawdown_ratio_pct = ((peak_pnl_pct - current_pnl_pct) / peak_pnl_pct) * Decimal("100")
    return drawdown_ratio_pct >= trail_pct


def format_decimal_value(value: Decimal | float | int | str | None) -> str | None:
    if value in (None, ""):
        return None
    try:
        return format(Decimal(str(value)), "f")
    except Exception:
        return str(value)


def decimal_or_none(value: Decimal | float | int | str | None) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def active_profit_lock_pct(config: BotConfig, peak_pnl_pct: Decimal | None) -> Decimal | None:
    if peak_pnl_pct is None:
        return None
    active_lock_pct = None
    for activate_pct, lock_pct in parse_profit_lock_tiers(config.profit_lock_tiers):
        if peak_pnl_pct >= activate_pct:
            active_lock_pct = lock_pct
    return active_lock_pct


def enrich_entry_audit_with_stop_loss(
    *,
    audit: dict[str, Any],
    config: BotConfig,
    stop_loss_result: dict[str, Any] | None,
) -> dict[str, Any]:
    enriched = dict(audit)
    enriched["stopLossEnabled"] = bool(config.enable_stop_loss)
    enriched["stopLossPct"] = format_decimal_value(config.stop_loss_pct)
    if not config.enable_stop_loss:
        enriched["stopLossConfigured"] = None
        enriched["stopLossStatus"] = "disabled"
        enriched["stopLossPrice"] = None
        return enriched

    enriched["stopLossConfigured"] = bool(
        stop_loss_result.get("configured") if stop_loss_result else False
    )
    enriched["stopLossStatus"] = (
        stop_loss_result.get("status") if stop_loss_result else "STOP_LOSS_SETUP_FAILED"
    )
    enriched["stopLossPrice"] = (
        stop_loss_result.get("stopPrice") if stop_loss_result else None
    )
    return enriched


def update_position_stop_loss_state(
    *,
    position: dict[str, Any],
    config: BotConfig,
    stop_loss_result: dict[str, Any] | None,
) -> None:
    position["stopLossEnabled"] = bool(config.enable_stop_loss)
    position["stopLossPct"] = format_decimal_value(config.stop_loss_pct)
    if not config.enable_stop_loss:
        position["stopLossConfigured"] = None
        position["stopLossStatus"] = "disabled"
        position["stopLossPrice"] = None
        position["stopLossOrderId"] = None
        position["stopLossUpdatedAt"] = time.time()
        return

    position["stopLossConfigured"] = bool(
        stop_loss_result.get("configured") if stop_loss_result else False
    )
    position["stopLossStatus"] = (
        stop_loss_result.get("status") if stop_loss_result else "STOP_LOSS_SETUP_FAILED"
    )
    position["stopLossPrice"] = (
        stop_loss_result.get("stopPrice") if stop_loss_result else None
    )
    position["stopLossOrderId"] = (
        stop_loss_result.get("orderId") if stop_loss_result else None
    )
    position["stopLossUpdatedAt"] = time.time()


def build_entry_audit_record(
    *,
    config: BotConfig,
    state: dict[str, Any],
    side: str,
    candidate_count: int,
    opened_before: int,
    side_positions_before: int,
    total_positions_before: int,
    margin_usage_pct: Decimal | None,
    quote_volume_24h: Decimal,
    max_range_pct: Decimal | None,
    funding_rate_pct: Decimal | None,
    trend_signal: dict[str, Any] | None,
    correlated_symbol: str | None,
    correlated_value: float | None,
) -> dict[str, Any]:
    side_limit = config.max_long_open_positions if side == LONG else config.max_short_open_positions
    return {
        "version": 1,
        "candidateCount": candidate_count,
        "minSignalFilterEnabled": bool(config.enable_min_signal_count_filter),
        "minSignalThreshold": int(config.min_signal_count_to_open),
        "cooldownMinutes": int(config.cooldown_minutes),
        "cooldownPassed": True,
        "marginModeCheckEnabled": bool(config.skip_if_margin_mode_unavailable),
        "requiredMarginMode": config.required_margin_mode,
        "marginModePassed": True,
        "marginUsageCapEnabled": bool(config.enable_margin_usage_cap),
        "marginUsagePct": format_decimal_value(margin_usage_pct),
        "maxMarginUsagePct": format_decimal_value(config.max_margin_usage_pct),
        "marginUsagePassed": True,
        "quoteVolume24hUsdt": format_decimal_value(quote_volume_24h),
        "minQuoteVolume24hUsdt": format_decimal_value(config.min_quote_volume_24h_usdt),
        "quoteVolumePassed": True,
        "volatilityFilterEnabled": bool(config.enable_volatility_filter),
        "maxRangePct": format_decimal_value(max_range_pct),
        "maxSingleBarRangePct": format_decimal_value(config.max_single_bar_range_pct),
        "volatilityPassed": True,
        "fundingRateFilterEnabled": bool(config.enable_funding_rate_filter),
        "fundingRatePct": format_decimal_value(funding_rate_pct),
        "maxAbsFundingRatePct": format_decimal_value(config.max_abs_funding_rate_pct),
        "fundingRatePassed": True,
        "trendConfirmationEnabled": bool(config.enable_trend_confirmation),
        "trendConfirmed": True if config.enable_trend_confirmation else None,
        "trendInterval": trend_signal.get("interval") if trend_signal else None,
        "trendClose": format_decimal_value(trend_signal.get("close")) if trend_signal else None,
        "trendMa": format_decimal_value(trend_signal.get("ma")) if trend_signal else None,
        "correlationFilterEnabled": bool(config.enable_correlation_filter),
        "correlationPassed": correlated_symbol is None,
        "correlatedSymbol": correlated_symbol,
        "correlation": round(correlated_value, 4) if correlated_value is not None else None,
        "correlationThreshold": config.correlation_threshold,
        "openedBefore": opened_before,
        "cycleLimit": int(config.max_new_positions_per_cycle),
        "cycleLimitPassed": opened_before < int(config.max_new_positions_per_cycle),
        "sidePositionsBefore": side_positions_before,
        "sideLimit": int(side_limit),
        "sideLimitPassed": side_positions_before < int(side_limit),
        "portfolioPositionsBefore": total_positions_before,
        "portfolioLimit": int(config.max_total_open_positions),
        "portfolioLimitPassed": total_positions_before < int(config.max_total_open_positions),
        "openPositionsForSide": count_open_positions_for_side(state, side),
        "totalOpenPositions": count_total_open_positions(state),
        "stopLossEnabled": bool(config.enable_stop_loss),
        "stopLossPct": format_decimal_value(config.stop_loss_pct),
        "stopLossConfigured": None,
        "stopLossStatus": None,
        "stopLossPrice": None,
    }


def build_exit_audit_record(
    *,
    config: BotConfig,
    position: dict[str, Any],
    reason: str,
    tracking: dict[str, Decimal | None] | None = None,
) -> dict[str, Any]:
    current_pnl_pct = (
        tracking.get("current") if tracking is not None else decimal_or_none(position.get("lastPnlPct"))
    )
    peak_pnl_pct = (
        tracking.get("peak") if tracking is not None else decimal_or_none(position.get("maxProfitPct"))
    )
    drawdown_pct = (
        tracking.get("drawdown") if tracking is not None else None
    )
    age_hours = position_age_hours(position)
    active_lock_pct = active_profit_lock_pct(config, peak_pnl_pct)
    drawdown_ratio_pct = None
    if (
        current_pnl_pct is not None
        and peak_pnl_pct is not None
        and peak_pnl_pct > Decimal("0")
    ):
        drawdown_ratio_pct = ((peak_pnl_pct - current_pnl_pct) / peak_pnl_pct) * Decimal("100")
    return {
        "version": 1,
        "reason": reason,
        "currentPnlPct": format_decimal_value(current_pnl_pct),
        "peakPnlPct": format_decimal_value(peak_pnl_pct),
        "drawdownPct": format_decimal_value(drawdown_pct),
        "drawdownRatioPct": format_decimal_value(drawdown_ratio_pct),
        "minPnlPct": format_decimal_value(position.get("minPnlPct")),
        "ageHours": round(age_hours, 4) if age_hours is not None else None,
        "timeExitEnabled": bool(config.enable_time_exit),
        "maxHoldHours": config.max_hold_hours,
        "timeExitMinPnlPct": format_decimal_value(config.time_exit_min_pnl_pct),
        "profitLockEnabled": bool(config.enable_profit_lock),
        "activeProfitLockPct": format_decimal_value(active_lock_pct),
        "profitLockTiers": config.profit_lock_tiers,
        "profitProtectionEnabled": bool(config.enable_profit_protection),
        "profitProtectionActivatePct": format_decimal_value(config.profit_protection_activate_pct),
        "profitProtectionTrailPct": format_decimal_value(config.profit_protection_trail_pct),
        "stopLossEnabled": bool(config.enable_stop_loss),
        "stopLossPct": format_decimal_value(config.stop_loss_pct),
        "stopLossConfigured": position.get("stopLossConfigured"),
        "configuredStopLossPrice": position.get("stopLossPrice"),
        "stopLossStatus": position.get("stopLossStatus"),
        "signalLostRounds": int(position.get("signalLostRounds", 0) or 0),
        "signalLostConfirmRounds": int(config.signal_lost_exit_confirm_rounds),
        "exchangePositionMissing": reason == "exchange_position_missing",
    }


def calculate_roundtrip_fee(
    *,
    entry_notional_raw: str | float | int | None,
    exit_price_raw: str | None,
    position: dict[str, Any],
    fee_rate: Decimal,
) -> Decimal | None:
    if entry_notional_raw in (None, "") or exit_price_raw in (None, ""):
        return None

    quantity = infer_position_quantity(position)
    if quantity is None:
        return None

    entry_notional = Decimal(str(entry_notional_raw))
    exit_notional = Decimal(str(exit_price_raw)) * quantity
    return (entry_notional * fee_rate) + (exit_notional * fee_rate)


def build_exit_record(
    *,
    asset: str,
    side: str,
    strategy_id: str,
    position: dict[str, Any],
    close_result: dict[str, Any],
    reason: str,
    fee_rate: Decimal,
    audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    realized_pnl = calculate_realized_pnl(position, close_result.get("exitPrice"))
    estimated_fee = calculate_roundtrip_fee(
        entry_notional_raw=position.get("notionalUsdt"),
        exit_price_raw=close_result.get("exitPrice"),
        position=position,
        fee_rate=fee_rate,
    )
    net_realized_pnl = None
    if realized_pnl is not None:
        net_realized_pnl = realized_pnl - (estimated_fee or Decimal("0"))

    close_side = "unknown"
    if net_realized_pnl is not None:
        close_side = "win" if net_realized_pnl > 0 else "loss"
        if net_realized_pnl == Decimal("0"):
            close_side = "flat"

    return {
        "timestamp": time.time(),
        "closedAtMs": close_result.get("closedAtMs"),
        "confirmedClosed": close_result.get("confirmedClosed"),
        "closeRetryCount": close_result.get("closeRetryCount", 0),
        "asset": asset,
        "contractSymbol": position["contractSymbol"],
        "side": side,
        "strategyId": strategy_id,
        "action": exit_action(side),
        "status": close_result["status"],
        "reason": reason,
        "exitPrice": close_result.get("exitPrice"),
        "entryPrice": position.get("entryPrice"),
        "entryNotionalUsdt": position.get("notionalUsdt"),
        "returnBasisUsdt": position.get("returnBasisUsdt"),
        "openedAt": position.get("openedAt"),
        "realizedPnlUsdt": str(realized_pnl) if realized_pnl is not None else None,
        "estimatedFeeUsdt": str(estimated_fee) if estimated_fee is not None else None,
        "netRealizedPnlUsdt": str(net_realized_pnl) if net_realized_pnl is not None else None,
        "closeSide": close_side,
        "maxProfitPct": position.get("maxProfitPct"),
        "maxProfitAt": position.get("maxProfitAt"),
        "minPnlPct": position.get("minPnlPct"),
        "minPnlAt": position.get("minPnlAt"),
        "lastPnlPct": position.get("lastPnlPct"),
        "lastPnlCheckAt": position.get("lastPnlCheckAt"),
        "auditVersion": 1 if audit else None,
        "audit": audit,
    }


def write_snapshot(path: Path, candidates: list[dict[str, Any]]) -> None:
    snapshot = [
        {
            "rank": item.get("rank"),
            "asset": normalize_asset(item.get("asset", ""), item.get("baseAsset")),
            "rawAsset": item.get("asset"),
            "assetType": item.get("assetType"),
            "score": get_score(item),
            "scoreLabel": item.get("metrics", {})
            .get("sentiment_score", {})
            .get("valueLabel"),
            "newsLabel": item.get("metrics", {})
            .get("sentiment_score_news", {})
            .get("valueLabel"),
            "socialLabel": item.get("metrics", {})
            .get("sentiment_score_social", {})
            .get("valueLabel"),
            "kolLabel": item.get("metrics", {})
            .get("sentiment_score_kol", {})
            .get("valueLabel"),
        }
        for item in candidates
    ]
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def load_snapshot_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def strategy_position_count(state: dict[str, Any], strategy_id: str, side: str) -> int:
    return sum(
        1
        for position in state.get("positions", {}).values()
        if side_from_position(position) == side and position.get("strategyId") == strategy_id
    )


def should_suspend_signal_lost_exit(
    *,
    config: BotConfig,
    previous_snapshot: list[dict[str, Any]],
    current_candidates: list[dict[str, Any]],
    current_position_count: int,
) -> bool:
    if not config.enable_signal_drop_guard:
        return False
    if current_position_count <= 0:
        return False
    previous_count = len(previous_snapshot)
    current_count = len(current_candidates)
    if current_count <= 0:
        return True
    if previous_count <= 0:
        return current_count < config.signal_drop_guard_min_candidates
    if current_count >= previous_count:
        return False
    threshold = max(
        config.signal_drop_guard_min_candidates,
        math.ceil(previous_count * config.signal_drop_guard_ratio),
    )
    return current_count < threshold


def should_preserve_previous_snapshot(
    *,
    previous_snapshot: list[dict[str, Any]],
    current_candidates: list[dict[str, Any]],
    current_position_count: int,
    suspend_signal_lost_exit: bool,
) -> bool:
    if current_position_count <= 0:
        return False
    if not current_candidates and previous_snapshot:
        return True
    return suspend_signal_lost_exit


def count_open_positions_for_side(state: dict[str, Any], side: str) -> int:
    return sum(
        1
        for position in state.get("positions", {}).values()
        if side_from_position(position) == side
    )


def closed_history_for_strategy(
    state: dict[str, Any], strategy_id: str, side: str
) -> list[dict[str, Any]]:
    target_action = exit_action(side)
    return [
        event
        for event in state.get("history", [])
        if event.get("strategyId") == strategy_id and event.get("action") == target_action
    ]


def sync_live_position_into_state(
    *,
    state: dict[str, Any],
    asset: str,
    side: str,
    strategy_id: str,
    contract_symbol: str,
    live_position: dict[str, Any],
    item: dict[str, Any] | None,
    config: BotConfig,
) -> None:
    api_return_basis = extract_api_return_basis(live_position)
    synced_position = {
        "entryPrice": live_position.get("entryPrice"),
        "notionalUsdt": abs(float(live_position.get("notional", "0"))),
        "quantity": abs(float(live_position.get("positionAmt", "0"))),
        "unRealizedProfit": live_position.get("unRealizedProfit"),
        "returnBasisUsdt": str(api_return_basis) if api_return_basis is not None else None,
        "positionInitialMargin": live_position.get("positionInitialMargin"),
        "initialMargin": live_position.get("initialMargin"),
        "isolatedWallet": live_position.get("isolatedWallet"),
        "isolatedMargin": live_position.get("isolatedMargin"),
        "leverage": live_position.get("leverage") or config.leverage,
        "side": side,
    }
    current_pnl_pct = calculate_unrealized_pnl_pct(
        synced_position, live_position.get("markPrice")
    )
    max_profit_pct = (
        current_pnl_pct
        if current_pnl_pct is not None and current_pnl_pct > Decimal("0")
        else Decimal("0")
    )
    state.setdefault("positions", {})[position_key(asset, side)] = {
        "asset": asset,
        "contractSymbol": contract_symbol,
        "openedAt": time.time(),
        "status": "FILLED",
        "quantity": abs(float(live_position.get("positionAmt", "0"))),
        "entryPrice": live_position.get("entryPrice"),
        "rank": item.get("rank") if item else None,
        "score": get_score(item) if item else None,
        "notionalUsdt": abs(float(live_position.get("notional", "0"))),
        "dryRun": False,
        "requiredMarginMode": config.required_margin_mode,
        "leverage": live_position.get("leverage") or config.leverage,
        "returnBasisUsdt": str(api_return_basis) if api_return_basis is not None else None,
        "positionInitialMargin": live_position.get("positionInitialMargin"),
        "initialMargin": live_position.get("initialMargin"),
        "isolatedWallet": live_position.get("isolatedWallet"),
        "isolatedMargin": live_position.get("isolatedMargin"),
        "unRealizedProfit": live_position.get("unRealizedProfit"),
        "maxProfitPct": format(max_profit_pct, "f"),
        "minPnlPct": format(current_pnl_pct, "f")
        if current_pnl_pct is not None
        else None,
        "minPnlAt": time.time(),
        "lastPnlPct": format(current_pnl_pct, "f")
        if current_pnl_pct is not None
        else None,
        "signalLostRounds": 0,
        "side": side,
        "strategyId": strategy_id,
    }


def is_close_result_success(close_result: dict[str, Any]) -> bool:
    return bool(close_result.get("confirmedClosed"))


def build_close_failed_decision(
    *,
    position: dict[str, Any],
    side: str,
    attempted_reason: str,
    close_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "asset": position["asset"],
        "side": side,
        "action": "hold",
        "reason": "close_failed",
        "attemptedReason": attempted_reason,
        "contractSymbol": position["contractSymbol"],
        "status": close_result.get("status"),
        "detail": close_result.get("error"),
    }


def process_strategy(
    *,
    config: BotConfig,
    broker: BrokerAdapter,
    futures_catalog: BinanceFuturesCatalog,
    state: dict[str, Any],
    strategy_id: str,
    side: str,
    candidates: list[dict[str, Any]],
    suspend_signal_lost_exit: bool = False,
    previous_snapshot: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    strategy_config = get_strategy_config(config.strategy_config_file, strategy_id)
    if not strategy_config.get("enabled", True):
        write_strategy_status(
            config.strategy_status_file,
            strategy_id,
            {
                "strategyId": strategy_id,
                "name": strategy_config.get("name", strategy_id),
                "category": strategy_config.get("category"),
                "enabled": False,
                "status": "disabled",
                "side": side,
                "candidateCount": 0,
                "openedCount": 0,
                "closedCount": 0,
                "closedWinCount": 0,
                "closedLossCount": 0,
                "closedFlatCount": 0,
                "realizedPnlUsdt": "0",
                "latestDecisions": [],
            },
        )
        return {
            "strategyId": strategy_id,
            "side": side,
            "candidateCount": 0,
            "openedCount": 0,
            "closedCount": 0,
            "decisions": [],
            "status": "disabled",
        }

    fee_rate = Decimal(str(config.estimated_taker_fee_rate))
    candidate_count = len(candidates)
    candidate_assets = {
        normalize_asset(item.get("asset", ""), item.get("baseAsset")) for item in candidates
    }
    previous_snapshot_assets = {
        row.get("asset") for row in (previous_snapshot or []) if row.get("asset")
    }

    decisions: list[dict[str, Any]] = []
    opened = 0
    closed = 0
    account_snapshot_cache: dict[str, Any] | None = None

    current_positions = {
        key: position
        for key, position in state.get("positions", {}).items()
        if side_from_position(position) == side and position.get("strategyId") == strategy_id
    }

    for key, position in list(current_positions.items()):
        if config.dry_run or broker.name != "binance_testnet":
            continue
        live_position = broker.get_live_position(position["contractSymbol"], side)
        if live_position is not None:
            continue
        close_result = {
            "status": "POSITION_MISSING",
            "exitPrice": format(broker.get_mark_price(position["contractSymbol"]), "f"),
        }
        exit_audit = build_exit_audit_record(
            config=config,
            position=position,
            reason="exchange_position_missing",
        )
        exit_event = build_exit_record(
            asset=position["asset"],
            side=side,
            strategy_id=strategy_id,
            position=position,
            close_result=close_result,
            reason="exchange_position_missing",
            fee_rate=fee_rate,
            audit=exit_audit,
        )
        state.setdefault("history", []).append(exit_event)
        state.get("positions", {}).pop(key, None)
        decisions.append(
            {
                "asset": position["asset"],
                "side": side,
                "action": exit_action(side),
                "contractSymbol": position["contractSymbol"],
                "status": close_result["status"],
                "reason": "exchange_position_missing",
                "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                "closeSide": exit_event["closeSide"],
            }
        )
        closed += 1

    current_positions = {
        key: position
        for key, position in state.get("positions", {}).items()
        if side_from_position(position) == side and position.get("strategyId") == strategy_id
    }
    for key, position in list(current_positions.items()):
        position["signalLostRounds"] = int(position.get("signalLostRounds", 0) or 0)
        live_position = None
        mark_price_raw = None
        if broker.name == "binance_testnet" and not config.dry_run:
            live_position = broker.get_live_position(position["contractSymbol"], side)
            if live_position is not None:
                mark_price_raw = live_position.get("markPrice")
                api_return_basis = extract_api_return_basis(live_position)
                position["entryPrice"] = live_position.get("entryPrice") or position.get("entryPrice")
                position["quantity"] = abs(float(live_position.get("positionAmt", "0")))
                position["notionalUsdt"] = abs(float(live_position.get("notional", "0")))
                position["leverage"] = live_position.get("leverage") or position.get("leverage")
                position["unRealizedProfit"] = live_position.get("unRealizedProfit")
                position["positionInitialMargin"] = live_position.get("positionInitialMargin")
                position["initialMargin"] = live_position.get("initialMargin")
                position["isolatedWallet"] = live_position.get("isolatedWallet")
                position["isolatedMargin"] = live_position.get("isolatedMargin")
                if api_return_basis is not None:
                    position["returnBasisUsdt"] = str(api_return_basis)
        if mark_price_raw in (None, ""):
            mark_price_raw = format(broker.get_mark_price(position["contractSymbol"]), "f")

        tracking = update_profit_tracking(
            position,
            calculate_unrealized_pnl_pct(position, mark_price_raw),
        )
        if config.enable_stop_loss:
            stop_loss_source_position = dict(position)
            if live_position is not None:
                stop_loss_source_position.update(live_position)
            try:
                stop_loss_result = broker.ensure_stop_loss(
                    contract_symbol=position["contractSymbol"],
                    side=side,
                    position=stop_loss_source_position,
                    stop_loss_pct=config.stop_loss_pct,
                    dry_run=config.dry_run,
                )
            except Exception as exc:
                logging.exception(
                    "stop_loss_setup_failed asset=%s symbol=%s side=%s error=%s",
                    position["asset"],
                    position["contractSymbol"],
                    side,
                    exc,
                )
                stop_loss_result = {
                    "orderId": None,
                    "status": "STOP_LOSS_SETUP_FAILED",
                    "configured": False,
                    "stopPrice": position.get("stopLossPrice"),
                    "stopLossPct": format_decimal_value(config.stop_loss_pct),
                }
        else:
            stop_loss_result = None
        update_position_stop_loss_state(
            position=position,
            config=config,
            stop_loss_result=stop_loss_result,
        )
        if should_trigger_stop_loss(
            config=config,
            current_pnl_pct=tracking["current"],
        ):
            exit_audit = build_exit_audit_record(
                config=config,
                position=position,
                reason="stop_loss",
                tracking=tracking,
            )
            close_result = broker.close_position(
                contract_symbol=position["contractSymbol"],
                asset=position["asset"],
                side=side,
                position=position,
                dry_run=config.dry_run,
            )
            if not is_close_result_success(close_result):
                decisions.append(
                    build_close_failed_decision(
                        position=position,
                        side=side,
                        attempted_reason="stop_loss",
                        close_result=close_result,
                    )
                )
                continue
            exit_event = build_exit_record(
                asset=position["asset"],
                side=side,
                strategy_id=strategy_id,
                position=position,
                close_result=close_result,
                reason="stop_loss",
                fee_rate=fee_rate,
                audit=exit_audit,
            )
            state.setdefault("history", []).append(exit_event)
            state.get("positions", {}).pop(key, None)
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": exit_action(side),
                    "contractSymbol": position["contractSymbol"],
                    "status": close_result["status"],
                    "reason": "stop_loss",
                    "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                    "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                    "closeSide": exit_event["closeSide"],
                    "currentPnlPct": format(tracking["current"], "f")
                    if tracking["current"] is not None
                    else None,
                }
            )
            closed += 1
            continue
        if should_trigger_profit_lock(
            config=config,
            position=position,
            current_pnl_pct=tracking["current"],
            peak_pnl_pct=tracking["peak"],
        ):
            exit_audit = build_exit_audit_record(
                config=config,
                position=position,
                reason="profit_lock",
                tracking=tracking,
            )
            close_result = broker.close_position(
                contract_symbol=position["contractSymbol"],
                asset=position["asset"],
                side=side,
                position=position,
                dry_run=config.dry_run,
            )
            if not is_close_result_success(close_result):
                decisions.append(
                    build_close_failed_decision(
                        position=position,
                        side=side,
                        attempted_reason="profit_lock",
                        close_result=close_result,
                    )
                )
                continue
            exit_event = build_exit_record(
                asset=position["asset"],
                side=side,
                strategy_id=strategy_id,
                position=position,
                close_result=close_result,
                reason="profit_lock",
                fee_rate=fee_rate,
                audit=exit_audit,
            )
            state.setdefault("history", []).append(exit_event)
            state.get("positions", {}).pop(key, None)
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": exit_action(side),
                    "contractSymbol": position["contractSymbol"],
                    "status": close_result["status"],
                    "reason": "profit_lock",
                    "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                    "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                    "closeSide": exit_event["closeSide"],
                }
            )
            closed += 1
            continue
        if should_trigger_profit_protection(
            config=config,
            position=position,
            current_pnl_pct=tracking["current"],
            peak_pnl_pct=tracking["peak"],
        ):
            exit_audit = build_exit_audit_record(
                config=config,
                position=position,
                reason="profit_retrace",
                tracking=tracking,
            )
            close_result = broker.close_position(
                contract_symbol=position["contractSymbol"],
                asset=position["asset"],
                side=side,
                position=position,
                dry_run=config.dry_run,
            )
            if not is_close_result_success(close_result):
                decisions.append(
                    build_close_failed_decision(
                        position=position,
                        side=side,
                        attempted_reason="profit_retrace",
                        close_result=close_result,
                    )
                )
                continue
            exit_event = build_exit_record(
                asset=position["asset"],
                side=side,
                strategy_id=strategy_id,
                position=position,
                close_result=close_result,
                reason="profit_retrace",
                fee_rate=fee_rate,
                audit=exit_audit,
            )
            state.setdefault("history", []).append(exit_event)
            state.get("positions", {}).pop(key, None)
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": exit_action(side),
                    "contractSymbol": position["contractSymbol"],
                    "status": close_result["status"],
                    "reason": "profit_retrace",
                    "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                    "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                    "closeSide": exit_event["closeSide"],
                    "peakPnlPct": format(tracking["peak"], "f")
                    if tracking["peak"] is not None
                    else None,
                    "currentPnlPct": format(tracking["current"], "f")
                    if tracking["current"] is not None
                    else None,
                    "drawdownPct": format(tracking["drawdown"], "f")
                    if tracking["drawdown"] is not None
                    else None,
                }
            )
            closed += 1
            continue
        if should_trigger_time_exit(
            config=config,
            position=position,
            current_pnl_pct=tracking["current"],
        ):
            exit_audit = build_exit_audit_record(
                config=config,
                position=position,
                reason="time_exit",
                tracking=tracking,
            )
            close_result = broker.close_position(
                contract_symbol=position["contractSymbol"],
                asset=position["asset"],
                side=side,
                position=position,
                dry_run=config.dry_run,
            )
            if not is_close_result_success(close_result):
                decisions.append(
                    build_close_failed_decision(
                        position=position,
                        side=side,
                        attempted_reason="time_exit",
                        close_result=close_result,
                    )
                )
                continue
            exit_event = build_exit_record(
                asset=position["asset"],
                side=side,
                strategy_id=strategy_id,
                position=position,
                close_result=close_result,
                reason="time_exit",
                fee_rate=fee_rate,
                audit=exit_audit,
            )
            state.setdefault("history", []).append(exit_event)
            state.get("positions", {}).pop(key, None)
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": exit_action(side),
                    "contractSymbol": position["contractSymbol"],
                    "status": close_result["status"],
                    "reason": "time_exit",
                    "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                    "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                    "closeSide": exit_event["closeSide"],
                }
            )
            closed += 1
            continue

        if position["asset"] in candidate_assets:
            position["signalLostRounds"] = 0
            continue
        if suspend_signal_lost_exit:
            position["signalLostRounds"] = 0
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": "hold",
                    "reason": "signal_drop_guard",
                }
            )
            continue
        if position["asset"] in previous_snapshot_assets:
            position["signalLostRounds"] = 0
            logging.warning(
                "signal_lost_blocked asset=%s side=%s reason=still_in_previous_snapshot "
                "candidate_assets=%s previous_snapshot_assets=%s",
                position["asset"],
                side,
                sorted(candidate_assets),
                sorted(previous_snapshot_assets),
            )
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": "hold",
                    "reason": "snapshot_protection",
                }
            )
            continue
        position["signalLostRounds"] += 1
        if position["signalLostRounds"] < config.signal_lost_exit_confirm_rounds:
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": "hold",
                    "reason": "signal_lost_pending",
                    "signalLostRounds": position["signalLostRounds"],
                    "signalLostConfirmRounds": config.signal_lost_exit_confirm_rounds,
                }
            )
            continue
        logging.info(
            "signal_lost_closing asset=%s side=%s rounds=%s candidate_assets=%s",
            position["asset"],
            side,
            position["signalLostRounds"],
            sorted(candidate_assets),
        )
        close_result = broker.close_position(
            contract_symbol=position["contractSymbol"],
            asset=position["asset"],
            side=side,
            position=position,
            dry_run=config.dry_run,
        )
        if not is_close_result_success(close_result):
            decisions.append(
                build_close_failed_decision(
                    position=position,
                    side=side,
                    attempted_reason="signal_lost",
                    close_result=close_result,
                )
            )
            continue
        exit_event = build_exit_record(
            asset=position["asset"],
            side=side,
            strategy_id=strategy_id,
            position=position,
            close_result=close_result,
            reason="signal_lost",
            fee_rate=fee_rate,
            audit=build_exit_audit_record(
                config=config,
                position=position,
                reason="signal_lost",
                tracking=tracking,
            ),
        )
        state.setdefault("history", []).append(exit_event)
        state.get("positions", {}).pop(key, None)
        decisions.append(
            {
                "asset": position["asset"],
                "side": side,
                "action": exit_action(side),
                "contractSymbol": position["contractSymbol"],
                "status": close_result["status"],
                "reason": "signal_lost",
                "signalLostRounds": position["signalLostRounds"],
                "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                "closeSide": exit_event["closeSide"],
            }
        )
        closed += 1

    for item in candidates:
        asset = normalize_asset(item.get("asset", ""), item.get("baseAsset"))
        contract = futures_catalog.get_contract(asset)
        if not contract:
            decisions.append(
                {"asset": asset, "side": side, "action": "skip", "reason": "no_usdt_perpetual"}
            )
            continue

        margin_usage_pct = None
        max_range_pct = None
        funding_rate_pct = None
        trend_signal = None
        correlated_symbol = None
        correlated_value = None

        if broker.has_open_position(state, asset, side):
            key = position_key(asset, side)
            if key not in state.get("positions", {}) and broker.name == "binance_testnet":
                live_position = broker.get_live_position(contract["symbol"], side)
                if live_position is not None:
                    sync_live_position_into_state(
                        state=state,
                        asset=asset,
                        side=side,
                        strategy_id=strategy_id,
                        contract_symbol=contract["symbol"],
                        live_position=live_position,
                        item=item,
                        config=config,
                    )
                    if config.enable_stop_loss:
                        synced_position = state.get("positions", {}).get(key)
                        if isinstance(synced_position, dict):
                            stop_loss_result = None
                            try:
                                stop_loss_result = broker.ensure_stop_loss(
                                    contract_symbol=contract["symbol"],
                                    side=side,
                                    position={**synced_position, **live_position},
                                    stop_loss_pct=config.stop_loss_pct,
                                    dry_run=config.dry_run,
                                )
                            except Exception as exc:
                                logging.exception(
                                    "synced_position_stop_loss_failed asset=%s symbol=%s side=%s error=%s",
                                    asset,
                                    contract["symbol"],
                                    side,
                                    exc,
                                )
                                stop_loss_result = {
                                    "orderId": None,
                                    "status": "STOP_LOSS_SETUP_FAILED",
                                    "configured": False,
                                    "stopPrice": synced_position.get("stopLossPrice"),
                                    "stopLossPct": format_decimal_value(config.stop_loss_pct),
                                }
                            update_position_stop_loss_state(
                                position=synced_position,
                                config=config,
                                stop_loss_result=stop_loss_result,
                            )
            decisions.append(
                {"asset": asset, "side": side, "action": "hold", "reason": signal_hold_reason(side)}
            )
            continue

        if (
            config.enable_min_signal_count_filter
            and candidate_count < config.min_signal_count_to_open
        ):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "signal_count_too_low",
                    "currentSignalCount": candidate_count,
                    "minSignalCountToOpen": config.min_signal_count_to_open,
                }
            )
            continue

        if is_in_cooldown(state, asset, side, config.cooldown_minutes):
            decisions.append({"asset": asset, "side": side, "action": "skip", "reason": "cooldown"})
            continue

        margin_modes = broker.supported_margin_modes(contract["symbol"])
        if (
            config.skip_if_margin_mode_unavailable
            and config.required_margin_mode not in margin_modes
        ):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": f"margin_mode_missing:{config.required_margin_mode}",
                }
            )
            continue

        if config.enable_margin_usage_cap:
            if account_snapshot_cache is None:
                account_snapshot_cache = broker.get_account_snapshot()
            margin_usage_pct = current_margin_usage_pct(account_snapshot_cache)
            if (
                margin_usage_pct is not None
                and margin_usage_pct >= Decimal(str(config.max_margin_usage_pct))
            ):
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "margin_usage_limit",
                        "marginUsagePct": format(margin_usage_pct, "f"),
                    }
                )
                continue

        quote_volume_24h = broker.get_quote_volume_24h(contract["symbol"])
        if quote_volume_24h < Decimal(str(config.min_quote_volume_24h_usdt)):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "low_24h_quote_volume",
                    "quoteVolume24hUsdt": format(quote_volume_24h, "f"),
                    "minRequiredQuoteVolume24hUsdt": format(
                        Decimal(str(config.min_quote_volume_24h_usdt)), "f"
                    ),
                }
            )
            continue

        if config.enable_volatility_filter:
            volatility_klines = broker.get_klines(
                contract["symbol"],
                config.volatility_interval,
                max(2, config.volatility_lookback_bars),
            )
            max_range_pct = candle_max_range_pct(volatility_klines)
            if max_range_pct > Decimal(str(config.max_single_bar_range_pct)):
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "high_volatility",
                        "maxRangePct": format(max_range_pct, "f"),
                    }
                )
                continue

        if config.enable_funding_rate_filter:
            funding_rate_pct = abs(broker.get_last_funding_rate_pct(contract["symbol"]))
            if funding_rate_pct > Decimal(str(config.max_abs_funding_rate_pct)):
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "funding_too_high",
                        "fundingRatePct": format(funding_rate_pct, "f"),
                    }
                )
                continue

        if config.enable_trend_confirmation:
            trend_signal = resolve_trend_confirmation(
                broker=broker,
                contract_symbol=contract["symbol"],
                config=config,
            )
            if trend_signal is None:
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "trend_data_unavailable",
                        "trendIntervalsTried": ",".join(
                            [config.trend_interval, *config.trend_fallback_intervals]
                        ),
                    }
                )
                continue
            ma_value = trend_signal["ma"]
            current_close = trend_signal["close"]
            trend_interval_used = trend_signal["interval"]
            if side == LONG and current_close <= ma_value:
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "trend_not_confirmed",
                        "close": format(current_close, "f"),
                        "ma": format(ma_value, "f"),
                        "trendInterval": trend_interval_used,
                    }
                )
                continue
            if side == SHORT and current_close >= ma_value:
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "trend_not_confirmed",
                        "close": format(current_close, "f"),
                        "ma": format(ma_value, "f"),
                        "trendInterval": trend_interval_used,
                    }
                )
                continue

        if config.enable_correlation_filter:
            candidate_klines = broker.get_klines(
                contract["symbol"],
                config.correlation_interval,
                max(3, config.correlation_lookback_bars),
            )
            candidate_returns = returns_from_closes(kline_closes(candidate_klines))
            correlated_symbol = None
            correlated_value = None
            for existing_position in state.get("positions", {}).values():
                if side_from_position(existing_position) != side:
                    continue
                existing_symbol = existing_position.get("contractSymbol")
                if not existing_symbol or existing_symbol == contract["symbol"]:
                    continue
                existing_klines = broker.get_klines(
                    existing_symbol,
                    config.correlation_interval,
                    max(3, config.correlation_lookback_bars),
                )
                corr = pearson_corr(
                    candidate_returns,
                    returns_from_closes(kline_closes(existing_klines)),
                )
                if corr is None:
                    continue
                if abs(corr) >= config.correlation_threshold:
                    correlated_symbol = existing_symbol
                    correlated_value = corr
                    break
            if correlated_symbol is not None:
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "correlated_with_existing",
                        "correlatedSymbol": correlated_symbol,
                        "correlation": round(correlated_value, 4),
                    }
                )
                continue

        if opened >= config.max_new_positions_per_cycle:
            decisions.append({"asset": asset, "side": side, "action": "skip", "reason": "cycle_limit"})
            continue
        side_limit = (
            config.max_long_open_positions if side == LONG else config.max_short_open_positions
        )
        side_positions_before = count_open_positions_for_side(state, side)
        total_positions_before = count_total_open_positions(state)
        if side_positions_before >= side_limit:
            decisions.append({"asset": asset, "side": side, "action": "skip", "reason": "side_limit"})
            continue
        if total_positions_before >= config.max_total_open_positions:
            decisions.append({"asset": asset, "side": side, "action": "skip", "reason": "portfolio_limit"})
            continue

        entry_audit = build_entry_audit_record(
            config=config,
            state=state,
            side=side,
            candidate_count=candidate_count,
            opened_before=opened,
            side_positions_before=side_positions_before,
            total_positions_before=total_positions_before,
            margin_usage_pct=margin_usage_pct,
            quote_volume_24h=quote_volume_24h,
            max_range_pct=max_range_pct,
            funding_rate_pct=funding_rate_pct,
            trend_signal=trend_signal,
            correlated_symbol=correlated_symbol,
            correlated_value=correlated_value,
        )

        metadata = {
            "rank": item.get("rank"),
            "score": get_score(item),
            "scoreLabel": get_score_label(item),
            "signalSource": "binance_ai_select",
            "interval": config.interval,
            "assetType": item.get("assetType"),
            "quoteVolume24hUsdt": format(quote_volume_24h, "f"),
            "requiredMarginMode": config.required_margin_mode,
            "leverage": config.leverage,
            "side": side,
        }
        try:
            if side == LONG:
                order_result = broker.place_long_market_order(
                    contract_symbol=contract["symbol"],
                    asset=asset,
                    notional_usdt=config.usdt_per_trade,
                    metadata=metadata,
                    dry_run=config.dry_run,
                )
            else:
                order_result = broker.place_short_market_order(
                    contract_symbol=contract["symbol"],
                    asset=asset,
                    notional_usdt=config.usdt_per_trade,
                    metadata=metadata,
                    dry_run=config.dry_run,
                )
        except Exception as exc:
            logging.exception(
                "order_open_failed asset=%s symbol=%s side=%s error=%s",
                asset,
                contract["symbol"],
                side,
                exc,
            )
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "order_open_failed",
                    "detail": str(exc),
                }
            )
            continue

        opened_position = {
            "asset": asset,
            "contractSymbol": contract["symbol"],
            "openedAt": time.time(),
            "status": order_result["status"],
            "quantity": order_result.get("quantity"),
            "entryPrice": order_result.get("entryPrice"),
            "rank": item.get("rank"),
            "score": get_score(item),
            "notionalUsdt": order_result.get("notionalUsdt", config.usdt_per_trade),
            "dryRun": config.dry_run,
            "requiredMarginMode": config.required_margin_mode,
            "leverage": config.leverage,
            "returnBasisUsdt": None,
            "maxProfitPct": "0",
            "minPnlPct": "0",
            "lastPnlPct": "0",
            "signalLostRounds": 0,
            "side": side,
            "strategyId": strategy_id,
        }
        opened_position["returnBasisUsdt"] = format_decimal_value(
            infer_return_basis_usdt(opened_position)
        )
        if config.enable_stop_loss:
            try:
                stop_loss_result = broker.ensure_stop_loss(
                    contract_symbol=contract["symbol"],
                    side=side,
                    position=opened_position,
                    stop_loss_pct=config.stop_loss_pct,
                    dry_run=config.dry_run,
                )
            except Exception as exc:
                logging.exception(
                    "entry_stop_loss_setup_failed asset=%s symbol=%s side=%s error=%s",
                    asset,
                    contract["symbol"],
                    side,
                    exc,
                )
                stop_loss_result = {
                    "orderId": None,
                    "status": "STOP_LOSS_SETUP_FAILED",
                    "configured": False,
                    "stopPrice": None,
                    "stopLossPct": format_decimal_value(config.stop_loss_pct),
                }
        else:
            stop_loss_result = None
        update_position_stop_loss_state(
            position=opened_position,
            config=config,
            stop_loss_result=stop_loss_result,
        )
        entry_audit = enrich_entry_audit_with_stop_loss(
            audit=entry_audit,
            config=config,
            stop_loss_result=stop_loss_result,
        )
        opened_position["entryAudit"] = entry_audit
        state.setdefault("positions", {})[position_key(asset, side)] = opened_position
        state.setdefault("history", []).append(
            {
                "timestamp": time.time(),
                "asset": asset,
                "contractSymbol": contract["symbol"],
                "side": side,
                "strategyId": strategy_id,
                "action": enter_action(side),
                "status": order_result["status"],
                "reason": signal_enter_reason(side),
                "entryPrice": order_result.get("entryPrice"),
                "quantity": order_result.get("quantity"),
                "stopLossPrice": opened_position.get("stopLossPrice"),
                "stopLossStatus": opened_position.get("stopLossStatus"),
                "auditVersion": 1,
                "audit": entry_audit,
            }
        )
        decisions.append(
            {
                "asset": asset,
                "side": side,
                "action": enter_action(side),
                "contractSymbol": contract["symbol"],
                "status": order_result["status"],
                "score": get_score(item),
                "quantity": order_result.get("quantity"),
                "stopLossPrice": opened_position.get("stopLossPrice"),
                "stopLossStatus": opened_position.get("stopLossStatus"),
            }
        )
        opened += 1
        account_snapshot_cache = None

    strategy_closed_history = closed_history_for_strategy(state, strategy_id, side)
    closed_win_count = sum(1 for event in strategy_closed_history if event.get("closeSide") == "win")
    closed_loss_count = sum(1 for event in strategy_closed_history if event.get("closeSide") == "loss")
    closed_flat_count = sum(1 for event in strategy_closed_history if event.get("closeSide") == "flat")
    realized_pnl_total = sum(
        Decimal(event.get("netRealizedPnlUsdt"))
        for event in strategy_closed_history
        if event.get("netRealizedPnlUsdt") not in (None, "")
    )

    write_strategy_status(
        config.strategy_status_file,
        strategy_id,
        {
            "strategyId": strategy_id,
            "name": strategy_config.get("name", strategy_id),
            "category": strategy_config.get("category"),
            "enabled": True,
            "status": "ok",
            "side": side,
            "candidateCount": len(candidates),
            "openedCount": opened,
            "closedCount": closed,
            "closedWinCount": closed_win_count,
            "closedLossCount": closed_loss_count,
            "closedFlatCount": closed_flat_count,
            "realizedPnlUsdt": str(realized_pnl_total),
            "latestDecisions": decisions,
        },
    )

    return {
        "strategyId": strategy_id,
        "side": side,
        "candidateCount": len(candidates),
        "openedCount": opened,
        "closedCount": closed,
        "closedWinCount": closed_win_count,
        "closedLossCount": closed_loss_count,
        "closedFlatCount": closed_flat_count,
        "realizedPnlUsdt": str(realized_pnl_total),
        "decisions": decisions,
    }


def run_once(config: BotConfig) -> dict[str, Any]:
    broker = select_broker_adapter()
    state_store = StateStore(config.state_file)
    state = migrate_state(state_store.load())
    futures_catalog = BinanceFuturesCatalog(broker.exchange_info_url())
    previous_positive_snapshot = load_snapshot_rows(config.positive_snapshot_file)
    previous_negative_snapshot = load_snapshot_rows(config.negative_snapshot_file)

    positive_candidates, negative_candidates = fetch_signal_assets(config)
    positive_assets = [normalize_asset(item.get("asset", ""), item.get("baseAsset")) for item in positive_candidates]
    negative_assets = [normalize_asset(item.get("asset", ""), item.get("baseAsset")) for item in negative_candidates]
    prev_positive_assets = [row.get("asset") for row in previous_positive_snapshot if row.get("asset")]
    logging.info(
        "signal_fetch_result positive=%d negative=%d positive_assets=%s prev_positive_assets=%s",
        len(positive_candidates),
        len(negative_candidates),
        positive_assets,
        prev_positive_assets,
    )
    suspend_long_signal_lost_exit = should_suspend_signal_lost_exit(
        config=config,
        previous_snapshot=previous_positive_snapshot,
        current_candidates=positive_candidates,
        current_position_count=strategy_position_count(state, LONG_STRATEGY_ID, LONG),
    )
    suspend_short_signal_lost_exit = should_suspend_signal_lost_exit(
        config=config,
        previous_snapshot=previous_negative_snapshot,
        current_candidates=negative_candidates,
        current_position_count=strategy_position_count(state, SHORT_STRATEGY_ID, SHORT),
    )
    if suspend_long_signal_lost_exit:
        logging.warning(
            "signal_drop_guard_active side=%s previous=%s current=%s",
            LONG,
            len(previous_positive_snapshot),
            len(positive_candidates),
        )
    if suspend_short_signal_lost_exit:
        logging.warning(
            "signal_drop_guard_active side=%s previous=%s current=%s",
            SHORT,
            len(previous_negative_snapshot),
            len(negative_candidates),
        )

    preserve_positive_snapshot = should_preserve_previous_snapshot(
        previous_snapshot=previous_positive_snapshot,
        current_candidates=positive_candidates,
        current_position_count=strategy_position_count(state, LONG_STRATEGY_ID, LONG),
        suspend_signal_lost_exit=suspend_long_signal_lost_exit,
    )
    preserve_negative_snapshot = should_preserve_previous_snapshot(
        previous_snapshot=previous_negative_snapshot,
        current_candidates=negative_candidates,
        current_position_count=strategy_position_count(state, SHORT_STRATEGY_ID, SHORT),
        suspend_signal_lost_exit=suspend_short_signal_lost_exit,
    )
    if preserve_positive_snapshot:
        logging.warning(
            "signal_snapshot_preserved side=%s previous=%s current=%s",
            LONG,
            len(previous_positive_snapshot),
            len(positive_candidates),
        )
    else:
        write_snapshot(config.positive_snapshot_file, positive_candidates)
    if preserve_negative_snapshot:
        logging.warning(
            "signal_snapshot_preserved side=%s previous=%s current=%s",
            SHORT,
            len(previous_negative_snapshot),
            len(negative_candidates),
        )
    else:
        write_snapshot(config.negative_snapshot_file, negative_candidates)

    long_result: dict[str, Any] | None = None
    short_result: dict[str, Any] | None = None
    try:
        long_result = process_strategy(
            config=config,
            broker=broker,
            futures_catalog=futures_catalog,
            state=state,
            strategy_id=LONG_STRATEGY_ID,
            side=LONG,
            candidates=positive_candidates,
            suspend_signal_lost_exit=suspend_long_signal_lost_exit,
            previous_snapshot=previous_positive_snapshot,
        )
        state_store.save(state)

        short_result = process_strategy(
            config=config,
            broker=broker,
            futures_catalog=futures_catalog,
            state=state,
            strategy_id=SHORT_STRATEGY_ID,
            side=SHORT,
            candidates=negative_candidates,
            suspend_signal_lost_exit=suspend_short_signal_lost_exit,
            previous_snapshot=previous_negative_snapshot,
        )
        state_store.save(state)
    except Exception:
        # Preserve any already-mutated state so live-vs-local reconciliation and
        # close history are not lost if one side fails mid-cycle.
        state_store.save(state)
        raise

    return {
        "timestamp": time.time(),
        "dryRun": config.dry_run,
        "strategies": {
            LONG_STRATEGY_ID: long_result,
            SHORT_STRATEGY_ID: short_result,
        },
        "openedCount": long_result["openedCount"] + short_result["openedCount"],
        "closedCount": long_result["closedCount"] + short_result["closedCount"],
    }


def build_config(workdir: Path) -> BotConfig:
    logs_dir = workdir / "runtime"
    logs_dir.mkdir(parents=True, exist_ok=True)
    cooldown_minutes_env = os.getenv("COOLDOWN_MINUTES")
    if cooldown_minutes_env not in (None, ""):
        cooldown_minutes = int(cooldown_minutes_env)
    else:
        cooldown_minutes = int(float(os.getenv("COOLDOWN_HOURS", "5")) * 60)
    return BotConfig(
        score_threshold=float(os.getenv("SCORE_THRESHOLD", "6.5")),
        negative_score_threshold=float(os.getenv("NEGATIVE_SCORE_THRESHOLD", "2.5")),
        interval=os.getenv("AI_SELECT_INTERVAL", "1h"),
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "900")),
        dry_run=get_env_bool("DRY_RUN", True),
        quote_asset=os.getenv("QUOTE_ASSET", "USDT"),
        usdt_per_trade=float(os.getenv("USDT_PER_TRADE", "25")),
        max_new_positions_per_cycle=int(os.getenv("MAX_NEW_POSITIONS_PER_CYCLE", "3")),
        max_total_open_positions=int(os.getenv("MAX_TOTAL_OPEN_POSITIONS", "5")),
        max_long_open_positions=int(
            os.getenv("MAX_LONG_OPEN_POSITIONS", os.getenv("MAX_TOTAL_OPEN_POSITIONS", "5"))
        ),
        max_short_open_positions=int(
            os.getenv("MAX_SHORT_OPEN_POSITIONS", os.getenv("MAX_TOTAL_OPEN_POSITIONS", "5"))
        ),
        enable_min_signal_count_filter=get_env_bool("ENABLE_MIN_SIGNAL_COUNT_FILTER", True),
        min_signal_count_to_open=int(os.getenv("MIN_SIGNAL_COUNT_TO_OPEN", "6")),
        cooldown_minutes=cooldown_minutes,
        required_margin_mode=os.getenv("REQUIRED_MARGIN_MODE", "CROSS").upper(),
        skip_if_margin_mode_unavailable=get_env_bool(
            "SKIP_IF_MARGIN_MODE_UNAVAILABLE", True
        ),
        leverage=int(os.getenv("LEVERAGE", "2")),
        min_quote_volume_24h_usdt=float(
            os.getenv("MIN_QUOTE_VOLUME_24H_USDT", "5000000")
        ),
        enable_margin_usage_cap=get_env_bool("ENABLE_MARGIN_USAGE_CAP", True),
        max_margin_usage_pct=float(os.getenv("MAX_MARGIN_USAGE_PCT", "60")),
        enable_volatility_filter=get_env_bool("ENABLE_VOLATILITY_FILTER", True),
        volatility_interval=os.getenv("VOLATILITY_INTERVAL", "1h"),
        volatility_lookback_bars=int(os.getenv("VOLATILITY_LOOKBACK_BARS", "4")),
        max_single_bar_range_pct=float(os.getenv("MAX_SINGLE_BAR_RANGE_PCT", "12")),
        enable_funding_rate_filter=get_env_bool("ENABLE_FUNDING_RATE_FILTER", True),
        max_abs_funding_rate_pct=float(os.getenv("MAX_ABS_FUNDING_RATE_PCT", "0.10")),
        enable_correlation_filter=get_env_bool("ENABLE_CORRELATION_FILTER", True),
        correlation_interval=os.getenv("CORRELATION_INTERVAL", "1h"),
        correlation_lookback_bars=int(os.getenv("CORRELATION_LOOKBACK_BARS", "24")),
        correlation_threshold=float(os.getenv("CORRELATION_THRESHOLD", "0.92")),
        enable_trend_confirmation=get_env_bool("ENABLE_TREND_CONFIRMATION", True),
        trend_interval=os.getenv("TREND_INTERVAL", "4h"),
        trend_ma_period=int(os.getenv("TREND_MA_PERIOD", "20")),
        trend_fallback_intervals=normalize_intervals(
            os.getenv("TREND_FALLBACK_INTERVALS", "1h,15m,5m")
        ),
        enable_time_exit=get_env_bool("ENABLE_TIME_EXIT", True),
        max_hold_hours=int(os.getenv("MAX_HOLD_HOURS", "48")),
        time_exit_min_pnl_pct=float(os.getenv("TIME_EXIT_MIN_PNL_PCT", "5")),
        enable_stop_loss=get_env_bool("ENABLE_STOP_LOSS", True),
        stop_loss_pct=float(os.getenv("STOP_LOSS_PCT", "32")),
        enable_profit_lock=get_env_bool("ENABLE_PROFIT_LOCK", True),
        profit_lock_tiers=os.getenv("PROFIT_LOCK_TIERS", "4:2,8:5,12:8"),
        enable_profit_protection=get_env_bool("ENABLE_PROFIT_PROTECTION", True),
        profit_protection_activate_pct=float(
            os.getenv("PROFIT_PROTECTION_ACTIVATE_PCT", "8")
        ),
        profit_protection_trail_pct=float(
            os.getenv("PROFIT_PROTECTION_TRAIL_PCT", "3")
        ),
        enable_signal_drop_guard=get_env_bool("ENABLE_SIGNAL_DROP_GUARD", True),
        signal_drop_guard_ratio=float(os.getenv("SIGNAL_DROP_GUARD_RATIO", "0.7")),
        signal_drop_guard_min_candidates=int(
            os.getenv("SIGNAL_DROP_GUARD_MIN_CANDIDATES", "5")
        ),
        signal_lost_exit_confirm_rounds=int(
            os.getenv("SIGNAL_LOST_EXIT_CONFIRM_ROUNDS", "3")
        ),
        estimated_taker_fee_rate=float(os.getenv("ESTIMATED_TAKER_FEE_RATE", "0.0005")),
        strategy_config_file=workdir / STRATEGY_CONFIG_FILE,
        strategy_status_file=workdir / STRATEGY_STATUS_FILE,
        state_file=workdir / os.getenv("STATE_FILE", "runtime/state.json"),
        positive_snapshot_file=workdir / os.getenv(
            "SNAPSHOT_FILE", "runtime/strong_positive_snapshot.json"
        ),
        negative_snapshot_file=workdir / os.getenv(
            "NEGATIVE_SNAPSHOT_FILE", "runtime/strong_negative_snapshot.json"
        ),
        log_file=workdir / os.getenv("LOG_FILE", "runtime/bot.log"),
    )


def setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Binance AI Select futures bot.")
    parser.add_argument("--loop", action="store_true", help="Poll continuously.")
    args = parser.parse_args()

    workdir = Path.cwd()
    load_dotenv(workdir / ".env")
    config = build_config(workdir)
    setup_logging(config.log_file)

    while True:
        try:
            load_dotenv(workdir / ".env")
            config = build_config(workdir)
            result = run_once(config)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            logging.info(
                "cycle_complete dry_run=%s long_candidates=%s short_candidates=%s opened=%s closed=%s",
                config.dry_run,
                result["strategies"][LONG_STRATEGY_ID]["candidateCount"],
                result["strategies"][SHORT_STRATEGY_ID]["candidateCount"],
                result["openedCount"],
                result["closedCount"],
            )
        except (RuntimeError, URLError, ValueError) as exc:
            logging.exception("cycle_failed: %s", exc)

        if not args.loop:
            return
        time.sleep(config.poll_interval_seconds)


if __name__ == "__main__":
    main()
