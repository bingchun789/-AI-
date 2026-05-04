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

from fetch_binance_ai_select import fetch_rendered_signal_lists
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
SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS = 3


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
    enable_signal_count_entry_gate: bool
    min_long_signal_count_to_open: int
    min_short_signal_count_to_open: int
    enable_signal_imbalance_filter: bool
    signal_imbalance_min_count: int
    signal_imbalance_ratio: float
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
    enable_signal_lost_exit: bool
    enable_signal_drop_guard: bool
    signal_drop_guard_ratio: float
    signal_drop_guard_min_candidates: int
    signal_lost_exit_confirm_rounds: int
    enable_signal_count_exit: bool
    long_signal_count_to_close_below: int
    short_signal_count_to_close_below: int
    enable_post_entry_weak_exit: bool
    long_weak_exit_start_minutes: int
    long_weak_exit_end_minutes: int
    long_weak_exit_min_peak_pnl_pct: float
    long_weak_exit_signal_drop_count: int
    long_weak_exit_rank_drop: int
    short_weak_exit_start_minutes: int
    short_weak_exit_end_minutes: int
    short_weak_exit_min_peak_pnl_pct: float
    short_weak_exit_signal_drop_count: int
    short_weak_exit_opposite_rebound_count: int
    estimated_taker_fee_rate: float
    enable_account_circuit_breaker: bool
    daily_loss_pause_pct: float
    max_consecutive_losses: int
    max_account_drawdown_pct: float
    circuit_breaker_cooldown_minutes: int
    enable_risk_position_sizing: bool
    risk_per_trade_pct: float
    min_notional_per_trade_usdt: float
    max_notional_per_trade_usdt: float
    enable_portfolio_risk_cap: bool
    max_side_open_risk_pct: float
    max_total_open_risk_pct: float
    max_correlated_positions_per_side: int
    enable_breakeven_stop: bool
    breakeven_trigger_pct: float
    breakeven_buffer_pct: float
    enable_partial_take_profit: bool
    partial_take_profit_trigger_pct: float
    partial_take_profit_close_ratio: float
    strategy_config_file: Path
    strategy_status_file: Path
    state_file: Path
    positive_snapshot_file: Path
    negative_snapshot_file: Path
    log_file: Path
    equity_history_file: Path


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"positions": {}, "history": []}
        return json.loads(self.path.read_text(encoding="utf-8-sig"))

    def save(self, state: dict[str, Any]) -> None:
        write_json_atomic(self.path, state)


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


class BinanceFuturesCatalog:
    def __init__(self, exchange_info_url: str) -> None:
        self.exchange_info_url = exchange_info_url
        self._symbols_by_base_asset: dict[str, dict[str, Any]] | None = None
        self._all_perpetuals_by_base_asset: dict[str, dict[str, Any]] | None = None

    def refresh(self) -> None:
        payload = http_get_json(self.exchange_info_url)
        symbols = {}
        all_perpetuals = {}
        for item in payload.get("symbols", []):
            if item.get("contractType") != "PERPETUAL":
                continue
            if item.get("quoteAsset") != "USDT":
                continue
            base_asset = item["baseAsset"]
            all_perpetuals[base_asset] = item
            if item.get("status") != "TRADING":
                continue
            symbols[base_asset] = item
        self._symbols_by_base_asset = symbols
        self._all_perpetuals_by_base_asset = all_perpetuals

    def get_contract(self, base_asset: str) -> dict[str, Any] | None:
        if self._symbols_by_base_asset is None:
            self.refresh()
        assert self._symbols_by_base_asset is not None
        return self._symbols_by_base_asset.get(base_asset)

    def get_contract_entry(self, base_asset: str) -> dict[str, Any] | None:
        if self._all_perpetuals_by_base_asset is None:
            self.refresh()
        assert self._all_perpetuals_by_base_asset is not None
        return self._all_perpetuals_by_base_asset.get(base_asset)


def position_key(asset: str, side: str) -> str:
    return f"{asset}:{side}"


def enter_action(side: str) -> str:
    return "enter_long" if side == LONG else "enter_short"


def exit_action(side: str) -> str:
    return "exit_long" if side == LONG else "exit_short"


def partial_exit_action(side: str) -> str:
    return "partial_exit_long" if side == LONG else "partial_exit_short"


def signal_enter_reason(side: str) -> str:
    return "strong_positive_signal" if side == LONG else "strong_negative_signal"


def signal_hold_reason(side: str) -> str:
    return "still_strong_positive" if side == LONG else "still_strong_negative"


def side_from_position(position: dict[str, Any]) -> str:
    side = position.get("side")
    if side in {LONG, SHORT}:
        return side
    return LONG


def entry_trade_side_for_position(side: str) -> str:
    return "BUY" if side == LONG else "SELL"


def close_trade_side_for_position(side: str) -> str:
    return "SELL" if side == LONG else "BUY"


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
    risk_state = state.get("riskState")
    state["riskState"] = risk_state if isinstance(risk_state, dict) else {}
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
        stop_price = resolve_effective_stop_loss_price(
            position,
            Decimal(str(stop_loss_pct)),
        )
        return {
            "orderId": None,
            "status": "DRY_RUN_PROTECTED" if dry_run else "STOP_LOSS_UNAVAILABLE",
            "configured": stop_price is not None,
            "stopPrice": format_decimal_value(stop_price),
            "stopLossPct": format_decimal_value(stop_loss_pct),
            "mode": "breakeven" if position.get("stopLossOverridePrice") else "fixed",
        }

    def close_position(
        self,
        *,
        contract_symbol: str,
        asset: str,
        side: str,
        position: dict[str, Any],
        dry_run: bool,
        close_ratio: float | None = None,
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
        close_ratio: float | None = None,
    ) -> dict[str, Any]:
        total_qty = infer_position_quantity(position)
        exit_qty = None
        if total_qty is not None:
            if close_ratio is not None and 0 < float(close_ratio) < 1:
                exit_qty = total_qty * Decimal(str(close_ratio))
            else:
                exit_qty = total_qty
        order_id = f"mock-close-{int(time.time() * 1000)}-{contract_symbol}"
        status = "DRY_RUN_CLOSE_ACCEPTED" if dry_run else "MOCK_CLOSED"
        return {
            "orderId": order_id,
            "status": status,
            "contractSymbol": contract_symbol,
            "asset": asset,
            "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
            "confirmedClosed": close_ratio is None or float(close_ratio) >= 1,
            "closedAtMs": int(time.time() * 1000),
            "closeRetryCount": 0,
            "exitQty": format_decimal_value(exit_qty),
            "partial": close_ratio is not None and 0 < float(close_ratio) < 1,
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
        stop_price = resolve_effective_stop_loss_price(
            position,
            Decimal(str(stop_loss_pct)),
        )
        return {
            "orderId": f"mock-stop-{int(time.time() * 1000)}-{contract_symbol}",
            "status": "DRY_RUN_PROTECTED" if dry_run else "MOCK_PROTECTED",
            "configured": stop_price is not None,
            "stopPrice": format_decimal_value(stop_price),
            "stopLossPct": format_decimal_value(stop_loss_pct),
            "mode": "breakeven" if position.get("stopLossOverridePrice") else "fixed",
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
        # Funding rate is an entry gate, so it must reflect the latest exchange value.
        payload = self._public_get("/fapi/v1/premiumIndex", {"symbol": contract_symbol})
        return Decimal(str(payload.get("lastFundingRate", "0"))) * Decimal("100")

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

    def _quantize_existing_quantity(
        self, contract_symbol: str, quantity: Decimal
    ) -> Decimal:
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
        steps = (quantity / step_size).quantize(Decimal("1"), rounding=ROUND_DOWN)
        normalized = (steps * step_size).normalize()
        if normalized < min_qty:
            raise ValueError(
                f"{contract_symbol} reduce quantity {normalized} is below minQty {min_qty}."
            )
        return normalized

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

    def _cancel_protective_stop_orders(
        self,
        contract_symbol: str,
        side: str,
        keep_order_ids: set[str] | None = None,
    ) -> None:
        order_side = "SELL" if side == LONG else "BUY"
        keep_order_ids = keep_order_ids or set()
        for order in self._list_open_orders(contract_symbol):
            if not self._is_protective_stop_order(order, order_side):
                continue
            order_id = str(order.get("orderId") or "")
            if order_id and order_id in keep_order_ids:
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
            if str(algo_id or "") in keep_order_ids or str(client_algo_id or "") in keep_order_ids:
                continue
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
        close_ratio: float | None = None,
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
        is_partial = close_ratio is not None and 0 < float(close_ratio) < 1
        close_quantity = quantity
        if is_partial:
            close_quantity = self._quantize_existing_quantity(
                contract_symbol,
                quantity * Decimal(str(close_ratio)),
            )
        path = "/fapi/v1/order/test" if dry_run else "/fapi/v1/order"
        retry_count = 0
        protective_stop_cancelled = False
        if not dry_run and not is_partial:
            try:
                self._cancel_protective_stop_orders(contract_symbol, side)
                protective_stop_cancelled = True
            except Exception as exc:
                logging.warning(
                    "stop_loss_cancel_before_close_failed symbol=%s side=%s error=%s",
                    contract_symbol,
                    side,
                    exc,
                )

        def failed_close_result(error: Exception, close_qty: Decimal) -> dict[str, Any]:
            payload = {
                "orderId": f"failed-close-{int(time.time() * 1000)}",
                "status": "CLOSE_REJECTED",
                "contractSymbol": contract_symbol,
                "asset": asset,
                "exitPrice": format(self.get_mark_price(contract_symbol), "f"),
                "confirmedClosed": False,
                "closedAtMs": int(time.time() * 1000),
                "closeRetryCount": retry_count,
                "quantity": format(close_qty, "f"),
                "exitQty": format(close_qty, "f"),
                "error": str(error),
                "partial": is_partial,
            }
            stop_loss_pct_raw = position.get("stopLossPct")
            if protective_stop_cancelled and stop_loss_pct_raw not in (None, ""):
                try:
                    restore_result = self.ensure_stop_loss(
                        contract_symbol=contract_symbol,
                        side=side,
                        position=position,
                        stop_loss_pct=float(stop_loss_pct_raw),
                        dry_run=dry_run,
                    )
                    payload["stopLossRestoreStatus"] = restore_result.get("status")
                    payload["stopLossRestored"] = bool(restore_result.get("configured"))
                except Exception as restore_exc:
                    logging.exception(
                        "stop_loss_restore_after_close_failed symbol=%s side=%s error=%s",
                        contract_symbol,
                        side,
                        restore_exc,
                    )
                    payload["stopLossRestoreStatus"] = "STOP_LOSS_RESTORE_FAILED"
                    payload["stopLossRestored"] = False
                    payload["stopLossRestoreError"] = str(restore_exc)
            return payload

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
            result = submit_close_market(close_quantity)
        except Exception as exc:
            if "-4131" in str(exc):
                try:
                    result = submit_close_limit_ioc(close_quantity)
                except Exception as fallback_exc:
                    return failed_close_result(fallback_exc, close_quantity)
            else:
                return failed_close_result(exc, close_quantity)
        self._position_risks = None
        confirmed_closed = not is_partial
        remaining_quantity = None
        remaining_notional = None
        if not dry_run and not is_partial:
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
                            return failed_close_result(fallback_exc, remaining_amt)
                    else:
                        return failed_close_result(exc, remaining_amt)
                self._position_risks = None
                time.sleep(0.5)
            else:
                remaining_position = self.get_live_position(contract_symbol, side)
                confirmed_closed = remaining_position is None
        elif not dry_run and is_partial:
            remaining_position = self.get_live_position(contract_symbol, side)
            if remaining_position is not None:
                remaining_amt = abs(Decimal(str(remaining_position.get("positionAmt", "0"))))
                if remaining_amt > Decimal("0"):
                    confirmed_closed = False
                    remaining_quantity = format(remaining_amt, "f")
                    remaining_notional = format(
                        abs(Decimal(str(remaining_position.get("notional", "0")))),
                        "f",
                    )
                else:
                    confirmed_closed = True
            else:
                confirmed_closed = True
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
            "quantity": format(close_quantity, "f"),
            "exitQty": format(close_quantity, "f"),
            "partial": is_partial,
            "remainingQuantity": remaining_quantity,
            "remainingNotionalUsdt": remaining_notional,
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
        stop_price = resolve_effective_stop_loss_price(
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
                "mode": "breakeven" if position.get("stopLossOverridePrice") else "fixed",
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
                    "mode": "breakeven" if position.get("stopLossOverridePrice") else "fixed",
                }

        previous_stop_price = (
            self._protective_stop_trigger_price(protective_orders[0])
            if protective_orders
            else None
        )

        def submit_stop_order() -> dict[str, Any]:
            return self._signed_request(
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

        lingering_protective_orders: list[dict[str, Any]] = []
        if protective_orders:
            self._cancel_protective_stop_orders(contract_symbol, side)
            time.sleep(0.2)
            lingering_protective_orders = [
                order
                for order in [
                    *self._list_open_orders(contract_symbol),
                    *self._list_open_algo_orders(contract_symbol),
                ]
                if self._is_protective_stop_order(order, order_side)
            ]

        try:
            result = submit_stop_order()
        except Exception as exc:
            if lingering_protective_orders:
                existing_stop_price = self._protective_stop_trigger_price(
                    lingering_protective_orders[0]
                )
                logging.warning(
                    "stop_loss_replace_failed_old_order_still_present symbol=%s side=%s requested=%s existing=%s error=%s",
                    contract_symbol,
                    side,
                    format(stop_price, "f"),
                    format_decimal_value(existing_stop_price),
                    exc,
                )
                return {
                    "orderId": protective_orders[0].get("orderId")
                    or protective_orders[0].get("algoId")
                    or protective_orders[0].get("clientAlgoId"),
                    "status": "STOP_LOSS_REPLACE_FAILED_OLD_PROTECTED",
                    "configured": True,
                    "stopPrice": format_decimal_value(existing_stop_price),
                    "requestedStopPrice": format(stop_price, "f"),
                    "stopLossPct": format_decimal_value(stop_loss_pct),
                    "mode": position.get("stopLossMode") or "fixed",
                    "error": str(exc),
                }
            if protective_orders:
                logging.error(
                    "stop_loss_replace_failed_after_cancel symbol=%s side=%s requested=%s previous=%s error=%s",
                    contract_symbol,
                    side,
                    format(stop_price, "f"),
                    format_decimal_value(previous_stop_price),
                    exc,
                )
                return {
                    "orderId": None,
                    "status": "STOP_LOSS_REPLACE_FAILED_AFTER_CANCEL",
                    "configured": False,
                    "stopPrice": None,
                    "requestedStopPrice": format(stop_price, "f"),
                    "previousStopPrice": format_decimal_value(previous_stop_price),
                    "stopLossPct": format_decimal_value(stop_loss_pct),
                    "mode": position.get("stopLossMode") or "fixed",
                    "error": str(exc),
                }
            raise

        new_order_ids = {
            str(value)
            for value in (
                result.get("orderId"),
                result.get("algoId"),
                result.get("clientAlgoId"),
            )
            if value not in (None, "")
        }
        if lingering_protective_orders and new_order_ids:
            self._cancel_protective_stop_orders(
                contract_symbol,
                side,
                keep_order_ids=new_order_ids,
            )
        elif lingering_protective_orders:
            logging.warning(
                "stop_loss_new_order_missing_id_old_orders_still_present symbol=%s side=%s requested=%s result=%s",
                contract_symbol,
                side,
                format(stop_price, "f"),
                result,
            )
        return {
            "orderId": result.get("orderId") or result.get("algoId") or result.get("clientAlgoId"),
            "status": result.get("algoStatus") or result.get("status") or "STOP_MARKET_PLACED",
            "configured": True,
            "stopPrice": format(stop_price, "f"),
            "stopLossPct": format_decimal_value(stop_loss_pct),
            "mode": "breakeven" if position.get("stopLossOverridePrice") else "fixed",
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
    try:
        return float(score)
    except (TypeError, ValueError):
        return 0.0


def get_display_score(item: dict[str, Any]) -> str | None:
    metrics = item.get("metrics", {})
    score = metrics.get("sentiment_score", {}).get("value")
    if score in (None, ""):
        return None
    return str(score)


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


def sort_signal_candidates(
    positive: list[dict[str, Any]],
    negative: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    positive.sort(key=lambda item: (item.get("rank") or 999999, -get_score(item)))
    negative.sort(key=lambda item: (-(item.get("rank") or 0), get_score(item)))
    return positive, negative


def build_signal_candidates_from_assets_payload(
    payload: dict[str, Any],
    config: "BotConfig",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not payload.get("ok"):
        raise RuntimeError(f"AI Select request failed: {payload.get('status')}")
    items = payload.get("json", {}).get("data", {}).get("items", [])
    positive = [item for item in items if is_strong_positive(item, config.score_threshold)]
    negative = [item for item in items if is_strong_negative(item, config.negative_score_threshold)]
    return sort_signal_candidates(positive, negative)


def is_signal_count_severe_collapse(
    *,
    previous_count: int,
    current_count: int,
    min_candidates: int,
) -> bool:
    if previous_count <= 0:
        return False
    if current_count <= 0:
        return True
    if current_count >= previous_count:
        return False
    if previous_count < max(10, int(min_candidates or 0)):
        return False
    severe_threshold = max(5, math.floor(previous_count * Decimal("0.30")))
    return current_count <= severe_threshold


def find_rendered_signal_issues(
    *,
    rendered_positive: list[dict[str, Any]],
    rendered_negative: list[dict[str, Any]],
    previous_positive_snapshot: list[dict[str, Any]],
    previous_negative_snapshot: list[dict[str, Any]],
    config: "BotConfig",
    source_item_count: int | None = None,
) -> list[str]:
    issues: list[str] = []
    if not rendered_positive and not rendered_negative:
        issues.append("source_empty")
        return issues
    positive_count = len(rendered_positive)
    negative_count = len(rendered_negative)
    previous_positive_count = len(previous_positive_snapshot)
    previous_negative_count = len(previous_negative_snapshot)

    if is_signal_count_severe_collapse(
        previous_count=previous_positive_count,
        current_count=positive_count,
        min_candidates=config.signal_drop_guard_min_candidates,
    ):
        issues.append(
            f"positive_collapse:{positive_count}/{previous_positive_count}"
        )
    if is_signal_count_severe_collapse(
        previous_count=previous_negative_count,
        current_count=negative_count,
        min_candidates=config.signal_drop_guard_min_candidates,
    ):
        issues.append(
            f"negative_collapse:{negative_count}/{previous_negative_count}"
        )
    return issues


def fetch_signal_assets(
    config: BotConfig,
    *,
    previous_positive_snapshot: list[dict[str, Any]] | None = None,
    previous_negative_snapshot: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """Return (strong_positive, strong_negative) candidate lists."""
    previous_positive_snapshot = previous_positive_snapshot or []
    previous_negative_snapshot = previous_negative_snapshot or []
    rendered_positive: list[dict[str, Any]] = []
    rendered_negative: list[dict[str, Any]] = []
    rendered_issues: list[str] = []
    rendered_fetch_errors: list[str] = []
    source_item_count = 0

    for attempt in range(1, 6):
        try:
            rendered_payload = fetch_rendered_signal_lists(config.interval)
            if not rendered_payload.get("ok"):
                rendered_fetch_errors = [f"rendered_table_http_{rendered_payload.get('status')}"]
                if attempt < 5:
                    time.sleep(2)
                continue
            data = rendered_payload.get("json", {}).get("data", {})
            source_item_count = int(data.get("scannedRowCount") or 0)
            rendered_positive = data.get("positiveItems", []) or []
            rendered_negative = data.get("negativeItems", []) or []
            rendered_positive, rendered_negative = sort_signal_candidates(
                rendered_positive,
                rendered_negative,
            )
            rendered_fetch_errors = []
            rendered_issues = find_rendered_signal_issues(
                rendered_positive=rendered_positive,
                rendered_negative=rendered_negative,
                previous_positive_snapshot=previous_positive_snapshot,
                previous_negative_snapshot=previous_negative_snapshot,
                config=config,
                source_item_count=source_item_count,
            )
            if (rendered_positive or rendered_negative) and not rendered_issues:
                return rendered_positive, rendered_negative, {
                    "source": "rendered_table",
                    "renderedIssues": [],
                    "blockNewEntries": False,
                    "freezeSignalDecisions": False,
                }
            if rendered_issues and attempt < 5:
                logging.warning(
                    "signal_assets_suspect_retry attempt=%s issues=%s source_items=%s rendered_positive=%s rendered_negative=%s",
                    attempt,
                    rendered_issues,
                    source_item_count,
                    len(rendered_positive),
                    len(rendered_negative),
                )
                time.sleep(2)
                continue
            if rendered_issues:
                logging.warning(
                    "signal_assets_suspect issues=%s source_items=%s rendered_positive=%s rendered_negative=%s previous_positive=%s previous_negative=%s",
                    rendered_issues,
                    source_item_count,
                    len(rendered_positive),
                    len(rendered_negative),
                    len(previous_positive_snapshot),
                    len(previous_negative_snapshot),
                )
        except Exception as exc:
            logging.warning("signal_assets_fetch_failed attempt=%s error=%s", attempt, exc)
            rendered_fetch_errors = [f"rendered_table_fetch_failed:{type(exc).__name__}"]
            if attempt < 5:
                time.sleep(2)

    if rendered_positive or rendered_negative:
        return rendered_positive, rendered_negative, {
            "source": "rendered_table_invalid",
            "renderedIssues": rendered_issues or rendered_fetch_errors or ["rendered_table_invalid"],
            "blockNewEntries": True,
            "freezeSignalDecisions": True,
        }
    return [], [], {
        "source": "rendered_table_unavailable",
        "renderedIssues": rendered_issues or rendered_fetch_errors or ["rendered_table_unavailable"],
        "blockNewEntries": True,
        "freezeSignalDecisions": True,
    }


def is_signal_imbalance_blocked(
    *,
    config: BotConfig,
    candidate_count: int,
    opposite_candidate_count: int,
) -> bool:
    if not config.enable_signal_imbalance_filter:
        return False
    min_count = int(config.signal_imbalance_min_count or 0)
    ratio = Decimal(str(config.signal_imbalance_ratio))
    if min_count <= 0 or ratio <= Decimal("0"):
        return False
    if candidate_count < min_count or opposite_candidate_count < min_count:
        return False
    if candidate_count <= 0:
        return False
    return Decimal(opposite_candidate_count) >= Decimal(candidate_count) * ratio


def signal_count_entry_threshold_for_side(config: BotConfig, side: str) -> int:
    return (
        int(config.min_long_signal_count_to_open)
        if side == LONG
        else int(config.min_short_signal_count_to_open)
    )


def signal_count_exit_threshold_for_side(config: BotConfig, side: str) -> int:
    return (
        int(config.long_signal_count_to_close_below)
        if side == LONG
        else int(config.short_signal_count_to_close_below)
    )


def update_signal_count_action_confirmation(
    *,
    state: dict[str, Any],
    side: str,
    action: str,
    triggered: bool,
    current_count: int,
    threshold: int,
) -> int:
    confirmations = state.setdefault("signalCountActionConfirmations", {})
    side_state = confirmations.setdefault(side, {})
    key = "entry" if action == "entry" else "exit"
    record = side_state.setdefault(key, {})
    if not triggered:
        record.clear()
        record.update(
            {
                "rounds": 0,
                "currentSignalCount": current_count,
                "threshold": threshold,
                "updatedAt": time.time(),
            }
        )
        return 0
    previous_rounds = int(record.get("rounds", 0) or 0)
    if int(record.get("threshold", threshold) or threshold) != threshold:
        previous_rounds = 0
    rounds = previous_rounds + 1
    record.clear()
    record.update(
        {
            "rounds": rounds,
            "requiredRounds": SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS,
            "currentSignalCount": current_count,
            "threshold": threshold,
            "updatedAt": time.time(),
        }
    )
    return rounds


def int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except Exception:
        return None


def normalize_minute_window(start_minutes: int, end_minutes: int) -> tuple[int, int]:
    start = max(0, int(start_minutes or 0))
    end = max(0, int(end_minutes or 0))
    if start <= end:
        return start, end
    return end, start


def extract_entry_signal_counts(
    side: str,
    entry_audit: dict[str, Any] | None,
) -> tuple[int | None, int | None]:
    if not isinstance(entry_audit, dict):
        return None, None
    same_side_count = int_or_none(entry_audit.get("candidateCount"))
    opposite_count = int_or_none(entry_audit.get("oppositeCandidateCount"))
    if side == LONG:
        return same_side_count, opposite_count
    return same_side_count, opposite_count


def has_met_peak_profit_threshold(
    peak_pnl_pct: Decimal | None,
    threshold_pct: float,
) -> bool:
    if peak_pnl_pct is None:
        return False
    threshold = Decimal(str(threshold_pct))
    if threshold <= Decimal("0"):
        return peak_pnl_pct > Decimal("0")
    return peak_pnl_pct >= threshold


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


def extract_account_equity(account_snapshot: dict[str, Any] | None) -> Decimal | None:
    if not account_snapshot:
        return None
    total_margin_balance = account_snapshot.get("totalMarginBalance")
    if total_margin_balance not in (None, ""):
        return Decimal(str(total_margin_balance))
    total_wallet_balance = account_snapshot.get("totalWalletBalance")
    total_unrealized_profit = account_snapshot.get("totalUnrealizedProfit")
    if total_wallet_balance in (None, ""):
        return None
    equity = Decimal(str(total_wallet_balance))
    if total_unrealized_profit not in (None, ""):
        equity += Decimal(str(total_unrealized_profit))
    return equity


def record_account_equity_snapshot(
    equity_history_file: Path, account_snapshot: dict[str, Any] | None
) -> list[dict[str, Any]]:
    existing = []
    if equity_history_file.exists():
        try:
            payload = json.loads(equity_history_file.read_text(encoding="utf-8-sig"))
            if isinstance(payload, list):
                existing = payload
        except Exception:
            existing = []

    equity = extract_account_equity(account_snapshot)
    now = time.time()
    if equity is not None:
        last_point = existing[-1] if existing else None
        should_append = True
        if isinstance(last_point, dict):
            try:
                last_ts = float(last_point.get("timestamp", 0) or 0)
            except Exception:
                last_ts = 0.0
            try:
                last_equity = Decimal(str(last_point.get("equityUsdt", "0")))
            except Exception:
                last_equity = Decimal("0")
            if now - last_ts < 8 and last_equity == equity:
                should_append = False
        if should_append:
            equity_history_file.parent.mkdir(parents=True, exist_ok=True)
            existing.append(
                {
                    "timestamp": now,
                    "equityUsdt": str(equity),
                    "walletBalanceUsdt": account_snapshot.get("totalWalletBalance")
                    if account_snapshot
                    else None,
                    "unrealizedPnlUsdt": account_snapshot.get("totalUnrealizedProfit")
                    if account_snapshot
                    else None,
                }
            )
            existing = existing[-20000:]
            write_json_atomic(equity_history_file, existing)
    return existing


def local_day_start_timestamp(now: float | None = None) -> float:
    if now is None:
        now = time.time()
    local = time.localtime(now)
    return time.mktime(
        (
            local.tm_year,
            local.tm_mon,
            local.tm_mday,
            0,
            0,
            0,
            local.tm_wday,
            local.tm_yday,
            local.tm_isdst,
        )
    )


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


def clamp_decimal(
    value: Decimal,
    *,
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
) -> Decimal:
    if min_value is not None and value < min_value:
        value = min_value
    if max_value is not None and value > max_value:
        value = max_value
    return value


def estimate_position_max_loss_usdt(
    position: dict[str, Any], stop_loss_pct: Decimal
) -> Decimal | None:
    return_basis = infer_return_basis_usdt(position)
    if return_basis is None or stop_loss_pct <= Decimal("0"):
        return None
    return (return_basis * stop_loss_pct) / Decimal("100")


def estimate_entry_max_loss_usdt(config: BotConfig, notional_usdt: Decimal) -> Decimal | None:
    if notional_usdt <= Decimal("0"):
        return None
    leverage = Decimal(str(config.leverage))
    if leverage <= Decimal("0"):
        return None
    margin_basis = notional_usdt / leverage
    stop_loss_pct = Decimal(str(config.stop_loss_pct))
    if stop_loss_pct <= Decimal("0"):
        return None
    return (margin_basis * stop_loss_pct) / Decimal("100")


def calculate_open_risk_summary(
    state: dict[str, Any], config: BotConfig
) -> dict[str, Decimal]:
    stop_loss_pct = Decimal(str(config.stop_loss_pct))
    total = Decimal("0")
    by_side = {LONG: Decimal("0"), SHORT: Decimal("0")}
    for position in state.get("positions", {}).values():
        if not isinstance(position, dict):
            continue
        side = side_from_position(position)
        risk = estimate_position_max_loss_usdt(position, stop_loss_pct)
        if risk is None:
            continue
        total += risk
        by_side[side] = by_side.get(side, Decimal("0")) + risk
    return {
        "totalRiskUsdt": total,
        "longRiskUsdt": by_side.get(LONG, Decimal("0")),
        "shortRiskUsdt": by_side.get(SHORT, Decimal("0")),
    }


def resolve_entry_notional_usdt(
    *,
    config: BotConfig,
    account_equity: Decimal | None,
) -> dict[str, Decimal | str | None]:
    fixed_notional = Decimal(str(config.usdt_per_trade))
    result: dict[str, Decimal | str | None] = {
        "mode": "fixed",
        "notionalUsdt": fixed_notional,
        "riskBudgetUsdt": None,
        "estimatedRiskUsdt": estimate_entry_max_loss_usdt(config, fixed_notional),
    }
    if (
        not config.enable_risk_position_sizing
        or account_equity is None
        or account_equity <= Decimal("0")
    ):
        return result

    stop_loss_pct = Decimal(str(config.stop_loss_pct))
    leverage = Decimal(str(config.leverage))
    risk_per_trade_pct = Decimal(str(config.risk_per_trade_pct))
    if (
        stop_loss_pct <= Decimal("0")
        or leverage <= Decimal("0")
        or risk_per_trade_pct <= Decimal("0")
    ):
        return result

    risk_budget = (account_equity * risk_per_trade_pct) / Decimal("100")
    margin_budget = (risk_budget * Decimal("100")) / stop_loss_pct
    notional = margin_budget * leverage
    notional = clamp_decimal(
        notional,
        min_value=Decimal(str(config.min_notional_per_trade_usdt)),
        max_value=Decimal(str(config.max_notional_per_trade_usdt)),
    )
    return {
        "mode": "risk_budget",
        "notionalUsdt": notional,
        "riskBudgetUsdt": risk_budget,
        "estimatedRiskUsdt": estimate_entry_max_loss_usdt(config, notional),
    }


def calculate_breakeven_stop_price(
    position: dict[str, Any], breakeven_buffer_pct: Decimal
) -> Decimal | None:
    quantity = infer_position_quantity(position)
    entry_price = decimal_or_none(position.get("entryPrice"))
    return_basis = infer_return_basis_usdt(position)
    if quantity in (None, Decimal("0")) or entry_price is None or return_basis is None:
        return None
    locked_profit_usdt = (return_basis * max(breakeven_buffer_pct, Decimal("0"))) / Decimal("100")
    price_delta = locked_profit_usdt / quantity
    if side_from_position(position) == SHORT:
        stop_price = entry_price - price_delta
    else:
        stop_price = entry_price + price_delta
    if stop_price <= Decimal("0"):
        return None
    return stop_price


def resolve_effective_stop_loss_price(
    position: dict[str, Any], stop_loss_pct: Decimal
) -> Decimal | None:
    override_price = decimal_or_none(position.get("stopLossOverridePrice"))
    if override_price is not None and override_price > Decimal("0"):
        return override_price
    return calculate_stop_loss_price(position, stop_loss_pct)


def should_activate_breakeven_stop(
    *,
    config: BotConfig,
    position: dict[str, Any],
    current_pnl_pct: Decimal | None,
) -> bool:
    if not config.enable_breakeven_stop:
        return False
    if current_pnl_pct is None:
        return False
    if position.get("breakevenActivatedAt") not in (None, ""):
        return False
    return current_pnl_pct >= Decimal(str(config.breakeven_trigger_pct))


def should_trigger_partial_take_profit(
    *,
    config: BotConfig,
    position: dict[str, Any],
    current_pnl_pct: Decimal | None,
) -> bool:
    if not config.enable_partial_take_profit:
        return False
    if current_pnl_pct is None:
        return False
    if position.get("partialTakeProfitDoneAt") not in (None, ""):
        return False
    close_ratio = Decimal(str(config.partial_take_profit_close_ratio))
    if close_ratio <= Decimal("0") or close_ratio >= Decimal("1"):
        return False
    return current_pnl_pct >= Decimal(str(config.partial_take_profit_trigger_pct))


def recent_consecutive_losses(state: dict[str, Any]) -> int:
    streak = 0
    for event in reversed(state.get("history", [])):
        if event.get("action") not in {"exit_long", "exit_short"}:
            continue
        net_pnl = event.get("netRealizedPnlUsdt")
        if net_pnl in (None, ""):
            net_pnl = event.get("realizedPnlUsdt")
        if net_pnl in (None, ""):
            break
        value = Decimal(str(net_pnl))
        if value < 0:
            streak += 1
            continue
        break
    return streak


def evaluate_account_circuit_breaker(
    *,
    config: BotConfig,
    state: dict[str, Any],
    account_snapshot: dict[str, Any] | None,
    equity_history: list[dict[str, Any]],
) -> dict[str, Any]:
    now = time.time()
    current_equity = extract_account_equity(account_snapshot)
    risk_state = state.setdefault("riskState", {})
    stored = risk_state.get("accountCircuitBreaker")
    if not isinstance(stored, dict):
        stored = {}

    day_start = local_day_start_timestamp(now)
    day_open_equity = None
    for point in equity_history:
        if not isinstance(point, dict):
            continue
        timestamp = float(point.get("timestamp", 0) or 0)
        if timestamp < day_start:
            continue
        try:
            day_open_equity = Decimal(str(point.get("equityUsdt", "0")))
        except Exception:
            day_open_equity = None
        break
    if day_open_equity in (None, Decimal("0")):
        day_open_equity = current_equity

    daily_net_realized = Decimal("0")
    for event in state.get("history", []):
        if event.get("action") not in {"exit_long", "exit_short"}:
            continue
        timestamp = float(event.get("timestamp", 0) or 0)
        if timestamp < day_start:
            continue
        net_pnl = event.get("netRealizedPnlUsdt")
        if net_pnl in (None, ""):
            net_pnl = event.get("realizedPnlUsdt")
        if net_pnl in (None, ""):
            continue
        daily_net_realized += Decimal(str(net_pnl))

    daily_loss_pct = None
    if (
        day_open_equity not in (None, Decimal("0"))
        and daily_net_realized < Decimal("0")
    ):
        daily_loss_pct = (abs(daily_net_realized) / day_open_equity) * Decimal("100")

    peak_equity = None
    current_drawdown_pct = None
    if equity_history:
        for point in equity_history:
            if not isinstance(point, dict):
                continue
            try:
                equity = Decimal(str(point.get("equityUsdt", "0")))
            except Exception:
                continue
            if peak_equity is None or equity > peak_equity:
                peak_equity = equity
        if (
            current_equity is not None
            and peak_equity is not None
            and peak_equity > Decimal("0")
        ):
            current_drawdown_pct = ((peak_equity - current_equity) / peak_equity) * Decimal("100")

    consecutive_losses = recent_consecutive_losses(state)
    trigger_reasons: list[str] = []
    if (
        daily_loss_pct is not None
        and daily_loss_pct >= Decimal(str(config.daily_loss_pause_pct))
    ):
        trigger_reasons.append("daily_loss")
    if (
        config.max_consecutive_losses > 0
        and consecutive_losses >= int(config.max_consecutive_losses)
    ):
        trigger_reasons.append("consecutive_losses")
    if (
        current_drawdown_pct is not None
        and current_drawdown_pct >= Decimal(str(config.max_account_drawdown_pct))
    ):
        trigger_reasons.append("account_drawdown")

    until_ts = float(stored.get("until", 0) or 0)
    is_still_paused = until_ts > now
    just_triggered = False
    if config.enable_account_circuit_breaker and trigger_reasons:
        candidate_until = now + max(0, int(config.circuit_breaker_cooldown_minutes)) * 60
        if candidate_until > until_ts:
            until_ts = candidate_until
        is_still_paused = True
        just_triggered = True
    elif not config.enable_account_circuit_breaker:
        until_ts = 0.0
        is_still_paused = False

    result = {
        "enabled": bool(config.enable_account_circuit_breaker),
        "active": bool(config.enable_account_circuit_breaker and is_still_paused),
        "until": until_ts if is_still_paused else None,
        "reasons": trigger_reasons or stored.get("reasons") or [],
        "dailyNetRealizedUsdt": format_decimal_value(daily_net_realized),
        "dailyLossPct": format_decimal_value(daily_loss_pct),
        "dayOpenEquityUsdt": format_decimal_value(day_open_equity),
        "currentEquityUsdt": format_decimal_value(current_equity),
        "peakEquityUsdt": format_decimal_value(peak_equity),
        "currentDrawdownPct": format_decimal_value(current_drawdown_pct),
        "consecutiveLosses": consecutive_losses,
        "cooldownMinutes": int(config.circuit_breaker_cooldown_minutes),
        "justTriggered": just_triggered,
        "updatedAt": now,
    }
    risk_state["accountCircuitBreaker"] = result
    return result


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


def summarize_correlated_positions(
    *,
    broker: BrokerAdapter,
    state: dict[str, Any],
    side: str,
    contract_symbol: str,
    config: BotConfig,
) -> dict[str, Any]:
    candidate_klines = broker.get_klines(
        contract_symbol,
        config.correlation_interval,
        max(3, config.correlation_lookback_bars),
    )
    candidate_returns = returns_from_closes(kline_closes(candidate_klines))
    matches: list[dict[str, Any]] = []
    strongest_match: dict[str, Any] | None = None
    for existing_position in state.get("positions", {}).values():
        if side_from_position(existing_position) != side:
            continue
        existing_symbol = existing_position.get("contractSymbol")
        if not existing_symbol or existing_symbol == contract_symbol:
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
        if (
            strongest_match is None
            or abs(corr) > abs(float(strongest_match["correlation"]))
        ):
            strongest_match = {"symbol": existing_symbol, "correlation": corr}
        if abs(corr) >= config.correlation_threshold:
            matches.append({"symbol": existing_symbol, "correlation": corr})
    return {
        "strongest": strongest_match,
        "matches": matches,
        "matchCount": len(matches),
    }


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
        position["stopLossMode"] = "disabled"
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
    position["stopLossMode"] = (
        stop_loss_result.get("mode") if stop_loss_result else "fixed"
    )
    position["stopLossAttempts"] = (
        stop_loss_result.get("attempts") if stop_loss_result else None
    )
    position["stopLossErrors"] = (
        stop_loss_result.get("errors") if stop_loss_result else []
    )
    position["stopLossUpdatedAt"] = time.time()


def ensure_stop_loss_with_retries(
    *,
    broker: BrokerAdapter,
    contract_symbol: str,
    side: str,
    position: dict[str, Any],
    stop_loss_pct: float,
    dry_run: bool,
    attempts: int = 3,
    retry_delay_seconds: float = 1.0,
    context: str = "",
) -> dict[str, Any]:
    errors: list[str] = []
    last_result: dict[str, Any] | None = None
    total_attempts = max(1, int(attempts or 1))
    for attempt in range(1, total_attempts + 1):
        try:
            result = dict(
                broker.ensure_stop_loss(
                    contract_symbol=contract_symbol,
                    side=side,
                    position=position,
                    stop_loss_pct=stop_loss_pct,
                    dry_run=dry_run,
                )
            )
            result["attempts"] = attempt
            if errors:
                result["errors"] = errors
            last_result = result
            if result.get("configured"):
                return result
            errors.append(
                f"attempt {attempt}: status={result.get('status') or 'unknown'}"
            )
        except Exception as exc:
            errors.append(f"attempt {attempt}: {type(exc).__name__}: {exc}")
            logging.warning(
                "stop_loss_setup_attempt_failed context=%s symbol=%s side=%s attempt=%s/%s error=%s",
                context,
                contract_symbol,
                side,
                attempt,
                total_attempts,
                exc,
            )
        if attempt < total_attempts:
            time.sleep(max(0.0, retry_delay_seconds))

    failed = dict(last_result or {})
    failed.update(
        {
            "orderId": failed.get("orderId"),
            "status": failed.get("status") or "STOP_LOSS_SETUP_FAILED",
            "configured": False,
            "stopPrice": failed.get("stopPrice") or position.get("stopLossPrice"),
            "stopLossPct": format_decimal_value(stop_loss_pct),
            "mode": failed.get("mode") or position.get("stopLossMode") or "fixed",
            "attempts": total_attempts,
            "errors": errors,
        }
    )
    return failed


def is_breakeven_stop_setup_failure(
    position: dict[str, Any],
    stop_loss_result: dict[str, Any] | None,
) -> bool:
    if not stop_loss_result:
        return False
    in_breakeven_mode = (
        position.get("stopLossMode") == "breakeven"
        or position.get("breakevenActivatedAt") not in (None, "")
        or stop_loss_result.get("mode") == "breakeven"
    )
    if not in_breakeven_mode:
        return False
    if stop_loss_result.get("status") == "STOP_LOSS_REPLACE_FAILED_OLD_PROTECTED":
        return True
    if stop_loss_result.get("configured"):
        return False
    return True


def build_entry_audit_record(
    *,
    config: BotConfig,
    state: dict[str, Any],
    side: str,
    candidate_count: int,
    opposite_candidate_count: int,
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
    correlated_match_count: int,
    account_equity: Decimal | None,
    sizing_result: dict[str, Decimal | str | None],
    risk_summary: dict[str, Decimal],
    circuit_breaker: dict[str, Any] | None,
) -> dict[str, Any]:
    side_limit = config.max_long_open_positions if side == LONG else config.max_short_open_positions
    estimated_entry_risk = sizing_result.get("estimatedRiskUsdt")
    side_open_risk = risk_summary["longRiskUsdt"] if side == LONG else risk_summary["shortRiskUsdt"]
    signal_count_entry_threshold = signal_count_entry_threshold_for_side(config, side)
    return {
        "version": 1,
        "candidateCount": candidate_count,
        "oppositeCandidateCount": opposite_candidate_count,
        "minSignalFilterEnabled": bool(config.enable_min_signal_count_filter),
        "minSignalThreshold": int(config.min_signal_count_to_open),
        "signalCountEntryGateEnabled": bool(config.enable_signal_count_entry_gate),
        "signalCountEntryThreshold": signal_count_entry_threshold,
        "signalCountEntryPassed": (
            not config.enable_signal_count_entry_gate
            or candidate_count >= signal_count_entry_threshold
        ),
        "signalImbalanceFilterEnabled": bool(config.enable_signal_imbalance_filter),
        "signalImbalanceMinCount": int(config.signal_imbalance_min_count),
        "signalImbalanceRatio": format_decimal_value(config.signal_imbalance_ratio),
        "signalImbalancePassed": not is_signal_imbalance_blocked(
            config=config,
            candidate_count=candidate_count,
            opposite_candidate_count=opposite_candidate_count,
        ),
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
        "correlationPassed": (
            True
            if not config.enable_correlation_filter
            else int(correlated_match_count or 0) == 0
        ),
        "correlatedSymbol": correlated_symbol,
        "correlation": round(correlated_value, 4) if correlated_value is not None else None,
        "correlatedMatchCount": correlated_match_count,
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
        "accountEquityUsdt": format_decimal_value(account_equity),
        "sizingMode": sizing_result.get("mode"),
        "plannedNotionalUsdt": format_decimal_value(sizing_result.get("notionalUsdt")),
        "riskBudgetUsdt": format_decimal_value(sizing_result.get("riskBudgetUsdt")),
        "estimatedEntryRiskUsdt": format_decimal_value(estimated_entry_risk),
        "portfolioRiskCapEnabled": bool(config.enable_portfolio_risk_cap),
        "openSideRiskUsdt": format_decimal_value(side_open_risk),
        "openTotalRiskUsdt": format_decimal_value(risk_summary["totalRiskUsdt"]),
        "maxSideOpenRiskPct": format_decimal_value(config.max_side_open_risk_pct),
        "maxTotalOpenRiskPct": format_decimal_value(config.max_total_open_risk_pct),
        "maxCorrelatedPositionsPerSide": int(config.max_correlated_positions_per_side),
        "circuitBreakerActive": bool(circuit_breaker and circuit_breaker.get("active")),
        "circuitBreakerReasons": (
            list(circuit_breaker.get("reasons") or []) if circuit_breaker else []
        ),
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
        "stopLossMode": position.get("stopLossMode"),
        "breakevenEnabled": bool(config.enable_breakeven_stop),
        "breakevenTriggerPct": format_decimal_value(config.breakeven_trigger_pct),
        "breakevenBufferPct": format_decimal_value(config.breakeven_buffer_pct),
        "breakevenActivatedAt": position.get("breakevenActivatedAt"),
        "partialTakeProfitEnabled": bool(config.enable_partial_take_profit),
        "partialTakeProfitTriggerPct": format_decimal_value(
            config.partial_take_profit_trigger_pct
        ),
        "partialTakeProfitCloseRatio": format_decimal_value(
            config.partial_take_profit_close_ratio
        ),
        "partialTakeProfitDoneAt": position.get("partialTakeProfitDoneAt"),
        "signalLostExitEnabled": bool(config.enable_signal_lost_exit),
        "signalLostRounds": int(position.get("signalLostRounds", 0) or 0),
        "signalLostConfirmRounds": int(config.signal_lost_exit_confirm_rounds),
        "exchangePositionMissing": reason == "exchange_position_missing",
    }


def enrich_exit_audit_with_signal_counts(
    audit: dict[str, Any],
    *,
    side: str,
    candidate_count: int,
    opposite_candidate_count: int,
) -> dict[str, Any]:
    if side == SHORT:
        long_count = opposite_candidate_count
        short_count = candidate_count
    else:
        long_count = candidate_count
        short_count = opposite_candidate_count
    audit["exitStrongLongCount"] = int(long_count or 0)
    audit["exitStrongShortCount"] = int(short_count or 0)
    audit["exitCandidateCount"] = int(candidate_count or 0)
    audit["exitOppositeCandidateCount"] = int(opposite_candidate_count or 0)
    return audit


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


def reconcile_missing_close_with_exchange(
    *,
    broker: BrokerAdapter,
    position: dict[str, Any],
    now_ms: int | None = None,
) -> dict[str, Any] | None:
    contract_symbol = str(position.get("contractSymbol") or "")
    asset = str(position.get("asset") or contract_symbol.replace("USDT", ""))
    side = side_from_position(position)
    if not contract_symbol or side not in {LONG, SHORT}:
        return None

    if now_ms is None:
        now_ms = int(time.time() * 1000)

    try:
        opened_at_ms = int(float(position.get("openedAt") or time.time()) * 1000)
    except Exception:
        opened_at_ms = now_ms
    start_time_ms = max(0, opened_at_ms - 60_000)

    close_trade_side = close_trade_side_for_position(side)
    entry_trade_side = entry_trade_side_for_position(side)

    try:
        trades = broker.get_user_trades(
            symbol=contract_symbol,
            start_time_ms=start_time_ms,
            end_time_ms=now_ms,
            limit=1000,
        )
    except Exception as exc:
        logging.warning(
            "missing_close_trade_lookup_failed symbol=%s side=%s error=%s",
            contract_symbol,
            side,
            exc,
        )
        return None

    close_groups: dict[str, dict[str, Any]] = {}
    entry_groups: dict[str, dict[str, Any]] = {}
    for trade in trades:
        trade_time_ms = int(trade.get("time", 0) or 0)
        if trade_time_ms < start_time_ms:
            continue
        trade_side = str(trade.get("side", "")).upper()
        order_id = str(trade.get("orderId") or "")
        if not order_id:
            continue
        qty = decimal_or_none(trade.get("qty")) or Decimal("0")
        if qty <= Decimal("0"):
            continue
        price = decimal_or_none(trade.get("price")) or Decimal("0")
        commission = abs(decimal_or_none(trade.get("commission")) or Decimal("0"))

        if trade_side == close_trade_side:
            grouped = close_groups.setdefault(
                order_id,
                {
                    "orderId": order_id,
                    "closedAtMs": trade_time_ms,
                    "exitQty": Decimal("0"),
                    "exitNotional": Decimal("0"),
                    "realizedPnl": Decimal("0"),
                    "commission": Decimal("0"),
                },
            )
            grouped["closedAtMs"] = max(grouped["closedAtMs"], trade_time_ms)
            grouped["exitQty"] += qty
            grouped["exitNotional"] += price * qty
            grouped["realizedPnl"] += decimal_or_none(trade.get("realizedPnl")) or Decimal("0")
            grouped["commission"] += commission
        elif trade_side == entry_trade_side:
            grouped = entry_groups.setdefault(
                order_id,
                {
                    "orderId": order_id,
                    "firstTradeMs": trade_time_ms,
                    "commission": Decimal("0"),
                },
            )
            grouped["firstTradeMs"] = min(grouped["firstTradeMs"], trade_time_ms)
            grouped["commission"] += commission

    if not close_groups:
        return None

    latest_close = max(close_groups.values(), key=lambda row: row["closedAtMs"])
    entry_group = (
        min(entry_groups.values(), key=lambda row: row["firstTradeMs"])
        if entry_groups
        else None
    )

    latest_order = None
    try:
        for order in broker.get_all_orders(
            symbol=contract_symbol,
            start_time_ms=start_time_ms,
            end_time_ms=now_ms,
            limit=500,
        ):
            if str(order.get("orderId") or "") == latest_close["orderId"]:
                latest_order = order
                break
    except Exception as exc:
        logging.warning(
            "missing_close_order_lookup_failed symbol=%s side=%s error=%s",
            contract_symbol,
            side,
            exc,
        )

    force_row = None
    force_start_ms = max(start_time_ms, latest_close["closedAtMs"] - 5 * 60 * 1000)
    force_end_ms = min(now_ms, latest_close["closedAtMs"] + 5 * 60 * 1000)
    if force_end_ms <= force_start_ms:
        force_end_ms = force_start_ms + 1
    try:
        for row in broker.get_force_orders(
            start_time_ms=force_start_ms,
            end_time_ms=force_end_ms,
            limit=100,
        ):
            if str(row.get("symbol") or "") != contract_symbol:
                continue
            if str(row.get("orderId") or "") != latest_close["orderId"]:
                continue
            force_row = row
            break
    except Exception as exc:
        logging.warning(
            "missing_close_force_order_lookup_failed symbol=%s side=%s error=%s",
            contract_symbol,
            side,
            exc,
        )

    total_fee = latest_close["commission"]
    if entry_group is not None:
        total_fee += entry_group["commission"]

    exit_price = format_decimal_value(position.get("entryPrice"))
    if latest_close["exitQty"] != Decimal("0"):
        exit_price = format(latest_close["exitNotional"] / latest_close["exitQty"], "f")

    reason = "exchange_trade"
    if force_row is not None:
        auto_close_type = str(force_row.get("autoCloseType", "")).upper()
        if auto_close_type == "LIQUIDATION":
            reason = "liquidation"
        elif auto_close_type == "ADL":
            reason = "adl"
        else:
            reason = "force_order"
    elif latest_order is not None and str(latest_order.get("closePosition")).lower() == "true":
        reason = "stop_loss"

    realized_pnl = latest_close["realizedPnl"]
    net_realized = realized_pnl - total_fee
    return {
        "orderId": latest_close["orderId"],
        "status": (latest_order.get("status") if latest_order else None) or "FILLED",
        "contractSymbol": contract_symbol,
        "asset": asset,
        "exitPrice": exit_price,
        "confirmedClosed": True,
        "closedAtMs": latest_close["closedAtMs"],
        "closeRetryCount": 0,
        "exitQty": format_decimal_value(latest_close["exitQty"]),
        "realizedPnlUsdt": format_decimal_value(realized_pnl),
        "estimatedFeeUsdt": format_decimal_value(total_fee),
        "netRealizedPnlUsdt": format_decimal_value(net_realized),
        "reason": reason,
    }


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
    realized_pnl = decimal_or_none(close_result.get("realizedPnlUsdt"))
    if realized_pnl is None:
        realized_pnl = calculate_realized_pnl(position, close_result.get("exitPrice"))

    estimated_fee = decimal_or_none(close_result.get("estimatedFeeUsdt"))
    if estimated_fee is None:
        estimated_fee = calculate_roundtrip_fee(
            entry_notional_raw=position.get("notionalUsdt"),
            exit_price_raw=close_result.get("exitPrice"),
            position=position,
            fee_rate=fee_rate,
        )

    net_realized_pnl = decimal_or_none(close_result.get("netRealizedPnlUsdt"))
    if net_realized_pnl is None and realized_pnl is not None:
        net_realized_pnl = realized_pnl - (estimated_fee or Decimal("0"))

    event_timestamp = time.time()
    closed_at_ms = close_result.get("closedAtMs")
    if closed_at_ms not in (None, ""):
        try:
            event_timestamp = int(closed_at_ms) / 1000
        except Exception:
            event_timestamp = time.time()

    if realized_pnl is not None and net_realized_pnl is None:
        net_realized_pnl = realized_pnl - (estimated_fee or Decimal("0"))

    close_side = "unknown"
    if net_realized_pnl is not None:
        close_side = "win" if net_realized_pnl > 0 else "loss"
        if net_realized_pnl == Decimal("0"):
            close_side = "flat"

    return {
        "timestamp": event_timestamp,
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
        "exitQty": close_result.get("exitQty"),
        "entryPrice": position.get("entryPrice"),
        "quantity": position.get("quantity"),
        "entryNotionalUsdt": position.get("notionalUsdt"),
        "returnBasisUsdt": position.get("returnBasisUsdt"),
        "openedAt": position.get("openedAt"),
        "entryReason": position.get("entryReason"),
        "entrySizingMode": position.get("entrySizingMode"),
        "plannedRiskUsdt": position.get("plannedRiskUsdt"),
        "entryScoreLabel": position.get("scoreLabel"),
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
        "stopLossMode": position.get("stopLossMode"),
        "partialTakeProfitDoneAt": position.get("partialTakeProfitDoneAt"),
        "breakevenActivatedAt": position.get("breakevenActivatedAt"),
        "auditVersion": 1 if audit else None,
        "audit": audit,
    }


def build_partial_exit_record(
    *,
    asset: str,
    side: str,
    strategy_id: str,
    position: dict[str, Any],
    close_result: dict[str, Any],
    reason: str,
    fee_rate: Decimal,
    close_ratio: Decimal,
    audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    partial_position = dict(position)
    total_quantity = infer_position_quantity(position)
    if total_quantity is not None and total_quantity > Decimal("0"):
        exit_qty = decimal_or_none(close_result.get("exitQty"))
        if exit_qty is None:
            exit_qty = total_quantity * close_ratio
        actual_ratio = clamp_decimal(exit_qty / total_quantity, min_value=Decimal("0"), max_value=Decimal("1"))
    else:
        actual_ratio = close_ratio
    for key in ("quantity", "notionalUsdt", "returnBasisUsdt"):
        value = decimal_or_none(position.get(key))
        if value is None:
            continue
        partial_position[key] = format_decimal_value(value * actual_ratio)
    event = build_exit_record(
        asset=asset,
        side=side,
        strategy_id=strategy_id,
        position=partial_position,
        close_result=close_result,
        reason=reason,
        fee_rate=fee_rate,
        audit=audit,
    )
    event["action"] = partial_exit_action(side)
    event["isPartial"] = True
    event["closeRatio"] = format_decimal_value(actual_ratio)
    event["remainingQuantity"] = close_result.get("remainingQuantity")
    event["remainingNotionalUsdt"] = close_result.get("remainingNotionalUsdt")
    return event


def build_signal_rows(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "rank": item.get("rank"),
            "sourceRank": item.get("sourceRank"),
            "asset": normalize_asset(item.get("asset", ""), item.get("baseAsset")),
            "rawAsset": item.get("asset"),
            "displayAsset": item.get("displayAsset"),
            "assetType": item.get("assetType"),
            "tokenId": item.get("tokenId"),
            "score": get_display_score(item),
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


def write_snapshot(path: Path, candidates: list[dict[str, Any]]) -> None:
    snapshot = build_signal_rows(candidates)
    write_json_atomic(path, snapshot)


def record_signal_count_snapshot(
    *,
    workdir: Path,
    positive_count: int,
    negative_count: int,
    signal_meta: dict[str, Any],
) -> None:
    if signal_meta.get("blockNewEntries"):
        return
    history_path = workdir / "runtime" / "signal_count_history.json"
    now_ts = time.time()
    cutoff_ts = now_ts - 25 * 3600
    try:
        existing = json.loads(history_path.read_text(encoding="utf-8-sig"))
    except Exception:
        existing = []
    rows = [
        row
        for row in (existing if isinstance(existing, list) else [])
        if isinstance(row, dict)
        and float(row.get("timestamp") or 0) >= cutoff_ts
    ]
    rows.append(
        {
            "timestamp": now_ts,
            "longCount": int(positive_count),
            "shortCount": int(negative_count),
            "source": signal_meta.get("source"),
        }
    )
    write_json_atomic(history_path, rows)


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
    return is_signal_count_severe_collapse(
        previous_count=previous_count,
        current_count=current_count,
        min_candidates=config.signal_drop_guard_min_candidates,
    )


def should_preserve_previous_snapshot(
    *,
    previous_snapshot: list[dict[str, Any]],
    current_candidates: list[dict[str, Any]],
    current_position_count: int,
    suspend_signal_lost_exit: bool,
) -> bool:
    if suspend_signal_lost_exit:
        return bool(previous_snapshot)
    if current_position_count <= 0:
        return False
    if not current_candidates and previous_snapshot:
        return True
    return False


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


def is_reduce_result_success(close_result: dict[str, Any]) -> bool:
    if close_result.get("status") == "CLOSE_REJECTED":
        return False
    return close_result.get("orderId") not in (None, "")


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


def evaluate_post_entry_weak_exit(
    *,
    config: BotConfig,
    side: str,
    position: dict[str, Any],
    tracking: dict[str, Decimal | None],
    candidate_count: int,
    opposite_candidate_count: int,
    current_rank: int | None,
    previous_snapshot_assets: set[str],
    block_new_entries: bool,
    suspend_signal_lost_exit: bool,
) -> dict[str, Any] | None:
    if not config.enable_post_entry_weak_exit:
        return None
    if block_new_entries or suspend_signal_lost_exit:
        return None
    age_hours = position_age_hours(position)
    if age_hours is None:
        return None
    age_minutes = age_hours * 60
    peak_pnl_pct = tracking.get("peak")
    if peak_pnl_pct is None:
        peak_pnl_pct = decimal_or_none(position.get("maxProfitPct"))

    entry_same_side_count, entry_opposite_count = extract_entry_signal_counts(
        side,
        position.get("entryAudit"),
    )
    entry_rank = int_or_none(position.get("rank"))

    if side == LONG:
        start_minutes, end_minutes = normalize_minute_window(
            config.long_weak_exit_start_minutes,
            config.long_weak_exit_end_minutes,
        )
        if age_minutes < start_minutes or age_minutes > end_minutes:
            return None
        if has_met_peak_profit_threshold(
            peak_pnl_pct,
            config.long_weak_exit_min_peak_pnl_pct,
        ):
            return None

        signal_drop_count = None
        signal_drop_triggered = False
        if entry_same_side_count is not None:
            signal_drop_count = entry_same_side_count - int(candidate_count or 0)
            signal_drop_triggered = (
                signal_drop_count >= int(config.long_weak_exit_signal_drop_count or 0)
            )

        rank_drop = None
        rank_drop_triggered = False
        rank_missing_triggered = False
        if entry_rank is not None:
            if current_rank is None:
                if position.get("asset") not in previous_snapshot_assets:
                    rank_missing_triggered = True
                    rank_drop_triggered = True
            else:
                rank_drop = current_rank - entry_rank
                rank_drop_triggered = (
                    rank_drop >= int(config.long_weak_exit_rank_drop or 0)
                )

        if not signal_drop_triggered and not rank_drop_triggered:
            return None

        trigger_modes: list[str] = []
        if signal_drop_triggered:
            trigger_modes.append("strong_long_count_drop")
        if rank_missing_triggered:
            trigger_modes.append("dropped_out_of_strong_long_list")
        elif rank_drop_triggered:
            trigger_modes.append("rank_drop")

        parts = [
            f"开仓后 {age_minutes:.0f} 分钟",
            f"历史最高收益 {format_decimal_value(peak_pnl_pct)}%",
        ]
        if signal_drop_count is not None:
            parts.append(
                f"强烈看多个数从 {entry_same_side_count} 降到 {candidate_count}"
            )
        if rank_missing_triggered:
            parts.append("当前已掉出强烈看多列表")
        elif entry_rank is not None and current_rank is not None:
            parts.append(f"排名从 {entry_rank} 后移到 {current_rank}")
        return {
            "reason": "post_entry_weakness_exit",
            "detail": "，".join(parts),
            "ageMinutes": round(age_minutes, 2),
            "peakPnlPct": format_decimal_value(peak_pnl_pct),
            "entrySignalCount": entry_same_side_count,
            "currentSignalCount": candidate_count,
            "signalDropCount": signal_drop_count,
            "entryRank": entry_rank,
            "currentRank": current_rank,
            "rankDrop": rank_drop,
            "triggerModes": trigger_modes,
        }

    start_minutes, end_minutes = normalize_minute_window(
        config.short_weak_exit_start_minutes,
        config.short_weak_exit_end_minutes,
    )
    if age_minutes < start_minutes or age_minutes > end_minutes:
        return None
    if has_met_peak_profit_threshold(
        peak_pnl_pct,
        config.short_weak_exit_min_peak_pnl_pct,
    ):
        return None

    signal_drop_count = None
    signal_drop_triggered = False
    if entry_same_side_count is not None:
        signal_drop_count = entry_same_side_count - int(candidate_count or 0)
        signal_drop_triggered = (
            signal_drop_count >= int(config.short_weak_exit_signal_drop_count or 0)
        )

    opposite_rebound_count = None
    opposite_rebound_triggered = False
    if entry_opposite_count is not None:
        opposite_rebound_count = int(opposite_candidate_count or 0) - entry_opposite_count
        opposite_rebound_triggered = (
            opposite_rebound_count
            >= int(config.short_weak_exit_opposite_rebound_count or 0)
        )

    if not signal_drop_triggered and not opposite_rebound_triggered:
        return None

    trigger_modes = []
    if signal_drop_triggered:
        trigger_modes.append("strong_short_count_drop")
    if opposite_rebound_triggered:
        trigger_modes.append("strong_long_rebound")

    parts = [
        f"开仓后 {age_minutes:.0f} 分钟",
        f"历史最高收益 {format_decimal_value(peak_pnl_pct)}%",
    ]
    if signal_drop_count is not None:
        parts.append(
            f"强烈看空个数从 {entry_same_side_count} 降到 {candidate_count}"
        )
    if opposite_rebound_count is not None:
        parts.append(
            f"强烈看多个数从 {entry_opposite_count} 升到 {opposite_candidate_count}"
        )
    return {
        "reason": "post_entry_weakness_exit",
        "detail": "，".join(parts),
        "ageMinutes": round(age_minutes, 2),
        "peakPnlPct": format_decimal_value(peak_pnl_pct),
        "entrySignalCount": entry_same_side_count,
        "currentSignalCount": candidate_count,
        "signalDropCount": signal_drop_count,
        "entryOppositeSignalCount": entry_opposite_count,
        "currentOppositeSignalCount": opposite_candidate_count,
        "oppositeReboundCount": opposite_rebound_count,
        "triggerModes": trigger_modes,
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
    opposite_candidate_count: int = 0,
    suspend_signal_lost_exit: bool = False,
    previous_snapshot: list[dict[str, Any]] | None = None,
    account_snapshot: dict[str, Any] | None = None,
    circuit_breaker: dict[str, Any] | None = None,
    block_new_entries: bool = False,
    block_new_entries_reason: str | None = None,
    freeze_signal_decisions: bool = False,
    signal_source: str | None = None,
    signal_fetch_issues: list[str] | None = None,
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
                "oppositeCandidateCount": 0,
                "openedCount": 0,
                "closedCount": 0,
                "closedWinCount": 0,
                "closedLossCount": 0,
                "closedFlatCount": 0,
                "realizedPnlUsdt": "0",
                "latestDecisions": [],
                "currentCandidateItems": [],
                "signalDropGuardActive": False,
                "blockNewEntriesActive": False,
                "blockNewEntriesReason": None,
                "signalSource": signal_source,
                "signalFetchIssues": signal_fetch_issues or [],
            },
        )
        return {
            "strategyId": strategy_id,
            "side": side,
            "candidateCount": 0,
            "oppositeCandidateCount": 0,
            "openedCount": 0,
            "closedCount": 0,
            "decisions": [],
            "status": "disabled",
        }

    fee_rate = Decimal(str(config.estimated_taker_fee_rate))
    candidate_count = len(candidates)
    entry_signal_count_threshold = signal_count_entry_threshold_for_side(config, side)
    exit_signal_count_threshold = signal_count_exit_threshold_for_side(config, side)
    candidate_assets = {
        normalize_asset(item.get("asset", ""), item.get("baseAsset")) for item in candidates
    }
    candidate_rank_map = {
        normalize_asset(item.get("asset", ""), item.get("baseAsset")): int_or_none(
            item.get("rank")
        )
        for item in candidates
    }
    previous_snapshot_assets = {
        row.get("asset") for row in (previous_snapshot or []) if row.get("asset")
    }

    decisions: list[dict[str, Any]] = []
    opened = 0
    closed = 0
    account_snapshot_cache: dict[str, Any] | None = account_snapshot

    def make_exit_audit(
        *,
        config: BotConfig,
        position: dict[str, Any],
        reason: str,
        tracking: dict[str, Decimal | None] | None = None,
    ) -> dict[str, Any]:
        return enrich_exit_audit_with_signal_counts(
            build_exit_audit_record(
                config=config,
                position=position,
                reason=reason,
                tracking=tracking,
            ),
            side=side,
            candidate_count=candidate_count,
            opposite_candidate_count=opposite_candidate_count,
        )

    current_positions = {
        key: position
        for key, position in state.get("positions", {}).items()
        if side_from_position(position) == side and position.get("strategyId") == strategy_id
    }
    entry_confirmation_rounds = update_signal_count_action_confirmation(
        state=state,
        side=side,
        action="entry",
        triggered=(
            bool(config.enable_signal_count_entry_gate)
            and not block_new_entries
            and not freeze_signal_decisions
            and candidate_count >= entry_signal_count_threshold
        ),
        current_count=candidate_count,
        threshold=entry_signal_count_threshold,
    )
    exit_confirmation_rounds = update_signal_count_action_confirmation(
        state=state,
        side=side,
        action="exit",
        triggered=(
            bool(config.enable_signal_count_exit)
            and not freeze_signal_decisions
            and bool(current_positions)
            and candidate_count < exit_signal_count_threshold
        ),
        current_count=candidate_count,
        threshold=exit_signal_count_threshold,
    )

    for key, position in list(current_positions.items()):
        if config.dry_run or broker.name != "binance_testnet":
            continue
        live_position = broker.get_live_position(position["contractSymbol"], side)
        if live_position is not None:
            continue
        now_ms = int(time.time() * 1000)
        close_result = reconcile_missing_close_with_exchange(
            broker=broker,
            position=position,
            now_ms=now_ms,
        )
        if close_result is None:
            close_result = {
                "status": "POSITION_MISSING",
                "exitPrice": format(broker.get_mark_price(position["contractSymbol"]), "f"),
                "confirmedClosed": False,
                "closedAtMs": None,
                "reason": "exchange_position_missing",
            }
        resolved_reason = str(close_result.get("reason") or "exchange_position_missing")
        if resolved_reason != "exchange_position_missing":
            logging.info(
                "missing_position_reconciled symbol=%s side=%s reason=%s orderId=%s",
                position["contractSymbol"],
                side,
                resolved_reason,
                close_result.get("orderId"),
            )
        exit_audit = make_exit_audit(
            config=config,
            position=position,
            reason=resolved_reason,
        )
        exit_event = build_exit_record(
            asset=position["asset"],
            side=side,
            strategy_id=strategy_id,
            position=position,
            close_result=close_result,
            reason=resolved_reason,
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
                "reason": resolved_reason,
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
        if should_activate_breakeven_stop(
            config=config,
            position=position,
            current_pnl_pct=tracking["current"],
        ):
            breakeven_price = calculate_breakeven_stop_price(
                position,
                Decimal(str(config.breakeven_buffer_pct)),
            )
            if breakeven_price is not None:
                position["stopLossOverridePrice"] = format_decimal_value(breakeven_price)
                position["stopLossMode"] = "breakeven"
                position["breakevenActivatedAt"] = time.time()
                position["breakevenTriggerPct"] = format_decimal_value(
                    config.breakeven_trigger_pct
                )
                position["breakevenBufferPct"] = format_decimal_value(
                    config.breakeven_buffer_pct
                )

        if should_trigger_partial_take_profit(
            config=config,
            position=position,
            current_pnl_pct=tracking["current"],
        ):
            partial_ratio = Decimal(str(config.partial_take_profit_close_ratio))
            partial_audit = make_exit_audit(
                config=config,
                position=position,
                reason="partial_take_profit",
                tracking=tracking,
            )
            partial_result = broker.close_position(
                contract_symbol=position["contractSymbol"],
                asset=position["asset"],
                side=side,
                position=position,
                dry_run=config.dry_run,
                close_ratio=float(partial_ratio),
            )
            if not is_reduce_result_success(partial_result):
                decisions.append(
                    build_close_failed_decision(
                        position=position,
                        side=side,
                        attempted_reason="partial_take_profit",
                        close_result=partial_result,
                    )
                )
            else:
                position["partialTakeProfitDoneAt"] = time.time()
                position["partialTakeProfitCloseRatio"] = format_decimal_value(partial_ratio)
                partial_event = build_partial_exit_record(
                    asset=position["asset"],
                    side=side,
                    strategy_id=strategy_id,
                    position=position,
                    close_result=partial_result,
                    reason="partial_take_profit",
                    fee_rate=fee_rate,
                    close_ratio=partial_ratio,
                    audit=partial_audit,
                )
                state.setdefault("history", []).append(partial_event)
                if broker.name == "binance_testnet" and not config.dry_run:
                    live_after = broker.get_live_position(position["contractSymbol"], side)
                    if live_after is None:
                        state.get("positions", {}).pop(key, None)
                        decisions.append(
                            {
                                "asset": position["asset"],
                                "side": side,
                                "action": partial_exit_action(side),
                                "contractSymbol": position["contractSymbol"],
                                "status": partial_result["status"],
                                "reason": "partial_take_profit",
                                "realizedPnlUsdt": partial_event["realizedPnlUsdt"],
                                "netRealizedPnlUsdt": partial_event["netRealizedPnlUsdt"],
                                "closeRatio": partial_event["closeRatio"],
                                "remainingQuantity": None,
                            }
                        )
                        continue
                    live_amount = abs(Decimal(str(live_after.get("positionAmt", "0"))))
                    if live_amount == Decimal("0"):
                        state.get("positions", {}).pop(key, None)
                        continue
                    api_return_basis = extract_api_return_basis(live_after)
                    position["quantity"] = format_decimal_value(live_amount)
                    position["notionalUsdt"] = format_decimal_value(
                        abs(Decimal(str(live_after.get("notional", "0"))))
                    )
                    position["entryPrice"] = live_after.get("entryPrice") or position.get("entryPrice")
                    position["unRealizedProfit"] = live_after.get("unRealizedProfit")
                    position["positionInitialMargin"] = live_after.get("positionInitialMargin")
                    position["initialMargin"] = live_after.get("initialMargin")
                    position["isolatedWallet"] = live_after.get("isolatedWallet")
                    position["isolatedMargin"] = live_after.get("isolatedMargin")
                    if api_return_basis is not None:
                        position["returnBasisUsdt"] = str(api_return_basis)
                else:
                    remaining_ratio = Decimal("1") - partial_ratio
                    for key_name in ("quantity", "notionalUsdt", "returnBasisUsdt"):
                        current_value = decimal_or_none(position.get(key_name))
                        if current_value is not None:
                            position[key_name] = format_decimal_value(
                                current_value * remaining_ratio
                            )
                if config.enable_stop_loss:
                    stop_loss_result = ensure_stop_loss_with_retries(
                        broker=broker,
                        contract_symbol=position["contractSymbol"],
                        side=side,
                        position=position,
                        stop_loss_pct=config.stop_loss_pct,
                        dry_run=config.dry_run,
                        context="partial_take_profit_refresh",
                    )
                    update_position_stop_loss_state(
                        position=position,
                        config=config,
                        stop_loss_result=stop_loss_result,
                    )
                decisions.append(
                    {
                        "asset": position["asset"],
                        "side": side,
                        "action": partial_exit_action(side),
                        "contractSymbol": position["contractSymbol"],
                        "status": partial_result["status"],
                        "reason": "partial_take_profit",
                        "realizedPnlUsdt": partial_event["realizedPnlUsdt"],
                        "netRealizedPnlUsdt": partial_event["netRealizedPnlUsdt"],
                        "closeRatio": partial_event["closeRatio"],
                        "remainingQuantity": partial_result.get("remainingQuantity"),
                    }
                )
                continue

        if config.enable_stop_loss:
            stop_loss_source_position = dict(position)
            if live_position is not None:
                stop_loss_source_position.update(live_position)
            stop_loss_result = ensure_stop_loss_with_retries(
                broker=broker,
                contract_symbol=position["contractSymbol"],
                side=side,
                position=stop_loss_source_position,
                stop_loss_pct=config.stop_loss_pct,
                dry_run=config.dry_run,
                context="position_refresh",
            )
        else:
            stop_loss_result = None
        update_position_stop_loss_state(
            position=position,
            config=config,
            stop_loss_result=stop_loss_result,
        )
        if is_breakeven_stop_setup_failure(position, stop_loss_result):
            exit_audit = make_exit_audit(
                config=config,
                position=position,
                reason="breakeven_stop_setup_failed",
                tracking=tracking,
            )
            exit_audit["stopLossSetupResult"] = stop_loss_result or {}
            close_result = broker.close_position(
                contract_symbol=position["contractSymbol"],
                asset=position["asset"],
                side=side,
                position=position,
                dry_run=config.dry_run,
            )
            if not is_close_result_success(close_result):
                failed_decision = build_close_failed_decision(
                    position=position,
                    side=side,
                    attempted_reason="breakeven_stop_setup_failed",
                    close_result=close_result,
                )
                failed_decision.update(
                    {
                        "detail": "保本止损切换失败，尝试主动平仓也失败。",
                        "stopLossStatus": (stop_loss_result or {}).get("status"),
                        "stopLossAttempts": (stop_loss_result or {}).get("attempts"),
                        "stopLossErrors": (stop_loss_result or {}).get("errors") or [],
                    }
                )
                decisions.append(failed_decision)
                continue
            exit_event = build_exit_record(
                asset=position["asset"],
                side=side,
                strategy_id=strategy_id,
                position=position,
                close_result=close_result,
                reason="breakeven_stop_setup_failed",
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
                    "reason": "breakeven_stop_setup_failed",
                    "detail": "保本止损切换失败，已主动平仓撤退。",
                    "stopLossStatus": (stop_loss_result or {}).get("status"),
                    "stopLossAttempts": (stop_loss_result or {}).get("attempts"),
                    "stopLossErrors": (stop_loss_result or {}).get("errors") or [],
                    "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                    "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                    "closeSide": exit_event["closeSide"],
                }
            )
            closed += 1
            continue
        if should_trigger_stop_loss(
            config=config,
            current_pnl_pct=tracking["current"],
        ):
            exit_audit = make_exit_audit(
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
            exit_audit = make_exit_audit(
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
            exit_audit = make_exit_audit(
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
            exit_audit = make_exit_audit(
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
        if freeze_signal_decisions:
            position["signalLostRounds"] = 0
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": "hold",
                    "reason": "signal_source_unstable",
                    "signalSource": signal_source,
                    "signalFetchIssues": signal_fetch_issues or [],
                }
            )
            continue
        weak_exit_context = evaluate_post_entry_weak_exit(
            config=config,
            side=side,
            position=position,
            tracking=tracking,
            candidate_count=candidate_count,
            opposite_candidate_count=opposite_candidate_count,
            current_rank=candidate_rank_map.get(position["asset"]),
            previous_snapshot_assets=previous_snapshot_assets,
            block_new_entries=block_new_entries,
            suspend_signal_lost_exit=suspend_signal_lost_exit,
        )
        if weak_exit_context is not None:
            exit_audit = make_exit_audit(
                config=config,
                position=position,
                reason="post_entry_weakness_exit",
                tracking=tracking,
            )
            exit_audit.update(
                {
                    "ageMinutes": weak_exit_context.get("ageMinutes"),
                    "peakPnlPct": weak_exit_context.get("peakPnlPct"),
                    "entrySignalCount": weak_exit_context.get("entrySignalCount"),
                    "currentSignalCount": weak_exit_context.get("currentSignalCount"),
                    "signalDropCount": weak_exit_context.get("signalDropCount"),
                    "entryOppositeSignalCount": weak_exit_context.get(
                        "entryOppositeSignalCount"
                    ),
                    "currentOppositeSignalCount": weak_exit_context.get(
                        "currentOppositeSignalCount"
                    ),
                    "oppositeReboundCount": weak_exit_context.get(
                        "oppositeReboundCount"
                    ),
                    "entryRank": weak_exit_context.get("entryRank"),
                    "currentRank": weak_exit_context.get("currentRank"),
                    "rankDrop": weak_exit_context.get("rankDrop"),
                    "triggerModes": weak_exit_context.get("triggerModes") or [],
                }
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
                        attempted_reason="post_entry_weakness_exit",
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
                reason="post_entry_weakness_exit",
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
                    "reason": "post_entry_weakness_exit",
                    "detail": weak_exit_context.get("detail"),
                    "ageMinutes": weak_exit_context.get("ageMinutes"),
                    "peakPnlPct": weak_exit_context.get("peakPnlPct"),
                    "triggerModes": weak_exit_context.get("triggerModes") or [],
                    "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                    "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                    "closeSide": exit_event["closeSide"],
                }
            )
            closed += 1
            continue
        if (
            config.enable_signal_count_exit
            and candidate_count < exit_signal_count_threshold
        ):
            if exit_confirmation_rounds < SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS:
                position["signalLostRounds"] = 0
                decisions.append(
                    {
                        "asset": position["asset"],
                        "side": side,
                        "action": "hold",
                        "reason": "signal_count_exit_confirming",
                        "detail": (
                            f"当前强信号 {candidate_count} 个，已低于平仓阈值 "
                            f"{exit_signal_count_threshold} 个；连续确认 "
                            f"{SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS} 轮后才按榜单数量平仓"
                        ),
                        "currentSignalCount": candidate_count,
                        "exitSignalCountThreshold": exit_signal_count_threshold,
                        "confirmationRounds": exit_confirmation_rounds,
                        "confirmationRequiredRounds": SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS,
                    }
                )
                continue
            exit_audit = make_exit_audit(
                config=config,
                position=position,
                reason="signal_count_below_exit_threshold",
                tracking=tracking,
            )
            exit_audit["currentSignalCount"] = candidate_count
            exit_audit["exitSignalCountThreshold"] = exit_signal_count_threshold
            exit_audit["confirmationRounds"] = exit_confirmation_rounds
            exit_audit["confirmationRequiredRounds"] = SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS
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
                        attempted_reason="signal_count_below_exit_threshold",
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
                reason="signal_count_below_exit_threshold",
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
                    "reason": "signal_count_below_exit_threshold",
                    "detail": (
                        f"当前强信号 {candidate_count} 个，低于平仓阈值 "
                        f"{exit_signal_count_threshold} 个"
                    ),
                    "currentSignalCount": candidate_count,
                    "exitSignalCountThreshold": exit_signal_count_threshold,
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
        if not config.enable_signal_lost_exit:
            position["signalLostRounds"] = 0
            decisions.append(
                {
                    "asset": position["asset"],
                    "side": side,
                    "action": "hold",
                    "reason": "signal_lost_exit_disabled",
                }
            )
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
            audit=make_exit_audit(
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
        if block_new_entries:
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": block_new_entries_reason or "signal_source_unstable",
                    "currentSignalCount": candidate_count,
                    "oppositeSignalCount": opposite_candidate_count,
                    "signalSource": signal_source,
                    "signalFetchIssues": signal_fetch_issues or [],
                }
            )
            continue
        contract = futures_catalog.get_contract(asset)
        if not contract:
            contract_entry = futures_catalog.get_contract_entry(asset)
            if contract_entry:
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "contract_not_trading",
                        "contractSymbol": contract_entry.get("symbol"),
                        "contractStatus": contract_entry.get("status"),
                    }
                )
            else:
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
        correlated_match_count = 0
        account_equity = None
        sizing_result: dict[str, Decimal | str | None] | None = None
        risk_summary = calculate_open_risk_summary(state, config)

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
                            stop_loss_result = ensure_stop_loss_with_retries(
                                broker=broker,
                                contract_symbol=contract["symbol"],
                                side=side,
                                position={**synced_position, **live_position},
                                stop_loss_pct=config.stop_loss_pct,
                                dry_run=config.dry_run,
                                context="synced_position",
                            )
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
            config.enable_signal_count_entry_gate
            and candidate_count < entry_signal_count_threshold
        ):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "signal_count_entry_gate_blocked",
                    "detail": (
                        f"当前强信号 {candidate_count} 个，榜单数量开仓至少需要 "
                        f"{entry_signal_count_threshold} 个"
                    ),
                    "currentSignalCount": candidate_count,
                    "requiredSignalCount": entry_signal_count_threshold,
                }
            )
            continue
        if (
            config.enable_signal_count_entry_gate
            and candidate_count >= entry_signal_count_threshold
            and entry_confirmation_rounds < SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS
        ):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "signal_count_entry_confirming",
                    "detail": (
                        f"当前强信号 {candidate_count} 个，已达到开仓阈值 "
                        f"{entry_signal_count_threshold} 个；连续确认 "
                        f"{SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS} 轮后才允许开仓"
                    ),
                    "currentSignalCount": candidate_count,
                    "requiredSignalCount": entry_signal_count_threshold,
                    "confirmationRounds": entry_confirmation_rounds,
                    "confirmationRequiredRounds": SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS,
                }
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

        if is_signal_imbalance_blocked(
            config=config,
            candidate_count=candidate_count,
            opposite_candidate_count=opposite_candidate_count,
        ):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "signal_imbalance_blocked",
                    "currentSignalCount": candidate_count,
                    "oppositeSignalCount": opposite_candidate_count,
                    "signalImbalanceMinCount": config.signal_imbalance_min_count,
                    "signalImbalanceRatio": format_decimal_value(config.signal_imbalance_ratio),
                }
            )
            continue

        if is_in_cooldown(state, asset, side, config.cooldown_minutes):
            decisions.append({"asset": asset, "side": side, "action": "skip", "reason": "cooldown"})
            continue

        if circuit_breaker and circuit_breaker.get("active"):
            decisions.append(
                {
                    "asset": asset,
                    "side": side,
                    "action": "skip",
                    "reason": "account_circuit_breaker",
                    "circuitBreakerUntil": circuit_breaker.get("until"),
                    "circuitBreakerReasons": circuit_breaker.get("reasons") or [],
                }
            )
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
                        "maxAbsFundingRatePct": format_decimal_value(
                            config.max_abs_funding_rate_pct
                        ),
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

        if (
            config.enable_correlation_filter
            or (
                config.enable_portfolio_risk_cap
                and config.max_correlated_positions_per_side > 0
            )
        ):
            correlation_summary = summarize_correlated_positions(
                broker=broker,
                state=state,
                side=side,
                contract_symbol=contract["symbol"],
                config=config,
            )
            strongest_correlation = correlation_summary.get("strongest") or {}
            correlated_symbol = strongest_correlation.get("symbol")
            correlated_value = strongest_correlation.get("correlation")
            correlated_match_count = int(correlation_summary.get("matchCount", 0) or 0)
            correlated_matches = correlation_summary.get("matches") or []
            first_match = correlated_matches[0] if correlated_matches else None
            if config.enable_correlation_filter and first_match is not None:
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "correlated_with_existing",
                        "correlatedSymbol": first_match.get("symbol"),
                        "correlation": round(first_match.get("correlation"), 4),
                    }
                )
                continue
            if (
                config.enable_portfolio_risk_cap
                and config.max_correlated_positions_per_side > 0
                and correlated_match_count >= int(config.max_correlated_positions_per_side)
            ):
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "correlated_cluster_limit",
                        "correlatedMatchCount": correlated_match_count,
                        "maxCorrelatedPositionsPerSide": int(
                            config.max_correlated_positions_per_side
                        ),
                        "correlatedSymbol": correlated_symbol,
                        "correlation": round(correlated_value, 4)
                        if correlated_value is not None
                        else None,
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

        if (
            account_snapshot_cache is None
            and (
                config.enable_risk_position_sizing
                or config.enable_portfolio_risk_cap
            )
        ):
            account_snapshot_cache = broker.get_account_snapshot()
        account_equity = extract_account_equity(account_snapshot_cache)
        sizing_result = resolve_entry_notional_usdt(
            config=config,
            account_equity=account_equity,
        )
        entry_notional = Decimal(str(sizing_result["notionalUsdt"]))
        estimated_entry_risk = sizing_result.get("estimatedRiskUsdt")
        if (
            config.enable_portfolio_risk_cap
            and account_equity is not None
            and account_equity > Decimal("0")
            and estimated_entry_risk is not None
        ):
            side_risk = risk_summary["longRiskUsdt"] if side == LONG else risk_summary["shortRiskUsdt"]
            projected_side_risk_pct = ((side_risk + estimated_entry_risk) / account_equity) * Decimal("100")
            projected_total_risk_pct = (
                (risk_summary["totalRiskUsdt"] + estimated_entry_risk)
                / account_equity
            ) * Decimal("100")
            if projected_side_risk_pct > Decimal(str(config.max_side_open_risk_pct)):
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "side_risk_limit",
                        "projectedSideRiskPct": format(projected_side_risk_pct, "f"),
                        "maxSideOpenRiskPct": format_decimal_value(config.max_side_open_risk_pct),
                    }
                )
                continue
            if projected_total_risk_pct > Decimal(str(config.max_total_open_risk_pct)):
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": "skip",
                        "reason": "portfolio_risk_limit",
                        "projectedTotalRiskPct": format(projected_total_risk_pct, "f"),
                        "maxTotalOpenRiskPct": format_decimal_value(config.max_total_open_risk_pct),
                    }
                )
                continue

        entry_audit = build_entry_audit_record(
            config=config,
            state=state,
            side=side,
            candidate_count=candidate_count,
            opposite_candidate_count=opposite_candidate_count,
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
            correlated_match_count=correlated_match_count,
            account_equity=account_equity,
            sizing_result=sizing_result,
            risk_summary=risk_summary,
            circuit_breaker=circuit_breaker,
        )

        metadata = {
            "rank": item.get("rank"),
            "sourceRank": item.get("sourceRank"),
            "score": get_score(item),
            "scoreLabel": get_score_label(item),
            "signalSource": "binance_ai_select",
            "interval": config.interval,
            "assetType": item.get("assetType"),
            "quoteVolume24hUsdt": format(quote_volume_24h, "f"),
            "requiredMarginMode": config.required_margin_mode,
            "leverage": config.leverage,
            "side": side,
            "sizingMode": sizing_result.get("mode"),
            "plannedNotionalUsdt": format_decimal_value(entry_notional),
            "estimatedEntryRiskUsdt": format_decimal_value(estimated_entry_risk),
        }
        try:
            if side == LONG:
                order_result = broker.place_long_market_order(
                    contract_symbol=contract["symbol"],
                    asset=asset,
                    notional_usdt=float(entry_notional),
                    metadata=metadata,
                    dry_run=config.dry_run,
                )
            else:
                order_result = broker.place_short_market_order(
                    contract_symbol=contract["symbol"],
                    asset=asset,
                    notional_usdt=float(entry_notional),
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
            "sourceRank": item.get("sourceRank"),
            "score": get_score(item),
            "scoreLabel": get_score_label(item),
            "notionalUsdt": order_result.get("notionalUsdt", format_decimal_value(entry_notional)),
            "dryRun": config.dry_run,
            "requiredMarginMode": config.required_margin_mode,
            "leverage": config.leverage,
            "returnBasisUsdt": None,
            "entryReason": signal_enter_reason(side),
            "entrySizingMode": sizing_result.get("mode"),
            "plannedRiskUsdt": format_decimal_value(estimated_entry_risk),
            "riskBudgetUsdt": format_decimal_value(sizing_result.get("riskBudgetUsdt")),
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
            stop_loss_result = ensure_stop_loss_with_retries(
                broker=broker,
                contract_symbol=contract["symbol"],
                side=side,
                position=opened_position,
                stop_loss_pct=config.stop_loss_pct,
                dry_run=config.dry_run,
                context="entry",
            )
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
        stop_loss_failed_after_entry = bool(
            config.enable_stop_loss and not (stop_loss_result or {}).get("configured")
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
                "notionalUsdt": opened_position.get("notionalUsdt"),
                "scoreLabel": opened_position.get("scoreLabel"),
                "entrySizingMode": opened_position.get("entrySizingMode"),
                "plannedRiskUsdt": opened_position.get("plannedRiskUsdt"),
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
                "notionalUsdt": opened_position.get("notionalUsdt"),
                "sizingMode": sizing_result.get("mode"),
                "estimatedEntryRiskUsdt": format_decimal_value(estimated_entry_risk),
                "stopLossPrice": opened_position.get("stopLossPrice"),
                "stopLossStatus": opened_position.get("stopLossStatus"),
            }
        )
        opened += 1
        account_snapshot_cache = None

        if stop_loss_failed_after_entry and not config.dry_run:
            logging.error(
                "entry_stop_loss_unprotected_closing asset=%s symbol=%s side=%s status=%s errors=%s",
                asset,
                contract["symbol"],
                side,
                (stop_loss_result or {}).get("status"),
                (stop_loss_result or {}).get("errors") or [],
            )
            emergency_audit = build_exit_audit_record(
                config=config,
                position=opened_position,
                reason="stop_loss_setup_failed",
            )
            enrich_exit_audit_with_signal_counts(
                emergency_audit,
                side=side,
                candidate_count=candidate_count,
                opposite_candidate_count=opposite_candidate_count,
            )
            emergency_audit["stopLossSetupResult"] = stop_loss_result or {}
            emergency_close_result = broker.close_position(
                contract_symbol=contract["symbol"],
                asset=asset,
                side=side,
                position=opened_position,
                dry_run=config.dry_run,
            )
            if is_close_result_success(emergency_close_result):
                exit_event = build_exit_record(
                    asset=asset,
                    side=side,
                    strategy_id=strategy_id,
                    position=opened_position,
                    close_result=emergency_close_result,
                    reason="stop_loss_setup_failed",
                    fee_rate=fee_rate,
                    audit=emergency_audit,
                )
                state.setdefault("history", []).append(exit_event)
                state.get("positions", {}).pop(position_key(asset, side), None)
                decisions.append(
                    {
                        "asset": asset,
                        "side": side,
                        "action": exit_action(side),
                        "contractSymbol": contract["symbol"],
                        "status": emergency_close_result["status"],
                        "reason": "stop_loss_setup_failed",
                        "detail": "开仓后连续重试仍未挂上保护止损，已立即平仓撤退。",
                        "stopLossStatus": (stop_loss_result or {}).get("status"),
                        "stopLossAttempts": (stop_loss_result or {}).get("attempts"),
                        "stopLossErrors": (stop_loss_result or {}).get("errors") or [],
                        "realizedPnlUsdt": exit_event["realizedPnlUsdt"],
                        "netRealizedPnlUsdt": exit_event["netRealizedPnlUsdt"],
                        "closeSide": exit_event["closeSide"],
                    }
                )
                closed += 1
                continue
            failed_decision = build_close_failed_decision(
                position=opened_position,
                side=side,
                attempted_reason="stop_loss_setup_failed",
                close_result=emergency_close_result,
            )
            failed_decision.update(
                {
                    "detail": "开仓后连续重试仍未挂上保护止损，尝试立即平仓但失败。",
                    "stopLossStatus": (stop_loss_result or {}).get("status"),
                    "stopLossAttempts": (stop_loss_result or {}).get("attempts"),
                    "stopLossErrors": (stop_loss_result or {}).get("errors") or [],
                }
            )
            decisions.append(failed_decision)

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
            "status": "guarded" if circuit_breaker and circuit_breaker.get("active") else "ok",
            "side": side,
            "candidateCount": len(candidates),
            "oppositeCandidateCount": opposite_candidate_count,
            "openedCount": opened,
            "closedCount": closed,
            "closedWinCount": closed_win_count,
            "closedLossCount": closed_loss_count,
            "closedFlatCount": closed_flat_count,
            "realizedPnlUsdt": str(realized_pnl_total),
            "accountCircuitBreaker": circuit_breaker or {},
            "latestDecisions": decisions,
            "currentCandidateItems": build_signal_rows(candidates),
            "signalDropGuardActive": suspend_signal_lost_exit,
            "blockNewEntriesActive": block_new_entries,
            "blockNewEntriesReason": block_new_entries_reason,
            "signalSource": signal_source,
            "signalFetchIssues": signal_fetch_issues or [],
        },
    )

    return {
        "strategyId": strategy_id,
        "side": side,
        "candidateCount": len(candidates),
        "oppositeCandidateCount": opposite_candidate_count,
        "openedCount": opened,
        "closedCount": closed,
        "closedWinCount": closed_win_count,
        "closedLossCount": closed_loss_count,
        "closedFlatCount": closed_flat_count,
        "realizedPnlUsdt": str(realized_pnl_total),
        "accountCircuitBreaker": circuit_breaker or {},
        "decisions": decisions,
        "currentCandidateItems": build_signal_rows(candidates),
        "signalDropGuardActive": suspend_signal_lost_exit,
        "blockNewEntriesActive": block_new_entries,
        "blockNewEntriesReason": block_new_entries_reason,
        "signalSource": signal_source,
        "signalFetchIssues": signal_fetch_issues or [],
    }


def run_once(config: BotConfig) -> dict[str, Any]:
    broker = select_broker_adapter()
    state_store = StateStore(config.state_file)
    state = migrate_state(state_store.load())
    futures_catalog = BinanceFuturesCatalog(broker.exchange_info_url())
    previous_positive_snapshot = load_snapshot_rows(config.positive_snapshot_file)
    previous_negative_snapshot = load_snapshot_rows(config.negative_snapshot_file)

    positive_candidates, negative_candidates, signal_meta = fetch_signal_assets(
        config,
        previous_positive_snapshot=previous_positive_snapshot,
        previous_negative_snapshot=previous_negative_snapshot,
    )
    positive_assets = [normalize_asset(item.get("asset", ""), item.get("baseAsset")) for item in positive_candidates]
    negative_assets = [normalize_asset(item.get("asset", ""), item.get("baseAsset")) for item in negative_candidates]
    prev_positive_assets = [row.get("asset") for row in previous_positive_snapshot if row.get("asset")]
    logging.info(
        "signal_fetch_result source=%s issues=%s positive=%d negative=%d positive_assets=%s prev_positive_assets=%s",
        signal_meta.get("source"),
        signal_meta.get("renderedIssues") or [],
        len(positive_candidates),
        len(negative_candidates),
        positive_assets,
        prev_positive_assets,
    )
    record_signal_count_snapshot(
        workdir=config.state_file.parent.parent,
        positive_count=len(positive_candidates),
        negative_count=len(negative_candidates),
        signal_meta=signal_meta,
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
    long_block_new_entries = bool(signal_meta.get("blockNewEntries")) or suspend_long_signal_lost_exit
    short_block_new_entries = bool(signal_meta.get("blockNewEntries")) or suspend_short_signal_lost_exit
    freeze_signal_decisions = bool(signal_meta.get("freezeSignalDecisions"))
    long_block_reason = (
        "signal_drop_guard"
        if suspend_long_signal_lost_exit
        else ("signal_source_unstable" if signal_meta.get("blockNewEntries") else None)
    )
    short_block_reason = (
        "signal_drop_guard"
        if suspend_short_signal_lost_exit
        else ("signal_source_unstable" if signal_meta.get("blockNewEntries") else None)
    )

    preserve_positive_snapshot = should_preserve_previous_snapshot(
        previous_snapshot=previous_positive_snapshot,
        current_candidates=positive_candidates,
        current_position_count=strategy_position_count(state, LONG_STRATEGY_ID, LONG),
        suspend_signal_lost_exit=suspend_long_signal_lost_exit or bool(signal_meta.get("blockNewEntries")),
    )
    preserve_negative_snapshot = should_preserve_previous_snapshot(
        previous_snapshot=previous_negative_snapshot,
        current_candidates=negative_candidates,
        current_position_count=strategy_position_count(state, SHORT_STRATEGY_ID, SHORT),
        suspend_signal_lost_exit=suspend_short_signal_lost_exit or bool(signal_meta.get("blockNewEntries")),
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

    try:
        account_snapshot = broker.get_account_snapshot()
    except Exception as exc:
        logging.warning("account_snapshot_failed_for_risk_controls error=%s", exc)
        account_snapshot = None
    equity_history = record_account_equity_snapshot(
        config.equity_history_file,
        account_snapshot,
    )
    circuit_breaker = evaluate_account_circuit_breaker(
        config=config,
        state=state,
        account_snapshot=account_snapshot,
        equity_history=equity_history,
    )
    if circuit_breaker.get("active"):
        logging.warning(
            "account_circuit_breaker_active reasons=%s until=%s",
            circuit_breaker.get("reasons"),
            circuit_breaker.get("until"),
        )

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
            opposite_candidate_count=len(negative_candidates),
            suspend_signal_lost_exit=suspend_long_signal_lost_exit,
            previous_snapshot=previous_positive_snapshot,
            account_snapshot=account_snapshot,
            circuit_breaker=circuit_breaker,
            block_new_entries=long_block_new_entries,
            block_new_entries_reason=long_block_reason,
            freeze_signal_decisions=freeze_signal_decisions,
            signal_source=signal_meta.get("source"),
            signal_fetch_issues=signal_meta.get("renderedIssues") or [],
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
            opposite_candidate_count=len(positive_candidates),
            suspend_signal_lost_exit=suspend_short_signal_lost_exit,
            previous_snapshot=previous_negative_snapshot,
            account_snapshot=account_snapshot,
            circuit_breaker=circuit_breaker,
            block_new_entries=short_block_new_entries,
            block_new_entries_reason=short_block_reason,
            freeze_signal_decisions=freeze_signal_decisions,
            signal_source=signal_meta.get("source"),
            signal_fetch_issues=signal_meta.get("renderedIssues") or [],
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
        "accountCircuitBreaker": circuit_breaker,
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
        enable_signal_count_entry_gate=get_env_bool("ENABLE_SIGNAL_COUNT_ENTRY_GATE", False),
        min_long_signal_count_to_open=int(os.getenv("MIN_LONG_SIGNAL_COUNT_TO_OPEN", "13")),
        min_short_signal_count_to_open=int(os.getenv("MIN_SHORT_SIGNAL_COUNT_TO_OPEN", "20")),
        enable_signal_imbalance_filter=get_env_bool("ENABLE_SIGNAL_IMBALANCE_FILTER", True),
        signal_imbalance_min_count=int(
            os.getenv(
                "SIGNAL_IMBALANCE_MIN_COUNT",
                os.getenv("MIN_SIGNAL_COUNT_TO_OPEN", "6"),
            )
        ),
        signal_imbalance_ratio=float(os.getenv("SIGNAL_IMBALANCE_RATIO", "2")),
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
        enable_signal_lost_exit=get_env_bool("ENABLE_SIGNAL_LOST_EXIT", True),
        enable_signal_drop_guard=get_env_bool("ENABLE_SIGNAL_DROP_GUARD", True),
        signal_drop_guard_ratio=float(os.getenv("SIGNAL_DROP_GUARD_RATIO", "0.7")),
        signal_drop_guard_min_candidates=int(
            os.getenv("SIGNAL_DROP_GUARD_MIN_CANDIDATES", "5")
        ),
        signal_lost_exit_confirm_rounds=int(
            os.getenv("SIGNAL_LOST_EXIT_CONFIRM_ROUNDS", "3")
        ),
        enable_signal_count_exit=get_env_bool("ENABLE_SIGNAL_COUNT_EXIT", False),
        long_signal_count_to_close_below=int(
            os.getenv("LONG_SIGNAL_COUNT_TO_CLOSE_BELOW", "11")
        ),
        short_signal_count_to_close_below=int(
            os.getenv("SHORT_SIGNAL_COUNT_TO_CLOSE_BELOW", "19")
        ),
        enable_post_entry_weak_exit=get_env_bool("ENABLE_POST_ENTRY_WEAK_EXIT", False),
        long_weak_exit_start_minutes=int(
            os.getenv("LONG_WEAK_EXIT_START_MINUTES", "45")
        ),
        long_weak_exit_end_minutes=int(
            os.getenv("LONG_WEAK_EXIT_END_MINUTES", "90")
        ),
        long_weak_exit_min_peak_pnl_pct=float(
            os.getenv("LONG_WEAK_EXIT_MIN_PEAK_PNL_PCT", "1")
        ),
        long_weak_exit_signal_drop_count=int(
            os.getenv("LONG_WEAK_EXIT_SIGNAL_DROP_COUNT", "2")
        ),
        long_weak_exit_rank_drop=int(
            os.getenv("LONG_WEAK_EXIT_RANK_DROP", "3")
        ),
        short_weak_exit_start_minutes=int(
            os.getenv("SHORT_WEAK_EXIT_START_MINUTES", "30")
        ),
        short_weak_exit_end_minutes=int(
            os.getenv("SHORT_WEAK_EXIT_END_MINUTES", "60")
        ),
        short_weak_exit_min_peak_pnl_pct=float(
            os.getenv("SHORT_WEAK_EXIT_MIN_PEAK_PNL_PCT", "0")
        ),
        short_weak_exit_signal_drop_count=int(
            os.getenv("SHORT_WEAK_EXIT_SIGNAL_DROP_COUNT", "1")
        ),
        short_weak_exit_opposite_rebound_count=int(
            os.getenv("SHORT_WEAK_EXIT_OPPOSITE_REBOUND_COUNT", "1")
        ),
        estimated_taker_fee_rate=float(os.getenv("ESTIMATED_TAKER_FEE_RATE", "0.0005")),
        enable_account_circuit_breaker=get_env_bool("ENABLE_ACCOUNT_CIRCUIT_BREAKER", True),
        daily_loss_pause_pct=float(os.getenv("DAILY_LOSS_PAUSE_PCT", "4")),
        max_consecutive_losses=int(os.getenv("MAX_CONSECUTIVE_LOSSES", "4")),
        max_account_drawdown_pct=float(os.getenv("MAX_ACCOUNT_DRAWDOWN_PCT", "12")),
        circuit_breaker_cooldown_minutes=int(
            os.getenv("CIRCUIT_BREAKER_COOLDOWN_MINUTES", "120")
        ),
        enable_risk_position_sizing=get_env_bool("ENABLE_RISK_POSITION_SIZING", True),
        risk_per_trade_pct=float(os.getenv("RISK_PER_TRADE_PCT", "0.6")),
        min_notional_per_trade_usdt=float(os.getenv("MIN_NOTIONAL_PER_TRADE_USDT", "100")),
        max_notional_per_trade_usdt=float(
            os.getenv("MAX_NOTIONAL_PER_TRADE_USDT", os.getenv("USDT_PER_TRADE", "500"))
        ),
        enable_portfolio_risk_cap=get_env_bool("ENABLE_PORTFOLIO_RISK_CAP", True),
        max_side_open_risk_pct=float(os.getenv("MAX_SIDE_OPEN_RISK_PCT", "3")),
        max_total_open_risk_pct=float(os.getenv("MAX_TOTAL_OPEN_RISK_PCT", "5")),
        max_correlated_positions_per_side=int(
            os.getenv("MAX_CORRELATED_POSITIONS_PER_SIDE", "2")
        ),
        enable_breakeven_stop=get_env_bool("ENABLE_BREAKEVEN_STOP", True),
        breakeven_trigger_pct=float(os.getenv("BREAKEVEN_TRIGGER_PCT", "12")),
        breakeven_buffer_pct=float(os.getenv("BREAKEVEN_BUFFER_PCT", "1")),
        enable_partial_take_profit=get_env_bool("ENABLE_PARTIAL_TAKE_PROFIT", True),
        partial_take_profit_trigger_pct=float(
            os.getenv("PARTIAL_TAKE_PROFIT_TRIGGER_PCT", "18")
        ),
        partial_take_profit_close_ratio=float(
            os.getenv("PARTIAL_TAKE_PROFIT_CLOSE_RATIO", "0.5")
        ),
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
        equity_history_file=workdir
        / os.getenv("EQUITY_HISTORY_FILE", "runtime/account_equity_history.json"),
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
