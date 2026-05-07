import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import logging
import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from ai_select_futures_bot import (
    LONG,
    SHORT,
    LONG_STRATEGY_ID,
    SHORT_STRATEGY_ID,
    SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS,
    build_config,
    estimate_position_max_loss_usdt,
    live_side_from_amount,
    load_dotenv,
    migrate_state,
    select_broker_adapter,
)


def _fmt_decimal(value: Decimal | None, places: int = 4) -> str:
    if value is None:
        return "-"
    return f"{value:.{places}f}"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _coerce_timestamp_seconds(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if numeric <= 0:
        return None
    if numeric >= 100000000000:
        numeric /= 1000
    return numeric


def _average_interval_seconds(timestamps: list[float]) -> int | None:
    if len(timestamps) < 2:
        return None
    intervals = [
        current - previous
        for previous, current in zip(timestamps, timestamps[1:])
        if current >= previous
    ]
    if not intervals:
        return None
    return int(sum(intervals) / len(intervals))


def _build_runtime_stats(
    all_history: list[dict[str, Any]],
    equity_history: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    strategy_statuses: dict[str, dict[str, Any]],
    reset_cutoff_ms: int | None,
) -> dict[str, Any]:
    cutoff_seconds = (
        float(reset_cutoff_ms) / 1000 if reset_cutoff_ms not in (None, 0) else None
    )
    candidates: list[tuple[float, str]] = []

    def add_candidate(value: Any, source: str) -> None:
        timestamp = _coerce_timestamp_seconds(value)
        if timestamp is None:
            return
        if cutoff_seconds is not None and timestamp < cutoff_seconds:
            return
        candidates.append((timestamp, source))

    if cutoff_seconds is not None:
        candidates.append((cutoff_seconds, "reset_marker"))

    for event in all_history:
        add_candidate(event.get("timestamp"), "history")
    for point in equity_history:
        add_candidate(point.get("timestamp"), "equity_history")
    for row in positions:
        add_candidate(row.get("openedAt"), "position")
    for item in strategy_statuses.values():
        add_candidate(item.get("updatedAt"), "strategy_status")
        add_candidate(item.get("currentCandidateUpdatedAt"), "strategy_status")
        add_candidate(item.get("signalSnapshotUpdatedAt"), "signal_snapshot")

    if not candidates:
        return {
            "startedAt": None,
            "durationSeconds": None,
            "startSource": None,
        }

    started_at, start_source = min(candidates, key=lambda item: item[0])
    duration_seconds = max(0, int(time.time() - started_at))
    return {
        "startedAt": started_at,
        "durationSeconds": duration_seconds,
        "startSource": start_source,
    }


def _build_open_frequency(all_history: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = sorted(
        timestamp
        for timestamp in (
            _coerce_timestamp_seconds(event.get("timestamp"))
            for event in all_history
            if event.get("action") in ENTRY_ACTIONS
        )
        if timestamp is not None
    )
    now_ts = time.time()
    one_hour_ago = now_ts - 3600
    twenty_four_hours_ago = now_ts - (24 * 3600)
    seven_days_ago = now_ts - (7 * 24 * 3600)

    last_1h = [timestamp for timestamp in timestamps if timestamp >= one_hour_ago]
    last_24h = [timestamp for timestamp in timestamps if timestamp >= twenty_four_hours_ago]
    last_7d = [timestamp for timestamp in timestamps if timestamp >= seven_days_ago]

    return {
        "openCount1h": len(last_1h),
        "openCount24h": len(last_24h),
        "openCount7d": len(last_7d),
        "openCountTotal": len(timestamps),
        "avgIntervalSeconds24h": _average_interval_seconds(last_24h),
        "avgIntervalSeconds7d": _average_interval_seconds(last_7d),
        "lastOpenedAt": timestamps[-1] if timestamps else None,
    }


def _normalize_signal_rows(raw_rows: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not isinstance(raw_rows, list):
        return rows
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "rank": item.get("rank"),
                "sourceRank": item.get("sourceRank"),
                "asset": item.get("asset") or item.get("rawAsset"),
                "rawAsset": item.get("rawAsset"),
                "assetType": item.get("assetType"),
                "score": item.get("score"),
                "scoreLabel": item.get("scoreLabel"),
                "newsLabel": item.get("newsLabel"),
                "socialLabel": item.get("socialLabel"),
                "kolLabel": item.get("kolLabel"),
            }
        )
    return rows


def _load_signal_snapshot(path: Path) -> dict[str, Any]:
    rows = _normalize_signal_rows(_load_json(path, []))
    updated_at = path.stat().st_mtime if path.exists() else None
    return {
        "updatedAt": updated_at,
        "count": len(rows),
        "items": rows,
    }


def _load_signal_count_history(workdir: Path) -> list[dict[str, Any]]:
    rows = _load_json(workdir / "runtime" / "signal_count_history.json", [])
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    cutoff = time.time() - 24 * 3600
    for row in rows:
        if not isinstance(row, dict):
            continue
        timestamp = _coerce_timestamp_seconds(row.get("timestamp"))
        if timestamp is None or timestamp < cutoff:
            continue
        normalized.append(
            {
                "timestamp": timestamp,
                "longCount": int(row.get("longCount", 0) or 0),
                "shortCount": int(row.get("shortCount", 0) or 0),
            }
        )
    return sorted(normalized, key=lambda item: item["timestamp"])


def _signal_count_peak_stats(
    rows: list[dict[str, Any]],
    count_key: str,
) -> dict[str, Any]:
    if not rows:
        return {
            "peakCount": None,
            "startedAt": None,
            "endedAt": None,
            "durationMinutes": None,
            "sampleCount": 0,
        }
    peak_count = max(int(row.get(count_key, 0) or 0) for row in rows)
    best_start = None
    best_end = None
    best_duration = Decimal("-1")
    active_start = None
    active_last_index = None
    now_ts = time.time()

    def close_segment(next_timestamp: float | None) -> None:
        nonlocal active_start, active_last_index, best_start, best_end, best_duration
        if active_start is None or active_last_index is None:
            return
        end_ts = next_timestamp if next_timestamp is not None else now_ts
        if end_ts < active_start:
            end_ts = rows[active_last_index]["timestamp"]
        duration = Decimal(str(max(0, end_ts - active_start))) / Decimal("60")
        if duration > best_duration:
            best_duration = duration
            best_start = active_start
            best_end = end_ts
        active_start = None
        active_last_index = None

    for index, row in enumerate(rows):
        timestamp = float(row["timestamp"])
        count = int(row.get(count_key, 0) or 0)
        if count == peak_count:
            if active_start is None:
                active_start = timestamp
            active_last_index = index
            continue
        close_segment(timestamp)
    close_segment(None)

    return {
        "peakCount": peak_count,
        "startedAt": best_start,
        "endedAt": best_end,
        "durationMinutes": float(best_duration) if best_duration >= 0 else 0,
        "sampleCount": len(rows),
    }


def _build_signal_count_peak_stats(workdir: Path) -> dict[str, dict[str, Any]]:
    rows = _load_signal_count_history(workdir)
    return {
        LONG_STRATEGY_ID: _signal_count_peak_stats(rows, "longCount"),
        SHORT_STRATEGY_ID: _signal_count_peak_stats(rows, "shortCount"),
    }


def _signal_count_threshold_stats(
    rows: list[dict[str, Any]],
    count_key: str,
    threshold: int,
    enabled: bool,
    confirm_rounds: int,
) -> dict[str, Any]:
    if threshold <= 0:
        return {
            "enabled": bool(enabled),
            "threshold": threshold,
            "confirmRounds": confirm_rounds,
            "occurrenceCount": 0,
            "confirmedOccurrenceCount": 0,
            "totalDurationMinutes": 0,
            "longestDurationMinutes": None,
            "longestStartedAt": None,
            "longestEndedAt": None,
            "recentStartedAt": None,
            "recentEndedAt": None,
            "recentDurationMinutes": None,
            "sampleCount": len(rows),
            "segments": [],
        }
    segments: list[dict[str, Any]] = []
    active_start: float | None = None
    active_last_index: int | None = None
    active_samples = 0
    active_max_count: int | None = None
    active_min_count: int | None = None
    now_ts = time.time()

    def close_segment(next_timestamp: float | None) -> None:
        nonlocal active_start, active_last_index, active_samples, active_max_count, active_min_count
        if active_start is None or active_last_index is None:
            return
        end_ts = next_timestamp if next_timestamp is not None else now_ts
        if end_ts < active_start:
            end_ts = rows[active_last_index]["timestamp"]
        duration_minutes = float(
            Decimal(str(max(0, end_ts - active_start))) / Decimal("60")
        )
        segments.append(
            {
                "startedAt": active_start,
                "endedAt": end_ts,
                "durationMinutes": duration_minutes,
                "sampleCount": active_samples,
                "maxCount": active_max_count,
                "minCount": active_min_count,
                "confirmed": active_samples >= confirm_rounds,
            }
        )
        active_start = None
        active_last_index = None
        active_samples = 0
        active_max_count = None
        active_min_count = None

    for index, row in enumerate(rows):
        timestamp = float(row["timestamp"])
        count = int(row.get(count_key, 0) or 0)
        if count >= threshold:
            if active_start is None:
                active_start = timestamp
                active_samples = 0
                active_max_count = count
                active_min_count = count
            active_last_index = index
            active_samples += 1
            active_max_count = max(active_max_count or count, count)
            active_min_count = min(active_min_count or count, count)
            continue
        close_segment(timestamp)
    close_segment(None)

    longest = max(segments, key=lambda item: item["durationMinutes"], default=None)
    recent = max(segments, key=lambda item: item["startedAt"], default=None)
    return {
        "enabled": bool(enabled),
        "threshold": threshold,
        "confirmRounds": confirm_rounds,
        "occurrenceCount": len(segments),
        "confirmedOccurrenceCount": sum(
            1 for item in segments if int(item.get("sampleCount") or 0) >= confirm_rounds
        ),
        "totalDurationMinutes": sum(item["durationMinutes"] for item in segments),
        "longestDurationMinutes": longest.get("durationMinutes") if longest else None,
        "longestStartedAt": longest.get("startedAt") if longest else None,
        "longestEndedAt": longest.get("endedAt") if longest else None,
        "recentStartedAt": recent.get("startedAt") if recent else None,
        "recentEndedAt": recent.get("endedAt") if recent else None,
        "recentDurationMinutes": recent.get("durationMinutes") if recent else None,
        "sampleCount": len(rows),
        "segments": sorted(
            segments,
            key=lambda item: float(item.get("startedAt") or 0),
            reverse=True,
        ),
    }


def _build_signal_count_entry_gate_stats(
    workdir: Path,
    config: Any,
) -> dict[str, dict[str, Any]]:
    rows = _load_signal_count_history(workdir)
    enabled = bool(getattr(config, "enable_signal_count_entry_gate", False))
    return {
        LONG_STRATEGY_ID: _signal_count_threshold_stats(
            rows,
            "longCount",
            int(getattr(config, "min_long_signal_count_to_open", 0) or 0),
            enabled,
            SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS,
        ),
        SHORT_STRATEGY_ID: _signal_count_threshold_stats(
            rows,
            "shortCount",
            int(getattr(config, "min_short_signal_count_to_open", 0) or 0),
            enabled,
            SIGNAL_COUNT_ACTION_CONFIRM_ROUNDS,
        ),
    }


def _signal_row_identity(row: dict[str, Any]) -> tuple[Any, Any]:
    return (row.get("rank"), row.get("asset") or row.get("rawAsset"))


def _is_signal_snapshot_protected(
    current_items: list[dict[str, Any]],
    snapshot_items: list[dict[str, Any]],
    current_count: int,
    snapshot_count: int,
) -> bool:
    if int(current_count or 0) != int(snapshot_count or 0):
        return True
    if len(current_items) != len(snapshot_items):
        return True
    return [
        _signal_row_identity(row) for row in current_items
    ] != [
        _signal_row_identity(row) for row in snapshot_items
    ]


def _local_state_positions_by_symbol_side(state: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for position in state.get("positions", {}).values():
        symbol = position.get("contractSymbol")
        side = position.get("side")
        if not symbol or side not in {LONG, SHORT}:
            continue
        rows[(symbol, side)] = position
    return rows


def _build_local_positions(state: dict[str, Any], broker: Any) -> list[dict[str, Any]]:
    contract_symbols = [
        pos.get("contractSymbol")
        for pos in state.get("positions", {}).values()
        if pos.get("contractSymbol")
    ]
    try:
        mark_prices = broker.get_mark_prices(contract_symbols) if contract_symbols else {}
    except Exception as exc:
        logging.warning("local_mark_price_batch_failed: %s", exc)
        mark_prices = {}
    rows = []
    for pos in state.get("positions", {}).values():
        contract_symbol = pos.get("contractSymbol")
        if not contract_symbol:
            continue
        entry_long_count, entry_short_count = _entry_signal_counts(
            pos.get("side", LONG),
            pos.get("entryAudit"),
        )
        entry_price = Decimal(str(pos.get("entryPrice", "0")))
        mark_price = mark_prices.get(contract_symbol)
        if mark_price is None:
            try:
                mark_price = broker.get_mark_price(contract_symbol)
            except Exception as exc:
                logging.warning("local_mark_price_single_failed symbol=%s error=%s", contract_symbol, exc)
                mark_price = entry_price
        quantity = Decimal(str(pos.get("quantity", "0") or "0"))
        if quantity == Decimal("0") and pos.get("notionalUsdt") not in (None, "") and entry_price != Decimal("0"):
            quantity = Decimal(str(pos.get("notionalUsdt"))) / entry_price
        quantity = abs(quantity)
        position_usdt = Decimal(str(pos.get("notionalUsdt", "0") or "0"))
        current_value_usdt = mark_price * quantity if quantity else None
        if pos.get("side") == SHORT:
            unrealized_profit = (entry_price - mark_price) * quantity if quantity else None
        else:
            unrealized_profit = (mark_price - entry_price) * quantity if quantity else None
        pnl_pct = None
        if unrealized_profit is not None and position_usdt != Decimal("0"):
            pnl_pct = (unrealized_profit / position_usdt) * Decimal("100")
        rows.append(
            {
                "asset": pos.get("asset"),
                "contractSymbol": contract_symbol,
                "side": pos.get("side", LONG),
                "entryPrice": entry_price,
                "markPrice": mark_price,
                "quantity": quantity,
                "positionUsdt": position_usdt,
                "currentValueUsdt": current_value_usdt,
                "unrealizedProfit": unrealized_profit,
                "pnlPct": pnl_pct,
                "status": pos.get("status"),
                "openedAt": pos.get("openedAt"),
                "entryStrongLongCount": entry_long_count,
                "entryStrongShortCount": entry_short_count,
                "returnBasisUsdt": pos.get("returnBasisUsdt"),
                "stopLossPrice": pos.get("stopLossPrice"),
                "stopLossStatus": pos.get("stopLossStatus"),
                "stopLossMode": pos.get("stopLossMode"),
                "breakevenActivatedAt": pos.get("breakevenActivatedAt"),
                "partialTakeProfitDoneAt": pos.get("partialTakeProfitDoneAt"),
            }
        )
    return rows


def _build_side_summary(side: str, positions: list[dict[str, Any]], closed_history: list[dict[str, Any]]) -> dict[str, Any]:
    scoped_positions = [row for row in positions if row.get("side") == side]
    scoped_history = [event for event in closed_history if event.get("side") == side]
    unrealized = sum(
        (row.get("unrealizedProfit") or Decimal("0")) for row in scoped_positions
    )
    position_value = sum(
        (row.get("currentValueUsdt") or Decimal("0")) for row in scoped_positions
    )
    realized = sum(
        Decimal(
            event.get("netRealizedPnlUsdt")
            if event.get("netRealizedPnlUsdt") not in (None, "")
            else event.get("realizedPnlUsdt")
        )
        for event in scoped_history
        if event.get("netRealizedPnlUsdt") not in (None, "") or event.get("realizedPnlUsdt") not in (None, "")
    )
    return {
        "side": side,
        "label": "做多" if side == LONG else "做空",
        "openPositions": len(scoped_positions),
        "currentValueUsdt": str(position_value),
        "unrealizedProfit": str(unrealized),
        "closedCount": len(scoped_history),
        "closedWinCount": sum(1 for event in scoped_history if event.get("closeSide") == "win"),
        "closedLossCount": sum(1 for event in scoped_history if event.get("closeSide") == "loss"),
        "closedFlatCount": sum(1 for event in scoped_history if event.get("closeSide") == "flat"),
        "realizedPnlUsdt": str(realized),
    }


def _normalize_margin_mode(raw_value: Any) -> str | None:
    if raw_value in (None, ""):
        return None
    value = str(raw_value).strip().upper()
    if value == "CROSSED":
        return "CROSS"
    return value


def _format_margin_mode_label(raw_value: Any) -> str:
    value = _normalize_margin_mode(raw_value)
    if value == "ISOLATED":
        return "逐仓"
    if value == "CROSS":
        return "全仓"
    if value == "MIXED":
        return "混合"
    return value or "-"


def _apply_live_trading_setup_to_rules(
    items: list[dict[str, str]],
    config: Any,
    trading_setup: dict[str, Any],
) -> list[dict[str, str]]:
    long_leverage = trading_setup.get("longLeverage") or str(config.leverage)
    short_leverage = trading_setup.get("shortLeverage") or str(config.leverage)
    long_margin_mode = _format_margin_mode_label(
        trading_setup.get("longMarginMode") or config.required_margin_mode
    )
    short_margin_mode = _format_margin_mode_label(
        trading_setup.get("shortMarginMode") or config.required_margin_mode
    )
    adjusted: list[dict[str, str]] = []
    for item in items:
        if item.get("title") == "杠杆模式":
            adjusted.append(
                {
                    "title": "杠杆模式",
                    "value": (
                        f"做多 {long_leverage}X {long_margin_mode}，"
                        f"做空 {short_leverage}X {short_margin_mode}"
                    ),
                }
            )
            continue
        adjusted.append(item)
    return adjusted


def _build_rule_summary(config: Any) -> list[dict[str, str]]:
    cooldown_label = (
        f"{config.cooldown_minutes // 60} 小时"
        if config.cooldown_minutes % 60 == 0
        else f"{config.cooldown_minutes} 分钟"
    )
    leverage_text = f"{config.leverage}X {('逐仓' if config.required_margin_mode == 'ISOLATED' else config.required_margin_mode)}"
    activate_pct = config.profit_protection_activate_pct
    trail_pct = config.profit_protection_trail_pct
    signal_lost_text = (
        f"在榜继续持有，掉出榜单连续 {config.signal_lost_exit_confirm_rounds} 轮确认后平仓"
        if config.enable_signal_lost_exit
        else "在榜继续持有；掉出榜单只记录，不按掉榜自动平仓"
    )
    return [
        {"title": "多空主逻辑", "value": signal_lost_text},
        {"title": "杠杆模式", "value": f"做多 {leverage_text}，做空 {leverage_text}"},
        {"title": "冷却时间", "value": f"平仓后 {cooldown_label} 内不重开同方向"},
        {
            "title": "仓位上限",
            "value": f"单边上限：做多最多 {config.max_long_open_positions} 个，做空最多 {config.max_short_open_positions} 个；总持仓上限：合计最多 {config.max_total_open_positions} 个",
        },
        {
            "title": "流动性要求",
            "value": f"24h 合约成交额至少 {int(config.min_quote_volume_24h_usdt):,} USDT",
        },
        {
            "title": "趋势确认",
            "value": (
                f"{config.trend_interval} MA{config.trend_ma_period} 同方向才开仓，"
                f"若K线不足则回退到 {'/'.join(config.trend_fallback_intervals)}"
            )
            if config.enable_trend_confirmation
            else "已关闭",
        },
        {
            "title": "硬止损",
            "value": (
                f"开仓后按价格反向 {config.stop_loss_pct:g}% 挂止损单，"
                f"按 {config.leverage}x 约等于收益率 -{float(config.stop_loss_pct) * float(config.leverage):.1f}%"
            )
            if config.enable_stop_loss
            else "已关闭",
        },
        {
            "title": "利润保护",
            "value": f"收益达到 {activate_pct:.0f}% 后启动，保留峰值收益的 {100 - trail_pct:.0f}%，相对峰值回撤 {trail_pct:.0f}% 平仓"
            if config.enable_profit_protection
            else "已关闭",
        },
        {
            "title": "分级锁盈",
            "value": config.profit_lock_tiers if config.enable_profit_lock else "已关闭",
        },
        {
            "title": "时间退出",
            "value": f"超过 {config.max_hold_hours} 小时且收益不高于 {config.time_exit_min_pnl_pct}% 就平仓"
            if config.enable_time_exit
            else "已关闭",
        },
    ]


def _build_config_toggles(config: Any) -> list[dict[str, Any]]:
    fallback_intervals = "/".join(config.trend_fallback_intervals) or "-"
    stop_loss_roi_pct = float(config.stop_loss_pct) * float(config.leverage)
    return [
        {
            "key": "DRY_RUN",
            "label": "模拟下单",
            "enabled": config.dry_run,
            "detail": "开启后只记录信号和结果，不向交易所提交真实订单。",
        },
        {
            "key": "COOLDOWN_MINUTES",
            "label": "冷却时间",
            "type": "number",
            "value": int(config.cooldown_minutes),
            "min": 0,
            "step": 1,
            "unit": "分钟",
            "detail": "平仓后按这里填写的分钟数，限制同方向再次开仓。",
        },
        {
            "key": "ENABLE_MARGIN_USAGE_CAP",
            "label": "保证金占用上限",
            "enabled": config.enable_margin_usage_cap,
            "detail": f"保证金使用率达到 {config.max_margin_usage_pct}% 后停止新开仓。",
        },
        {
            "key": "ENABLE_VOLATILITY_FILTER",
            "label": "波动率过滤",
            "enabled": config.enable_volatility_filter,
            "detail": (
                f"{config.volatility_interval} 最近 {config.volatility_lookback_bars} 根K内，"
                f"单根振幅超过 {config.max_single_bar_range_pct}% 就跳过。"
            ),
        },
        {
            "key": "ENABLE_FUNDING_RATE_FILTER",
            "label": "资金费过滤",
            "enabled": config.enable_funding_rate_filter,
            "detail": f"资金费绝对值大于 {config.max_abs_funding_rate_pct}% 时跳过开仓。",
        },
        {
            "key": "ENABLE_CORRELATION_FILTER",
            "label": "相关性过滤",
            "enabled": config.enable_correlation_filter,
            "detail": (
                f"{config.correlation_interval} 最近 {config.correlation_lookback_bars} 根K的相关系数"
                f"达到 {config.correlation_threshold} 时跳过。"
            ),
        },
        {
            "key": "ENABLE_TREND_CONFIRMATION",
            "label": "趋势确认",
            "enabled": config.enable_trend_confirmation,
            "detail": (
                f"主周期 {config.trend_interval} MA{config.trend_ma_period}；"
                f"新币自动回退 {fallback_intervals}。"
            ),
        },
        {
            "key": "ENABLE_TIME_EXIT",
            "label": "时间止盈/止损",
            "enabled": config.enable_time_exit,
            "detail": (
                f"持仓超过 {config.max_hold_hours} 小时且收益不高于 "
                f"{config.time_exit_min_pnl_pct}% 时平仓。"
            ),
        },
        {
            "key": "ENABLE_STOP_LOSS",
            "label": "硬止损",
            "enabled": config.enable_stop_loss,
            "detail": f"开仓后按价格反向 {config.stop_loss_pct}% 挂硬止损；按 {config.leverage}x 约等于收益率 -{stop_loss_roi_pct:.1f}%。",
        },
        {
            "key": "STOP_LOSS_PCT",
            "label": "\u786c\u6b62\u635f\u4ef7\u683c\u6bd4\u4f8b",
            "type": "number",
            "value": float(config.stop_loss_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "showWhen": {"key": "ENABLE_STOP_LOSS"},
            "detail": f"\u8fd9\u91cc\u586b\u4ef7\u683c\u53cd\u5411\u767e\u5206\u6bd4\uff0c\u4e0d\u7528\u624b\u52a8\u4e58\u6760\u6746\uff1b\u7cfb\u7edf\u4f1a\u6309 {config.leverage}x \u81ea\u52a8\u6362\u7b97\uff0c\u5f53\u524d\u7ea6\u7b49\u4e8e\u6536\u76ca\u7387 -{stop_loss_roi_pct:.1f}%\u3002",
        },
        {
            "key": "ENABLE_PROFIT_LOCK",
            "label": "分级锁盈",
            "enabled": config.enable_profit_lock,
            "detail": f"按 {config.profit_lock_tiers} 进行阶梯锁盈。",
        },
        {
            "key": "ENABLE_PROFIT_PROTECTION",
            "label": "利润回撤保护",
            "enabled": config.enable_profit_protection,
            "detail": (
                f"收益达到 {config.profit_protection_activate_pct}% 后启动，"
                f"相对峰值回撤 {config.profit_protection_trail_pct}% 平仓。"
            ),
        },
        {
            "key": "PROFIT_PROTECTION_ACTIVATE_PCT",
            "label": "\u542f\u52a8\u6536\u76ca\u7387",
            "type": "number",
            "value": float(config.profit_protection_activate_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "showWhen": {"key": "ENABLE_PROFIT_PROTECTION"},
            "detail": "\u6536\u76ca\u7387\u5230\u8fbe\u8fd9\u4e2a\u9608\u503c\u540e\uff0c\u624d\u5f00\u59cb\u542f\u7528\u5229\u6da6\u56de\u64a4\u4fdd\u62a4\u3002",
        },
        {
            "key": "PROFIT_PROTECTION_TRAIL_PCT",
            "label": "\u5cf0\u503c\u56de\u64a4\u5e73\u4ed3",
            "type": "number",
            "value": float(config.profit_protection_trail_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "showWhen": {"key": "ENABLE_PROFIT_PROTECTION"},
            "detail": "\u542f\u52a8\u540e\uff0c\u82e5\u5f53\u524d\u6536\u76ca\u8f83\u5386\u53f2\u5cf0\u503c\u56de\u64a4\u5230\u8fd9\u4e2a\u6bd4\u4f8b\uff0c\u5219\u6267\u884c\u5e73\u4ed3\u3002",
        },
        {
            "key": "ENABLE_SIGNAL_LOST_EXIT",
            "label": "掉榜平仓",
            "enabled": config.enable_signal_lost_exit,
            "detail": (
                f"开启后，持仓币种连续 {config.signal_lost_exit_confirm_rounds} 轮不在当前强烈看多/看空榜单内才平仓；"
                "关闭后，掉榜只记录，不会因为掉榜自动平仓。"
            ),
        },
        {
            "key": "SIGNAL_LOST_EXIT_CONFIRM_ROUNDS",
            "label": "掉榜确认轮数",
            "type": "number",
            "value": int(config.signal_lost_exit_confirm_rounds),
            "min": 1,
            "step": 1,
            "unit": "轮",
            "showWhen": {"key": "ENABLE_SIGNAL_LOST_EXIT"},
            "detail": "持仓币种连续多少轮不在当前榜单里，才确认执行掉榜平仓。",
        },
        {
            "key": "ENABLE_SIGNAL_DROP_GUARD",
            "label": "信号骤降保护",
            "enabled": config.enable_signal_drop_guard,
            "detail": (
                f"候选数低于上一轮的 {config.signal_drop_guard_ratio:.0%}，"
                f"且少于 {config.signal_drop_guard_min_candidates} 个时暂停掉榜平仓。"
            ),
        },
        {
            "key": "SKIP_IF_MARGIN_MODE_UNAVAILABLE",
            "label": "保证金模式校验",
            "enabled": config.skip_if_margin_mode_unavailable,
            "detail": f"不支持 {config.required_margin_mode} 模式的合约直接跳过。",
        },
    ]


def _augment_rule_summary(items: list[dict[str, str]], config: Any) -> list[dict[str, str]]:
    rule = {
        "title": "最少强信号数",
        "value": (
            f"同方向强烈信号少于 {config.min_signal_count_to_open} 个时，不开该方向新仓"
            if config.enable_min_signal_count_filter
            else "已关闭"
        ),
    }
    entry_gate_rule = {
        "title": "榜单数量开仓",
        "value": (
            f"主流币做多达到 {config.min_mainstream_long_signal_count_to_open} 个，小市值做多达到 {config.min_smallcap_long_signal_count_to_open} 个；"
            f"主流币做空达到 {config.min_mainstream_short_signal_count_to_open} 个，小市值做空达到 {config.min_smallcap_short_signal_count_to_open} 个；"
            "达到门槛后连续确认 3 轮才执行"
            if config.enable_signal_count_entry_gate
            else "已关闭"
        ),
    }
    imbalance_rule = {
        "title": "\u591a\u7a7a\u5931\u8861\u8fc7\u6ee4",
        "value": (
            f"\u5f53\u591a\u7a7a\u4e24\u8fb9\u5f3a\u4fe1\u53f7\u90fd\u8fbe\u5230 {config.signal_imbalance_min_count} \u4e2a\uff0c"
            f"\u4e14\u4e00\u8fb9\u6570\u91cf\u8fbe\u5230\u53e6\u4e00\u8fb9\u7684 {config.signal_imbalance_ratio:g} \u500d\u65f6\uff0c"
            "\u6682\u505c\u5f31\u52bf\u65b9\u5411\u5f00\u65b0\u4ed3"
            if config.enable_signal_imbalance_filter
            else "\u5df2\u5173\u95ed"
        ),
    }
    exit_gate_rule = {
        "title": "榜单数量平仓",
        "value": (
            f"主流币做多少于 {config.mainstream_long_signal_count_to_close_below} 个，小市值做多少于 {config.smallcap_long_signal_count_to_close_below} 个；"
            f"主流币做空少于 {config.mainstream_short_signal_count_to_close_below} 个，小市值做空少于 {config.smallcap_short_signal_count_to_close_below} 个；"
            "跌破门槛后连续确认 3 轮才执行"
            if config.enable_signal_count_exit
            else "已关闭"
        ),
    }
    signal_lost_rule = {
        "title": "掉榜平仓",
        "value": (
            f"开启，连续 {config.signal_lost_exit_confirm_rounds} 轮掉出当前榜单后平仓"
            if config.enable_signal_lost_exit
            else "已关闭，掉出榜单只记录，不自动平仓"
        ),
    }
    post_entry_weak_rule = {
        "title": "开仓后弱化平仓",
        "value": (
            f"做多 {config.long_weak_exit_start_minutes}-{config.long_weak_exit_end_minutes} 分钟内，"
            f"若历史最高收益未到 {config.long_weak_exit_min_peak_pnl_pct:g}% 且强烈看多个数减少 "
            f"{config.long_weak_exit_signal_drop_count} 个或排名后移 {config.long_weak_exit_rank_drop} 名则平仓；"
            f"做空 {config.short_weak_exit_start_minutes}-{config.short_weak_exit_end_minutes} 分钟内，"
            f"若历史最高收益未跑出正收益且强烈看空减少 {config.short_weak_exit_signal_drop_count} 个"
            f"或强烈看多回升 {config.short_weak_exit_opposite_rebound_count} 个则平仓"
            if config.enable_post_entry_weak_exit
            else "已关闭"
        ),
    }
    new_rules = [
        {
            "title": "账户级熔断",
            "value": (
                f"当日亏损 {config.daily_loss_pause_pct:.1f}% / 连亏 {config.max_consecutive_losses} 笔 / "
                f"账户回撤 {config.max_account_drawdown_pct:.1f}% 任一触发，暂停开仓 {config.circuit_breaker_cooldown_minutes} 分钟"
                if config.enable_account_circuit_breaker
                else "已关闭"
            ),
        },
        {
            "title": "风险仓位",
            "value": (
                f"每笔按账户权益 {config.risk_per_trade_pct:.2f}% 风险预算反推仓位，"
                f"下限 {config.min_notional_per_trade_usdt:.0f}U，上限 {config.max_notional_per_trade_usdt:.0f}U"
                if config.enable_risk_position_sizing
                else f"固定每笔 {config.usdt_per_trade:.0f}U"
            ),
        },
        {
            "title": "组合风险",
            "value": (
                f"单边最大开口风险 {config.max_side_open_risk_pct:.1f}%，总开口风险 {config.max_total_open_risk_pct:.1f}%，"
                f"同向高相关持仓最多 {config.max_correlated_positions_per_side} 个"
                if config.enable_portfolio_risk_cap
                else "已关闭"
            ),
        },
        {
            "title": "保本与分批止盈",
            "value": (
                f"收益达到 {config.breakeven_trigger_pct:.1f}% 后止损抬到保本+{config.breakeven_buffer_pct:.1f}%；"
                f"达到 {config.partial_take_profit_trigger_pct:.1f}% 后先平 {config.partial_take_profit_close_ratio:.0%}"
                if config.enable_breakeven_stop or config.enable_partial_take_profit
                else "已关闭"
            ),
        },
    ]
    return [
        *items[:5],
        rule,
        entry_gate_rule,
        imbalance_rule,
        exit_gate_rule,
        signal_lost_rule,
        post_entry_weak_rule,
        *new_rules,
        *items[5:],
    ]


def _augment_config_toggles(items: list[dict[str, Any]], config: Any) -> list[dict[str, Any]]:
    toggle = {
        "key": "ENABLE_MIN_SIGNAL_COUNT_FILTER",
        "label": "最少强信号数过滤",
        "enabled": config.enable_min_signal_count_filter,
        "detail": f"同方向强烈看多/看空少于 {config.min_signal_count_to_open} 个时，不开该方向新仓。",
    }
    entry_gate_controls = [
        {
            "key": "ENABLE_SIGNAL_COUNT_ENTRY_GATE",
            "label": "榜单数量开仓",
            "enabled": config.enable_signal_count_entry_gate,
            "detail": "按强烈看多、强烈看空的列表数量控制开仓，并且主流币和小市值币使用不同门槛。",
        },
        {
            "key": "ENABLE_SIGNAL_COUNT_EXIT",
            "label": "榜单数量平仓",
            "enabled": config.enable_signal_count_exit,
            "detail": "开启后，当前榜单数量跌破对应平仓门槛时，连续确认 3 轮后平掉对应分组持仓。",
        },
        {
            "key": "MAINSTREAM_LONG_SIGNAL_COUNT_GATE_PAIR",
            "label": "主流币做多门槛",
            "type": "pair",
            "fields": [
                {
                    "key": "MIN_MAINSTREAM_LONG_SIGNAL_COUNT_TO_OPEN",
                    "label": "开仓门槛",
                    "value": int(config.min_mainstream_long_signal_count_to_open),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
                {
                    "key": "MAINSTREAM_LONG_SIGNAL_COUNT_TO_CLOSE_BELOW",
                    "label": "平仓门槛",
                    "value": int(config.mainstream_long_signal_count_to_close_below),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
            ],
            "showWhen": {"key": "ENABLE_SIGNAL_COUNT_ENTRY_GATE"},
            "detail": "主流币做多：达到开仓门槛才开多；低于平仓门槛时按榜单数量平仓。",
        },
        {
            "key": "SMALLCAP_LONG_SIGNAL_COUNT_GATE_PAIR",
            "label": "小市值币做多门槛",
            "type": "pair",
            "fields": [
                {
                    "key": "MIN_SMALLCAP_LONG_SIGNAL_COUNT_TO_OPEN",
                    "label": "开仓门槛",
                    "value": int(config.min_smallcap_long_signal_count_to_open),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
                {
                    "key": "SMALLCAP_LONG_SIGNAL_COUNT_TO_CLOSE_BELOW",
                    "label": "平仓门槛",
                    "value": int(config.smallcap_long_signal_count_to_close_below),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
            ],
            "showWhen": {"key": "ENABLE_SIGNAL_COUNT_ENTRY_GATE"},
            "detail": "小市值币做多：达到开仓门槛才开多；低于平仓门槛时按榜单数量平仓。",
        },
        {
            "key": "MAINSTREAM_SHORT_SIGNAL_COUNT_GATE_PAIR",
            "label": "主流币做空门槛",
            "type": "pair",
            "fields": [
                {
                    "key": "MIN_MAINSTREAM_SHORT_SIGNAL_COUNT_TO_OPEN",
                    "label": "开仓门槛",
                    "value": int(config.min_mainstream_short_signal_count_to_open),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
                {
                    "key": "MAINSTREAM_SHORT_SIGNAL_COUNT_TO_CLOSE_BELOW",
                    "label": "平仓门槛",
                    "value": int(config.mainstream_short_signal_count_to_close_below),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
            ],
            "showWhen": {"key": "ENABLE_SIGNAL_COUNT_ENTRY_GATE"},
            "detail": "主流币做空：达到开仓门槛才开空；低于平仓门槛时按榜单数量平仓。",
        },
        {
            "key": "SMALLCAP_SHORT_SIGNAL_COUNT_GATE_PAIR",
            "label": "小市值币做空门槛",
            "type": "pair",
            "fields": [
                {
                    "key": "MIN_SMALLCAP_SHORT_SIGNAL_COUNT_TO_OPEN",
                    "label": "开仓门槛",
                    "value": int(config.min_smallcap_short_signal_count_to_open),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
                {
                    "key": "SMALLCAP_SHORT_SIGNAL_COUNT_TO_CLOSE_BELOW",
                    "label": "平仓门槛",
                    "value": int(config.smallcap_short_signal_count_to_close_below),
                    "min": 0,
                    "step": 1,
                    "unit": "个",
                },
            ],
            "showWhen": {"key": "ENABLE_SIGNAL_COUNT_ENTRY_GATE"},
            "detail": "小市值币做空：达到开仓门槛才开空；低于平仓门槛时按榜单数量平仓。",
        },
        {
            "key": "MAINSTREAM_ASSETS",
            "label": "主流币名单",
            "type": "text",
            "value": ",".join(config.mainstream_assets),
            "unit": "",
            "showWhen": {"key": "ENABLE_SIGNAL_COUNT_ENTRY_GATE"},
            "detail": "用英文逗号分隔。名单内按主流币门槛，其余币按小市值币门槛。",
        },
    ]
    imbalance_controls = [
        {
            "key": "ENABLE_SIGNAL_IMBALANCE_FILTER",
            "label": "\u591a\u7a7a\u5931\u8861\u8fc7\u6ee4",
            "enabled": config.enable_signal_imbalance_filter,
            "detail": (
                f"\u5f53\u591a\u7a7a\u4e24\u8fb9\u5f3a\u4fe1\u53f7\u90fd\u8fbe\u5230 {config.signal_imbalance_min_count} \u4e2a\uff0c"
                f"\u4e14\u4e00\u8fb9\u6570\u91cf\u8fbe\u5230\u53e6\u4e00\u8fb9\u7684 {config.signal_imbalance_ratio:g} \u500d\u65f6\uff0c"
                "\u6682\u505c\u5f31\u52bf\u65b9\u5411\u5f00\u65b0\u4ed3\u3002"
            ),
        },
        {
            "key": "SIGNAL_IMBALANCE_MIN_COUNT",
            "label": "\u5931\u8861\u8d77\u7b97\u6570\u91cf",
            "type": "number",
            "value": int(config.signal_imbalance_min_count),
            "min": 1,
            "step": 1,
            "unit": "\u4e2a",
            "showWhen": {"key": "ENABLE_SIGNAL_IMBALANCE_FILTER"},
            "detail": "\u53ea\u6709\u591a\u7a7a\u4e24\u8fb9\u5f3a\u4fe1\u53f7\u90fd\u8fbe\u5230\u8fd9\u4e2a\u6570\u91cf\u540e\uff0c\u624d\u542f\u7528\u591a\u7a7a\u5931\u8861\u8fc7\u6ee4\u3002",
        },
        {
            "key": "SIGNAL_IMBALANCE_RATIO",
            "label": "\u5931\u8861\u500d\u6570",
            "type": "number",
            "value": float(config.signal_imbalance_ratio),
            "min": 1,
            "step": 0.1,
            "unit": "\u500d",
            "showWhen": {"key": "ENABLE_SIGNAL_IMBALANCE_FILTER"},
            "detail": "\u5f53\u5f3a\u52bf\u4e00\u8fb9\u6570\u91cf\u8fbe\u5230\u5f31\u52bf\u4e00\u8fb9\u8fd9\u4e2a\u500d\u6570\u65f6\uff0c\u6682\u505c\u5f31\u52bf\u4e00\u8fb9\u5f00\u65b0\u4ed3\u3002",
        },
    ]
    position_limit_controls = [
        {
            "key": "MAX_TOTAL_OPEN_POSITIONS",
            "label": "\u603b\u6301\u4ed3\u4e0a\u9650",
            "type": "number",
            "value": int(config.max_total_open_positions),
            "min": 0,
            "step": 1,
            "unit": "\u4e2a",
            "detail": "\u5168\u90e8\u4ed3\u4f4d\u5408\u8ba1\u6700\u591a\u5141\u8bb8\u7684\u540c\u65f6\u6301\u4ed3\u6570\u91cf\u3002",
        },
        {
            "key": "MAX_LONG_OPEN_POSITIONS",
            "label": "\u505a\u591a\u6301\u4ed3\u4e0a\u9650",
            "type": "number",
            "value": int(config.max_long_open_positions),
            "min": 0,
            "step": 1,
            "unit": "\u4e2a",
            "detail": "\u505a\u591a\u65b9\u5411\u6700\u591a\u5141\u8bb8\u7684\u540c\u65f6\u6301\u4ed3\u6570\u91cf\u3002",
        },
        {
            "key": "MAX_SHORT_OPEN_POSITIONS",
            "label": "\u505a\u7a7a\u6301\u4ed3\u4e0a\u9650",
            "type": "number",
            "value": int(config.max_short_open_positions),
            "min": 0,
            "step": 1,
            "unit": "\u4e2a",
            "detail": "\u505a\u7a7a\u65b9\u5411\u6700\u591a\u5141\u8bb8\u7684\u540c\u65f6\u6301\u4ed3\u6570\u91cf\u3002",
        },
    ]
    risk_controls = [
        {
            "key": "ENABLE_ACCOUNT_CIRCUIT_BREAKER",
            "label": "账户级熔断",
            "enabled": config.enable_account_circuit_breaker,
            "detail": "触发当日亏损、连续亏损或账户回撤阈值后，只暂停新开仓，不影响已有仓位平仓。",
        },
        {
            "key": "DAILY_LOSS_PAUSE_PCT",
            "label": "当日亏损熔断",
            "type": "number",
            "value": float(config.daily_loss_pause_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "当天已实现净亏损达到这个比例后暂停开新仓。",
        },
        {
            "key": "MAX_CONSECUTIVE_LOSSES",
            "label": "连续亏损熔断",
            "type": "number",
            "value": int(config.max_consecutive_losses),
            "min": 0,
            "step": 1,
            "unit": "笔",
            "detail": "最近连续亏损达到这个笔数后暂停开新仓。",
        },
        {
            "key": "MAX_ACCOUNT_DRAWDOWN_PCT",
            "label": "账户回撤熔断",
            "type": "number",
            "value": float(config.max_account_drawdown_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "账户权益相对历史峰值回撤达到这个比例后暂停开新仓。",
        },
        {
            "key": "CIRCUIT_BREAKER_COOLDOWN_MINUTES",
            "label": "熔断暂停时间",
            "type": "number",
            "value": int(config.circuit_breaker_cooldown_minutes),
            "min": 0,
            "step": 1,
            "unit": "分钟",
            "detail": "熔断触发后至少暂停开仓的时间。",
        },
        {
            "key": "ENABLE_RISK_POSITION_SIZING",
            "label": "风险仓位",
            "enabled": config.enable_risk_position_sizing,
            "detail": "按账户权益和止损距离反推下单金额，不再只用固定 USDT。",
        },
        {
            "key": "RISK_PER_TRADE_PCT",
            "label": "单笔风险预算",
            "type": "number",
            "value": float(config.risk_per_trade_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "每笔止损最多亏账户权益的比例。",
        },
        {
            "key": "MIN_NOTIONAL_PER_TRADE_USDT",
            "label": "最小下单金额",
            "type": "number",
            "value": float(config.min_notional_per_trade_usdt),
            "min": 0,
            "step": 1,
            "unit": "USDT",
            "detail": "风险仓位计算后低于这个值会按这个值下单。",
        },
        {
            "key": "ENABLE_PORTFOLIO_RISK_CAP",
            "label": "组合风险限制",
            "enabled": config.enable_portfolio_risk_cap,
            "detail": "限制单边/总风险敞口，并限制同向高相关持仓数量。",
        },
        {
            "key": "MAX_SIDE_OPEN_RISK_PCT",
            "label": "单边风险上限",
            "type": "number",
            "value": float(config.max_side_open_risk_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "做多或做空任一方向的预估最大止损风险上限。",
        },
        {
            "key": "MAX_TOTAL_OPEN_RISK_PCT",
            "label": "总风险上限",
            "type": "number",
            "value": float(config.max_total_open_risk_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "所有持仓合计预估最大止损风险上限。",
        },
        {
            "key": "MAX_CORRELATED_POSITIONS_PER_SIDE",
            "label": "同向相关仓位上限",
            "type": "number",
            "value": int(config.max_correlated_positions_per_side),
            "min": 0,
            "step": 1,
            "unit": "个",
            "detail": "候选币与同向已有持仓高度相关时，最多允许的相关持仓数量。",
        },
        {
            "key": "ENABLE_BREAKEVEN_STOP",
            "label": "保本止损",
            "enabled": config.enable_breakeven_stop,
            "detail": "盈利达到阈值后，把止损抬到开仓成本附近。",
        },
        {
            "key": "BREAKEVEN_TRIGGER_PCT",
            "label": "保本触发收益率",
            "type": "number",
            "value": float(config.breakeven_trigger_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "持仓收益率达到这个值后触发保本止损。",
        },
        {
            "key": "BREAKEVEN_BUFFER_PCT",
            "label": "保本缓冲",
            "type": "number",
            "value": float(config.breakeven_buffer_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "止损抬到成本后额外锁住的收益率。",
        },
        {
            "key": "ENABLE_PARTIAL_TAKE_PROFIT",
            "label": "分批止盈",
            "enabled": config.enable_partial_take_profit,
            "detail": "盈利达到阈值后先减掉一部分仓位，剩余仓位继续跑。",
        },
        {
            "key": "PARTIAL_TAKE_PROFIT_TRIGGER_PCT",
            "label": "分批止盈触发",
            "type": "number",
            "value": float(config.partial_take_profit_trigger_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "detail": "持仓收益率达到这个值后执行一次分批止盈。",
        },
        {
            "key": "PARTIAL_TAKE_PROFIT_CLOSE_RATIO",
            "label": "分批止盈比例",
            "type": "number",
            "value": float(config.partial_take_profit_close_ratio),
            "min": 0.01,
            "step": 0.01,
            "unit": "0-1",
            "detail": "例如 0.5 表示先平掉一半仓位。",
        },
    ]
    exit_gate_controls: list[dict[str, Any]] = []
    post_entry_weak_exit_controls = [
        {
            "key": "ENABLE_POST_ENTRY_WEAK_EXIT",
            "label": "开仓后弱化平仓",
            "enabled": config.enable_post_entry_weak_exit,
            "detail": "开仓后一段时间内，如果一直没跑出目标收益，同时榜单数量或排名明显走弱，就提前平仓。",
        },
        {
            "key": "LONG_WEAK_EXIT_START_MINUTES",
            "label": "做多观察开始",
            "type": "number",
            "value": int(config.long_weak_exit_start_minutes),
            "min": 0,
            "step": 1,
            "unit": "分钟",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做多开仓后，从这个分钟数开始检查是否需要按弱化规则提前平仓。",
        },
        {
            "key": "LONG_WEAK_EXIT_END_MINUTES",
            "label": "做多观察结束",
            "type": "number",
            "value": int(config.long_weak_exit_end_minutes),
            "min": 0,
            "step": 1,
            "unit": "分钟",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做多开仓后，到这个分钟数为止都参与弱化平仓判断。",
        },
        {
            "key": "LONG_WEAK_EXIT_MIN_PEAK_PNL_PCT",
            "label": "做多最低达标收益率",
            "type": "number",
            "value": float(config.long_weak_exit_min_peak_pnl_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做多在观察窗口内，历史最高收益至少达到这个值，才算真正发动成功。",
        },
        {
            "key": "LONG_WEAK_EXIT_SIGNAL_DROP_COUNT",
            "label": "做多榜单减少个数",
            "type": "number",
            "value": int(config.long_weak_exit_signal_drop_count),
            "min": 1,
            "step": 1,
            "unit": "个",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做多开仓后，强烈看多个数比开仓时减少达到这个个数时，满足弱化条件之一。",
        },
        {
            "key": "LONG_WEAK_EXIT_RANK_DROP",
            "label": "做多排名后移名次",
            "type": "number",
            "value": int(config.long_weak_exit_rank_drop),
            "min": 1,
            "step": 1,
            "unit": "名",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做多开仓后，当前排名比开仓时后移到这个名次数，满足弱化条件之一。",
        },
        {
            "key": "SHORT_WEAK_EXIT_START_MINUTES",
            "label": "做空观察开始",
            "type": "number",
            "value": int(config.short_weak_exit_start_minutes),
            "min": 0,
            "step": 1,
            "unit": "分钟",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做空开仓后，从这个分钟数开始检查是否需要按弱化规则提前平仓。",
        },
        {
            "key": "SHORT_WEAK_EXIT_END_MINUTES",
            "label": "做空观察结束",
            "type": "number",
            "value": int(config.short_weak_exit_end_minutes),
            "min": 0,
            "step": 1,
            "unit": "分钟",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做空开仓后，到这个分钟数为止都参与弱化平仓判断。",
        },
        {
            "key": "SHORT_WEAK_EXIT_MIN_PEAK_PNL_PCT",
            "label": "做空最低达标收益率",
            "type": "number",
            "value": float(config.short_weak_exit_min_peak_pnl_pct),
            "min": 0,
            "step": 0.1,
            "unit": "%",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做空在观察窗口内，历史最高收益至少要高于这个值；填 0 表示至少跑出正收益。",
        },
        {
            "key": "SHORT_WEAK_EXIT_SIGNAL_DROP_COUNT",
            "label": "做空榜单减少个数",
            "type": "number",
            "value": int(config.short_weak_exit_signal_drop_count),
            "min": 1,
            "step": 1,
            "unit": "个",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做空开仓后，强烈看空个数比开仓时减少达到这个个数时，满足弱化条件之一。",
        },
        {
            "key": "SHORT_WEAK_EXIT_OPPOSITE_REBOUND_COUNT",
            "label": "做空对侧回升个数",
            "type": "number",
            "value": int(config.short_weak_exit_opposite_rebound_count),
            "min": 1,
            "step": 1,
            "unit": "个",
            "showWhen": {"key": "ENABLE_POST_ENTRY_WEAK_EXIT"},
            "detail": "做空开仓后，强烈看多个数比开仓时回升达到这个个数时，满足弱化条件之一。",
        },
    ]
    return (
        [
            items[0],
            toggle,
            *entry_gate_controls,
            *imbalance_controls,
            *position_limit_controls,
            *risk_controls,
            *exit_gate_controls,
            *post_entry_weak_exit_controls,
            *items[1:],
        ]
        if items
        else [
            toggle,
            *entry_gate_controls,
            *imbalance_controls,
            *position_limit_controls,
            *risk_controls,
            *exit_gate_controls,
            *post_entry_weak_exit_controls,
        ]
    )


def _format_unopened_detail(item: dict[str, Any]) -> str | None:
    reason = item.get("reason")
    if reason == "contract_not_trading":
        contract_symbol = item.get("contractSymbol") or item.get("asset")
        contract_status = item.get("contractStatus")
        if contract_symbol and contract_status:
            return f"{contract_symbol} 当前状态 {contract_status}"
        if contract_symbol:
            return str(contract_symbol)
    if reason == "signal_count_too_low":
        current_count = item.get("currentSignalCount")
        min_required = item.get("minSignalCountToOpen")
        if current_count not in (None, "") and min_required not in (None, ""):
            return f"当前强信号 {current_count} 个，至少需要 {min_required} 个"
    if reason == "signal_count_entry_gate_blocked":
        current_count = item.get("currentSignalCount")
        required_count = item.get("requiredSignalCount")
        if current_count not in (None, "") and required_count not in (None, ""):
            return f"当前强信号 {current_count} 个，榜单数量开仓至少需要 {required_count} 个"
    if reason == "signal_count_entry_confirming":
        current_count = item.get("currentSignalCount")
        required_count = item.get("requiredSignalCount")
        rounds = item.get("confirmationRounds")
        required_rounds = item.get("confirmationRequiredRounds")
        if current_count not in (None, "") and required_count not in (None, ""):
            return (
                f"当前强信号 {current_count} 个，已达到开仓门槛 {required_count} 个；"
                f"确认 {rounds or 0}/{required_rounds or 3} 轮"
            )
    if reason == "trend_not_confirmed":
        close = item.get("close")
        ma = item.get("ma")
        interval = item.get("trendInterval")
        if close not in (None, "") and ma not in (None, ""):
            if interval:
                return f"{interval} 现价 {close}，MA {ma}"
            return f"现价 {close}，MA {ma}"
    if reason == "trend_data_unavailable":
        intervals = item.get("trendIntervalsTried")
        if intervals not in (None, ""):
            return f"可用K线不足，已尝试 {intervals}"
    if reason == "low_24h_quote_volume":
        quote_volume = item.get("quoteVolume24hUsdt")
        min_required = item.get("minRequiredQuoteVolume24hUsdt")
        if quote_volume not in (None, "") and min_required not in (None, ""):
            return f"24h 成交额 {quote_volume}，要求至少 {min_required}"
    if reason == "margin_usage_limit":
        current_usage = item.get("currentMarginUsagePct")
        max_usage = item.get("maxMarginUsagePct")
        if current_usage not in (None, "") and max_usage not in (None, ""):
            return f"当前保证金使用率 {current_usage}% ，上限 {max_usage}%"
    if reason == "account_circuit_breaker":
        until_ts = item.get("circuitBreakerUntil")
        reasons = ",".join(item.get("circuitBreakerReasons") or [])
        if until_ts not in (None, ""):
            return f"账户熔断生效到 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(until_ts)))}，原因: {reasons or '-'}"
    if reason == "side_risk_limit":
        return f"预计单边风险 {item.get('projectedSideRiskPct', '-')}%，上限 {item.get('maxSideOpenRiskPct', '-')}%"
    if reason == "portfolio_risk_limit":
        return f"预计总风险 {item.get('projectedTotalRiskPct', '-')}%，上限 {item.get('maxTotalOpenRiskPct', '-')}"
    if reason == "correlated_cluster_limit":
        return f"同向高相关持仓 {item.get('correlatedMatchCount', '-')} 个，上限 {item.get('maxCorrelatedPositionsPerSide', '-')}"
    if reason == "correlated_with_existing":
        correlated_with = item.get("correlatedWith")
        corr = item.get("correlation")
        if correlated_with and corr not in (None, ""):
            return f"与 {correlated_with} 相关性 {corr}"
    if reason == "cooldown":
        cooldown_until = item.get("cooldownUntil")
        if cooldown_until not in (None, ""):
            return f"冷却到 {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(cooldown_until)))}"
    detail = item.get("detail")
    if detail not in (None, ""):
        return str(detail)
    return None


def _build_unopened_candidates(strategy_statuses: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for strategy_id in (LONG_STRATEGY_ID, SHORT_STRATEGY_ID):
        strategy = strategy_statuses.get(strategy_id, {})
        side = strategy.get("side", LONG)
        strategy_name = strategy.get("name")
        for item in strategy.get("latestDecisions", []):
            if item.get("action") != "skip":
                continue
            rows.append(
                {
                    "strategyId": strategy_id,
                    "strategyName": strategy_name,
                    "side": side,
                    "asset": item.get("asset"),
                    "reason": item.get("reason"),
                    "detail": _format_unopened_detail(item),
                }
            )
    return rows


def _build_active_cooldowns(
    state: dict[str, Any], cooldown_minutes: int, positions: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if cooldown_minutes <= 0:
        return []
    now = time.time()
    cooldown_seconds = cooldown_minutes * 60
    open_keys = {
        (str(row.get("asset") or "").upper(), row.get("side"))
        for row in positions
        if row.get("asset") and row.get("side")
    }
    latest_exit_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for event in state.get("history", []):
        action = event.get("action")
        if action not in {"exit_long", "exit_short"}:
            continue
        side = LONG if action == "exit_long" else SHORT
        asset = str(event.get("asset") or "").upper()
        if not asset:
            continue
        key = (asset, side)
        timestamp = float(event.get("timestamp", 0) or 0)
        previous = latest_exit_by_key.get(key)
        if previous is None or timestamp > float(previous.get("timestamp", 0) or 0):
            latest_exit_by_key[key] = {
                "asset": asset,
                "side": side,
                "timestamp": timestamp,
                "reason": event.get("reason"),
                "contractSymbol": event.get("contractSymbol") or f"{asset}USDT",
                "cooldownUntilOverride": event.get("cooldownUntilOverride"),
            }
    rows: list[dict[str, Any]] = []
    for key, item in latest_exit_by_key.items():
        if key in open_keys:
            continue
        cooldown_until_override = item.get("cooldownUntilOverride")
        if cooldown_until_override not in (None, ""):
            cooldown_until = float(cooldown_until_override)
        else:
            cooldown_until = item["timestamp"] + cooldown_seconds
        remaining_seconds = int(cooldown_until - now)
        if remaining_seconds <= 0:
            continue
        rows.append(
            {
                "asset": item["asset"],
                "contractSymbol": item.get("contractSymbol"),
                "side": item["side"],
                "reason": item.get("reason") or "cooldown",
                "cooldownUntil": cooldown_until,
                "remainingSeconds": remaining_seconds,
            }
        )
    rows.sort(key=lambda item: item.get("cooldownUntil", 0))
    return rows


def _build_readiness(report_source: str, account_snapshot: dict[str, Any], state: dict[str, Any], strategy_statuses: dict[str, Any], config: Any, positions: list[dict[str, Any]], closed_history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    long_closed = [event for event in closed_history if event.get("side") == LONG]
    short_closed = [event for event in closed_history if event.get("side") == SHORT]
    recent_closed = sorted(closed_history, key=lambda item: item.get("timestamp", 0), reverse=True)[:20]
    recent_unconfirmed = [
        event for event in recent_closed if event.get("confirmedClosed") is False
    ]

    strategy_fresh = True
    now = time.time()
    freshness_limit = max(1800, config.poll_interval_seconds * 2 + 60)
    for item in strategy_statuses.values():
        updated_at = item.get("updatedAt")
        if updated_at in (None, "") or now - float(updated_at) > freshness_limit:
            strategy_fresh = False
            break

    live_position_count = account_snapshot.get("positionCount") if account_snapshot else None
    state_position_count = len(state.get("positions", {}))
    count_aligned = (
        live_position_count == len(positions)
        if report_source == "binance_testnet" and live_position_count is not None
        else True
    )

    return [
        {
            "title": "已连接币安模拟盘",
            "ok": report_source == "binance_testnet" and not config.dry_run,
            "detail": "当前看板和执行都来自 Binance Testnet",
        },
        {
            "title": "机器人在持续更新",
            "ok": strategy_fresh,
            "detail": f"轮询间隔 {config.poll_interval_seconds // 60} 分钟，最近状态需要持续刷新",
        },
        {
            "title": "持仓数据对得上",
            "ok": count_aligned,
            "detail": f"交易所持仓 {live_position_count if live_position_count is not None else '-'}，看板持仓 {len(positions)}，本地记录 {state_position_count}",
        },
        {
            "title": "平仓确认正常",
            "ok": len(recent_unconfirmed) == 0,
            "detail": f"最近 20 笔平仓里未确认 {len(recent_unconfirmed)} 笔",
        },
        {
            "title": "样本量达到观察门槛",
            "ok": len(closed_history) >= 100,
            "detail": f"累计已平仓 {len(closed_history)} / 100",
        },
        {
            "title": "做多样本够了",
            "ok": len(long_closed) >= 30,
            "detail": f"做多已平仓 {len(long_closed)} / 30",
        },
        {
            "title": "做空样本够了",
            "ok": len(short_closed) >= 30,
            "detail": f"做空已平仓 {len(short_closed)} / 30",
        },
    ]


def _build_risk_stats(
    positions: list[dict[str, Any]],
    closed_history: list[dict[str, Any]],
    account_snapshot: dict[str, Any] | None,
    equity_history: list[dict[str, Any]],
    config: Any,
) -> dict[str, Any]:
    ordered_closes = sorted(closed_history, key=lambda item: item.get("timestamp", 0))
    pnl_values: list[Decimal] = [
        Decimal(
            str(
                event.get("netRealizedPnlUsdt")
                if event.get("netRealizedPnlUsdt") not in (None, "")
                else event.get("realizedPnlUsdt")
            )
        )
        for event in ordered_closes
        if event.get("netRealizedPnlUsdt") not in (None, "") or event.get("realizedPnlUsdt") not in (None, "")
    ]

    gross_profit = sum((value for value in pnl_values if value > 0), Decimal("0"))
    gross_loss_abs = sum((abs(value) for value in pnl_values if value < 0), Decimal("0"))
    wins = [value for value in pnl_values if value > 0]
    losses = [abs(value) for value in pnl_values if value < 0]
    flats = [value for value in pnl_values if value == 0]

    cumulative = Decimal("0")
    peak = Decimal("0")
    strategy_max_drawdown = Decimal("0")
    strategy_max_drawdown_pct = None
    current_loss_streak = 0
    current_win_streak = 0
    max_loss_streak = 0
    max_win_streak = 0

    for value in pnl_values:
        cumulative += value
        if cumulative > peak:
            peak = cumulative
        drawdown = peak - cumulative
        if drawdown > strategy_max_drawdown:
            strategy_max_drawdown = drawdown

        if value < 0:
            current_loss_streak += 1
            current_win_streak = 0
        elif value > 0:
            current_win_streak += 1
            current_loss_streak = 0
        else:
            current_win_streak = 0
            current_loss_streak = 0
        max_loss_streak = max(max_loss_streak, current_loss_streak)
        max_win_streak = max(max_win_streak, current_win_streak)

    total_trades = len(pnl_values)
    win_rate_pct = None
    if total_trades > 0:
        win_rate_pct = (Decimal(len(wins)) / Decimal(total_trades)) * Decimal("100")

    avg_win = (gross_profit / Decimal(len(wins))) if wins else None
    avg_loss = (gross_loss_abs / Decimal(len(losses))) if losses else None
    avg_win_loss_ratio = None
    if avg_win is not None and avg_loss not in (None, Decimal("0")):
        avg_win_loss_ratio = avg_win / avg_loss

    profit_factor = None
    if gross_loss_abs > 0:
        profit_factor = gross_profit / gross_loss_abs
    elif gross_profit > 0:
        profit_factor = Decimal("999")

    current_unrealized = sum(
        (row.get("unrealizedProfit") or Decimal("0")) for row in positions
    )
    current_position_value = sum(
        (row.get("currentValueUsdt") or Decimal("0")) for row in positions
    )
    current_wallet_balance = None
    if account_snapshot:
        wallet_balance_raw = account_snapshot.get("totalWalletBalance")
        if wallet_balance_raw not in (None, ""):
            try:
                current_wallet_balance = Decimal(str(wallet_balance_raw))
            except Exception:
                current_wallet_balance = None
    current_unrealized_pct = None
    if current_wallet_balance not in (None, Decimal("0")):
        current_unrealized_pct = (current_unrealized / current_wallet_balance) * Decimal("100")
    max_historical_unrealized_loss = None
    max_historical_unrealized_loss_pct = None
    worst_historical_unrealized = None
    stop_loss_pct = Decimal(str(config.stop_loss_pct))
    open_risk_by_side = {LONG: Decimal("0"), SHORT: Decimal("0")}
    for row in positions:
        try:
            estimated_risk = estimate_position_max_loss_usdt(row, stop_loss_pct)
        except Exception:
            estimated_risk = None
        if estimated_risk is None:
            continue
        open_risk_by_side[row.get("side", LONG)] = (
            open_risk_by_side.get(row.get("side", LONG), Decimal("0")) + estimated_risk
        )
    open_risk_total = open_risk_by_side[LONG] + open_risk_by_side[SHORT]
    current_equity = _extract_account_equity(account_snapshot)
    account_peak_equity = None
    account_max_drawdown = None
    account_max_drawdown_pct = None
    current_drawdown = None
    current_drawdown_pct = None
    initial_equity = None
    initial_equity_started_at = None
    min_equity_since_initial = None
    max_loss_from_initial = None
    max_loss_from_initial_pct = None
    current_return_from_initial = None
    current_return_from_initial_pct = None
    tracking_started_at = None
    tracking_sample_count = 0

    if equity_history:
        tracking_sample_count = len(equity_history)
        ordered_history = sorted(
            equity_history, key=lambda item: item.get("timestamp", 0)
        )
        for point in ordered_history:
            unrealized_raw = point.get("unrealizedPnlUsdt")
            if unrealized_raw in (None, ""):
                continue
            try:
                unrealized_value = Decimal(str(unrealized_raw))
            except Exception:
                continue
            if worst_historical_unrealized is None or unrealized_value < worst_historical_unrealized:
                worst_historical_unrealized = unrealized_value
                max_historical_unrealized_loss_pct = None
                wallet_balance_raw = point.get("walletBalanceUsdt")
                if wallet_balance_raw not in (None, ""):
                    try:
                        wallet_balance_value = Decimal(str(wallet_balance_raw))
                        if wallet_balance_value != 0:
                            max_historical_unrealized_loss_pct = abs(
                                (unrealized_value / wallet_balance_value) * Decimal("100")
                            )
                    except Exception:
                        max_historical_unrealized_loss_pct = None
        if worst_historical_unrealized is not None and worst_historical_unrealized < 0:
            max_historical_unrealized_loss = abs(worst_historical_unrealized)
        configured_initial_equity = None
        configured_initial_equity_raw = os.getenv("BASELINE_EQUITY_USDT")
        if configured_initial_equity_raw not in (None, ""):
            try:
                candidate = Decimal(str(configured_initial_equity_raw))
                if candidate > Decimal("0"):
                    configured_initial_equity = candidate
            except Exception:
                configured_initial_equity = None
        peak_equity = None
        max_drawdown_value = Decimal("0")
        max_drawdown_rate = None
        for point in ordered_history:
            equity = Decimal(str(point.get("equityUsdt", "0")))
            if peak_equity is None or equity > peak_equity:
                peak_equity = equity
            drawdown = (peak_equity or Decimal("0")) - equity
            if drawdown > max_drawdown_value:
                max_drawdown_value = drawdown
                if peak_equity and peak_equity > 0:
                    max_drawdown_rate = (drawdown / peak_equity) * Decimal("100")
        if ordered_history:
            tracking_started_at = ordered_history[0].get("timestamp")
        account_peak_equity = peak_equity
        account_max_drawdown = max_drawdown_value
        account_max_drawdown_pct = max_drawdown_rate

        baseline_history = ordered_history
        if configured_initial_equity is not None:
            baseline_threshold = configured_initial_equity * Decimal("0.99")
            matched_point = next(
                (
                    point
                    for point in ordered_history
                    if Decimal(str(point.get("equityUsdt", "0"))) >= baseline_threshold
                ),
                None,
            )
            if matched_point is not None:
                initial_equity = configured_initial_equity
                initial_equity_started_at = matched_point.get("timestamp")
                baseline_history = [
                    point
                    for point in ordered_history
                    if float(point.get("timestamp", 0) or 0) >= float(initial_equity_started_at or 0)
                ]
        if initial_equity is None and ordered_history:
            first_point = ordered_history[0]
            initial_equity = Decimal(str(first_point.get("equityUsdt", "0")))
            initial_equity_started_at = first_point.get("timestamp")

        if baseline_history:
            min_equity_since_initial = min(
                Decimal(str(point.get("equityUsdt", "0"))) for point in baseline_history
            )
        elif current_equity is not None:
            min_equity_since_initial = current_equity

        if (
            initial_equity is not None
            and min_equity_since_initial is not None
            and initial_equity > Decimal("0")
        ):
            max_loss_from_initial = max(
                Decimal("0"),
                initial_equity - min_equity_since_initial,
            )
            max_loss_from_initial_pct = (max_loss_from_initial / initial_equity) * Decimal("100")
            if current_equity is not None:
                current_return_from_initial = current_equity - initial_equity
                current_return_from_initial_pct = (
                    current_return_from_initial / initial_equity
                ) * Decimal("100")
        if (
            current_equity is not None
            and peak_equity is not None
            and peak_equity > Decimal("0")
        ):
            current_drawdown = peak_equity - current_equity
            current_drawdown_pct = (current_drawdown / peak_equity) * Decimal("100")
        strategy_peak_equity = None
        if initial_equity is not None:
            strategy_peak_equity = initial_equity + peak
        elif current_wallet_balance is not None:
            strategy_peak_equity = current_wallet_balance + peak
        if strategy_peak_equity is not None and strategy_peak_equity > Decimal("0"):
            strategy_max_drawdown_pct = (
                strategy_max_drawdown / strategy_peak_equity
            ) * Decimal("100")
        elif peak > Decimal("0"):
            strategy_max_drawdown_pct = (strategy_max_drawdown / peak) * Decimal("100")

    return {
        "tradeCount": total_trades,
        "winCount": len(wins),
        "lossCount": len(losses),
        "flatCount": len(flats),
        "winRatePct": str(win_rate_pct) if win_rate_pct is not None else None,
        "grossProfitUsdt": str(gross_profit),
        "grossLossUsdtAbs": str(gross_loss_abs),
        "profitFactor": str(profit_factor) if profit_factor is not None else None,
        "avgWinUsdt": str(avg_win) if avg_win is not None else None,
        "avgLossUsdtAbs": str(avg_loss) if avg_loss is not None else None,
        "avgWinLossRatio": str(avg_win_loss_ratio) if avg_win_loss_ratio is not None else None,
        "maxDrawdownUsdt": str(account_max_drawdown) if account_max_drawdown is not None else None,
        "maxDrawdownPct": str(account_max_drawdown_pct) if account_max_drawdown_pct is not None else None,
        "accountMaxDrawdownUsdt": str(account_max_drawdown) if account_max_drawdown is not None else None,
        "accountMaxDrawdownPct": str(account_max_drawdown_pct) if account_max_drawdown_pct is not None else None,
        "accountPeakEquityUsdt": str(account_peak_equity) if account_peak_equity is not None else None,
        "currentEquityUsdt": str(current_equity) if current_equity is not None else None,
        "currentDrawdownUsdt": str(current_drawdown) if current_drawdown is not None else None,
        "currentDrawdownPct": str(current_drawdown_pct) if current_drawdown_pct is not None else None,
        "initialEquityUsdt": str(initial_equity) if initial_equity is not None else None,
        "initialEquityStartedAt": initial_equity_started_at,
        "minEquitySinceInitialUsdt": str(min_equity_since_initial) if min_equity_since_initial is not None else None,
        "maxLossFromInitialUsdt": str(max_loss_from_initial) if max_loss_from_initial is not None else None,
        "maxLossFromInitialPct": str(max_loss_from_initial_pct) if max_loss_from_initial_pct is not None else None,
        "currentReturnFromInitialUsdt": str(current_return_from_initial) if current_return_from_initial is not None else None,
        "currentReturnFromInitialPct": str(current_return_from_initial_pct) if current_return_from_initial_pct is not None else None,
        "equityTrackingStartedAt": tracking_started_at,
        "equityTrackingSamples": tracking_sample_count,
        "strategyMaxDrawdownUsdt": str(strategy_max_drawdown),
        "strategyMaxDrawdownPct": str(strategy_max_drawdown_pct) if strategy_max_drawdown_pct is not None else None,
        "maxConsecutiveLosses": max_loss_streak,
        "maxConsecutiveWins": max_win_streak,
        "currentUnrealizedPnlUsdt": str(current_unrealized),
        "currentUnrealizedPnlPct": str(current_unrealized_pct) if current_unrealized_pct is not None else None,
        "maxHistoricalUnrealizedLossUsdt": str(max_historical_unrealized_loss)
        if max_historical_unrealized_loss is not None
        else None,
        "maxHistoricalUnrealizedLossPct": str(max_historical_unrealized_loss_pct)
        if max_historical_unrealized_loss_pct is not None
        else None,
        "currentPositionValueUsdt": str(current_position_value),
        "netRealizedPnlUsdt": str(sum(pnl_values, Decimal("0"))),
        "openRiskUsdt": str(open_risk_total),
        "openLongRiskUsdt": str(open_risk_by_side[LONG]),
        "openShortRiskUsdt": str(open_risk_by_side[SHORT]),
        "openRiskPct": str((open_risk_total / current_equity) * Decimal("100"))
        if current_equity not in (None, Decimal("0"))
        else None,
        "openLongRiskPct": str((open_risk_by_side[LONG] / current_equity) * Decimal("100"))
        if current_equity not in (None, Decimal("0"))
        else None,
        "openShortRiskPct": str((open_risk_by_side[SHORT] / current_equity) * Decimal("100"))
        if current_equity not in (None, Decimal("0"))
        else None,
    }


def _build_recovery_stats(closed_history: list[dict[str, Any]]) -> dict[str, Any]:
    def _event_net_pnl(event: dict[str, Any]) -> Decimal | None:
        net_pnl_raw = event.get("netRealizedPnlUsdt")
        if net_pnl_raw in (None, ""):
            net_pnl_raw = event.get("realizedPnlUsdt")
        try:
            return Decimal(str(net_pnl_raw)) if net_pnl_raw not in (None, "") else None
        except Exception:
            return None

    def _event_return_basis(event: dict[str, Any]) -> Decimal | None:
        for key in ("returnBasisUsdt", "entryNotionalUsdt"):
            raw_value = event.get(key)
            if raw_value in (None, "", 0, "0"):
                continue
            try:
                basis = abs(Decimal(str(raw_value)))
            except Exception:
                continue
            if basis != Decimal("0"):
                return basis
        return None

    def _event_final_return_pct(
        event: dict[str, Any], net_pnl: Decimal | None
    ) -> Decimal | None:
        if net_pnl is None:
            return None
        basis = _event_return_basis(event)
        if basis in (None, Decimal("0")):
            return None
        return (net_pnl / basis) * Decimal("100")

    tracked_events: list[dict[str, Any]] = []
    underwater_events: list[dict[str, Any]] = []
    recovered_events: list[dict[str, Any]] = []
    max_drawdown_event: dict[str, Any] | None = None
    max_recovered_drawdown_event: dict[str, Any] | None = None
    total_underwater_drawdown = Decimal("0")
    total_recovered_drawdown = Decimal("0")
    total_underwater_profit = Decimal("0")
    total_underwater_loss_abs = Decimal("0")
    total_underwater_net_result = Decimal("0")
    total_underwater_return_basis = Decimal("0")
    total_underwater_return_pnl = Decimal("0")
    underwater_loss_count = 0
    underwater_flat_count = 0

    for event in closed_history:
        min_pnl_pct = None
        min_pnl_raw = event.get("minPnlPct")
        if min_pnl_raw not in (None, ""):
            try:
                min_pnl_pct = Decimal(str(min_pnl_raw))
            except Exception:
                min_pnl_pct = None

        net_pnl = _event_net_pnl(event)
        final_return_pct = _event_final_return_pct(event, net_pnl)
        reason = str(event.get("reason") or "").lower()
        if min_pnl_pct is None and reason in {"liquidation", "adl", "force_order"}:
            if final_return_pct is not None and final_return_pct < 0:
                # Forced-close events may not carry a separate intratrade trough; use the
                # realized return as the drawdown floor so liquidation is not omitted.
                min_pnl_pct = final_return_pct
        if min_pnl_pct is None:
            continue

        row = {
            "asset": event.get("asset"),
            "contractSymbol": event.get("contractSymbol"),
            "side": event.get("side"),
            "timestamp": event.get("timestamp"),
            "minPnlPct": str(min_pnl_pct),
            "maxDrawdownPct": str(min_pnl_pct),
            "netRealizedPnlUsdt": str(net_pnl) if net_pnl is not None else None,
            "finalReturnPct": str(final_return_pct) if final_return_pct is not None else None,
            "reason": event.get("reason"),
            "stopLossMode": event.get("stopLossMode"),
            "breakevenActivatedAt": event.get("breakevenActivatedAt"),
            "closeSide": event.get("closeSide"),
            "recoveredToProfit": bool(net_pnl is not None and net_pnl > 0),
        }
        tracked_events.append(row)

        if min_pnl_pct < 0:
            underwater_events.append(row)
            total_underwater_drawdown += min_pnl_pct
            if (
                max_drawdown_event is None
                or min_pnl_pct < Decimal(str(max_drawdown_event["minPnlPct"]))
            ):
                max_drawdown_event = row
            if net_pnl is not None:
                total_underwater_net_result += net_pnl
                if net_pnl > 0:
                    total_underwater_profit += net_pnl
                elif net_pnl < 0:
                    total_underwater_loss_abs += abs(net_pnl)
                    underwater_loss_count += 1
                else:
                    underwater_flat_count += 1
            basis = _event_return_basis(event)
            if basis is not None and net_pnl is not None:
                total_underwater_return_basis += basis
                total_underwater_return_pnl += net_pnl
            if net_pnl is not None and net_pnl > 0:
                recovered_events.append(row)
                total_recovered_drawdown += min_pnl_pct
                if (
                    max_recovered_drawdown_event is None
                    or min_pnl_pct < Decimal(str(max_recovered_drawdown_event["minPnlPct"]))
                ):
                    max_recovered_drawdown_event = row

    tracked_count = len(tracked_events)
    underwater_count = len(underwater_events)
    recovered_count = len(recovered_events)
    recovered_rate_pct = None
    overall_recovered_rate_pct = None
    avg_underwater_drawdown_pct = None
    avg_recovered_drawdown_pct = None
    underwater_net_return_pct = None

    if underwater_count > 0:
        recovered_rate_pct = (Decimal(recovered_count) / Decimal(underwater_count)) * Decimal("100")
        avg_underwater_drawdown_pct = total_underwater_drawdown / Decimal(underwater_count)
    if tracked_count > 0:
        overall_recovered_rate_pct = (Decimal(recovered_count) / Decimal(tracked_count)) * Decimal("100")
    if recovered_count > 0:
        avg_recovered_drawdown_pct = total_recovered_drawdown / Decimal(recovered_count)
    if total_underwater_return_basis > Decimal("0"):
        underwater_net_return_pct = (
            total_underwater_return_pnl / total_underwater_return_basis
        ) * Decimal("100")

    underwater_cases = sorted(
        underwater_events,
        key=lambda item: Decimal(str(item["minPnlPct"])),
    )
    recovered_cases = sorted(
        recovered_events,
        key=lambda item: Decimal(str(item["minPnlPct"])),
    )

    return {
        "trackedCloseCount": tracked_count,
        "underwaterCloseCount": underwater_count,
        "recoveredWinCount": recovered_count,
        "recoveredWinRatePct": str(recovered_rate_pct) if recovered_rate_pct is not None else None,
        "overallRecoveredWinRatePct": (
            str(overall_recovered_rate_pct) if overall_recovered_rate_pct is not None else None
        ),
        "avgUnderwaterDrawdownPct": (
            str(avg_underwater_drawdown_pct) if avg_underwater_drawdown_pct is not None else None
        ),
        "avgRecoveredDrawdownPct": (
            str(avg_recovered_drawdown_pct) if avg_recovered_drawdown_pct is not None else None
        ),
        "underwaterLossCount": underwater_loss_count,
        "underwaterFlatCount": underwater_flat_count,
        "underwaterFinalProfitUsdt": str(total_underwater_profit),
        "underwaterFinalLossUsdtAbs": str(total_underwater_loss_abs),
        "underwaterNetResultUsdt": str(total_underwater_net_result),
        "underwaterNetReturnPct": (
            str(underwater_net_return_pct) if underwater_net_return_pct is not None else None
        ),
        "maxDrawdownCase": max_drawdown_event,
        "maxRecoveredDrawdownCase": max_recovered_drawdown_event,
        "underwaterCases": underwater_cases,
        "recoveredCases": recovered_cases,
    }


def _match_local_close_reason(
    *,
    closed_history: list[dict[str, Any]],
    contract_symbol: str,
    side: str,
    trade_time_ms: int | None,
    net_realized_pnl: Decimal | None,
) -> str | None:
    best_event = None
    best_score = None
    for event in closed_history:
        if event.get("contractSymbol") != contract_symbol:
            continue
        if event.get("side") != side:
            continue
        event_time_ms = event.get("closedAtMs")
        if event_time_ms in (None, ""):
            timestamp = event.get("timestamp")
            if timestamp not in (None, ""):
                event_time_ms = int(float(timestamp) * 1000)
        time_gap = 999999999
        if trade_time_ms is not None and event_time_ms not in (None, ""):
            time_gap = abs(int(event_time_ms) - int(trade_time_ms))
        pnl_gap = Decimal("999999")
        if (
            net_realized_pnl is not None
            and event.get("netRealizedPnlUsdt") not in (None, "")
        ):
            pnl_gap = abs(
                Decimal(str(event.get("netRealizedPnlUsdt"))) - net_realized_pnl
            )
        score = (time_gap, pnl_gap)
        if best_score is None or score < best_score:
            best_score = score
            best_event = event
    if not best_event or best_score is None:
        return None
    if best_score[0] <= 5 * 60 * 1000:
        return best_event.get("reason")
    return None


def _match_close_event(
    *,
    trade_item: dict[str, Any],
    close_events: list[dict[str, Any]],
    max_gap_ms: int = 5 * 60 * 1000,
) -> dict[str, Any] | None:
    symbol = str(trade_item.get("symbol") or trade_item.get("contractSymbol") or "")
    item_time = int(trade_item.get("timeMs") or trade_item.get("closedAtMs") or 0)
    if not symbol or not item_time:
        return None

    best_event = None
    best_score = None
    for event in close_events:
        event_symbol = str(event.get("contractSymbol") or event.get("symbol") or "")
        if event_symbol != symbol:
            continue
        event_time_ms = event.get("closedAtMs")
        if event_time_ms in (None, ""):
            timestamp = event.get("timestamp")
            if timestamp not in (None, ""):
                event_time_ms = int(float(timestamp) * 1000)
        if event_time_ms in (None, ""):
            continue
        time_gap = abs(int(event_time_ms) - item_time)
        if time_gap > max_gap_ms:
            continue
        pnl_gap = Decimal("999999")
        item_pnl = trade_item.get("realizedPnlUsdt")
        event_pnl = (
            event.get("netRealizedPnlUsdt")
            if event.get("netRealizedPnlUsdt") not in (None, "")
            else event.get("realizedPnlUsdt")
        )
        if item_pnl not in (None, "") and event_pnl not in (None, ""):
            pnl_gap = abs(Decimal(str(event_pnl)) - Decimal(str(item_pnl)))
        score = (time_gap, pnl_gap)
        if best_score is None or score < best_score:
            best_score = score
            best_event = event
    return best_event


def _event_net_pnl(event: dict[str, Any]) -> Decimal | None:
    net_pnl_raw = event.get("netRealizedPnlUsdt")
    if net_pnl_raw in (None, ""):
        net_pnl_raw = event.get("realizedPnlUsdt")
    if net_pnl_raw in (None, ""):
        return None
    try:
        return Decimal(str(net_pnl_raw))
    except Exception:
        return None


def _event_return_pct(event: dict[str, Any], net_pnl: Decimal | None) -> Decimal | None:
    if net_pnl is None:
        return None
    for key in ("returnBasisUsdt", "entryNotionalUsdt"):
        raw_value = event.get(key)
        if raw_value in (None, "", 0, "0"):
            continue
        try:
            basis = abs(Decimal(str(raw_value)))
        except Exception:
            continue
        if basis != Decimal("0"):
            return (net_pnl / basis) * Decimal("100")
    return None


ATTRIBUTION_COMPARISON_MIN_CLOSED_TRADES = 6
ATTRIBUTION_COMPARISON_MIN_GROUP_TRADES = 3
ENTRY_ACTIONS = {
    "enter_long",
    "enter_short",
}


def _strategy_display_name(strategy_id: Any) -> str:
    if strategy_id == LONG_STRATEGY_ID:
        return "AI 精选做多"
    if strategy_id == SHORT_STRATEGY_ID:
        return "AI 精选做空"
    return str(strategy_id or "-")


def _make_attribution_bucket(label: str | None = None) -> dict[str, Any]:
    return {
        "label": label,
        "tradeCount": 0,
        "winCount": 0,
        "lossCount": 0,
        "netRealizedPnlUsdt": Decimal("0"),
        "_returnValues": [],
    }


def _touch_attribution_bucket(
    rows: dict[str, dict[str, Any]],
    key: Any,
    *,
    label: str | None = None,
) -> dict[str, Any]:
    row = rows.setdefault(str(key), _make_attribution_bucket(label))
    if label and not row.get("label"):
        row["label"] = label
    return row


def _aggregate_attribution_rows(
    rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for key, item in rows.items():
        trade_count = int(item["tradeCount"])
        win_count = int(item["winCount"])
        loss_count = int(item["lossCount"])
        return_values = list(item.get("_returnValues", []))
        net_pnl = Decimal(str(item["netRealizedPnlUsdt"]))
        avg_return_pct = (
            sum(return_values, Decimal("0")) / Decimal(len(return_values))
            if return_values
            else None
        )
        avg_net_pnl = net_pnl / Decimal(trade_count) if trade_count else None
        flat_count = max(trade_count - win_count - loss_count, 0)
        output.append(
            {
                "key": key,
                "label": item.get("label"),
                "tradeCount": trade_count,
                "winCount": win_count,
                "lossCount": loss_count,
                "flatCount": flat_count,
                "winRatePct": str((Decimal(win_count) / Decimal(trade_count)) * Decimal("100"))
                if trade_count
                else None,
                "netRealizedPnlUsdt": str(net_pnl),
                "avgNetPnlUsdt": str(avg_net_pnl) if avg_net_pnl is not None else None,
                "avgReturnPct": str(avg_return_pct) if avg_return_pct is not None else None,
            }
        )
    output.sort(
        key=lambda item: (
            Decimal(str(item["netRealizedPnlUsdt"])),
            Decimal(str(item["avgReturnPct"] or "0")),
            int(item["tradeCount"]),
        ),
        reverse=True,
    )
    return output


def _rank_attribution_rows(
    rows: list[dict[str, Any]],
    *,
    min_trade_count: int,
    reverse: bool = True,
) -> list[dict[str, Any]]:
    scoped = [
        row
        for row in rows
        if int(row.get("tradeCount") or 0) >= int(min_trade_count)
    ]
    return sorted(
        scoped,
        key=lambda item: (
            Decimal(str(item.get("netRealizedPnlUsdt") or "0")),
            Decimal(str(item.get("avgReturnPct") or "0")),
            int(item.get("tradeCount") or 0),
        ),
        reverse=reverse,
    )


def _build_optimality_summary(
    *,
    closed_trade_count: int,
    by_strategy_rows: list[dict[str, Any]],
    by_entry_hour_rows: list[dict[str, Any]],
    by_close_reason_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    min_closed_trade_count = ATTRIBUTION_COMPARISON_MIN_CLOSED_TRADES
    min_group_trade_count = ATTRIBUTION_COMPARISON_MIN_GROUP_TRADES
    qualified_strategy_rows = _rank_attribution_rows(
        by_strategy_rows,
        min_trade_count=min_group_trade_count,
    )
    qualified_entry_hour_rows = _rank_attribution_rows(
        by_entry_hour_rows,
        min_trade_count=min_group_trade_count,
    )
    losing_close_reason_rows = [
        row
        for row in _rank_attribution_rows(
            by_close_reason_rows,
            min_trade_count=1,
            reverse=False,
        )
        if Decimal(str(row.get("netRealizedPnlUsdt") or "0")) < Decimal("0")
    ]

    comparison_ready = (
        closed_trade_count >= min_closed_trade_count
        and len(qualified_strategy_rows) >= 2
    )
    notes: list[str] = []
    if closed_trade_count < min_closed_trade_count:
        notes.append(
            f"当前仅有 {closed_trade_count} 笔已平仓，至少累计到 {min_closed_trade_count} 笔后再判断最优策略更稳。"
        )
    if len(qualified_strategy_rows) < 2:
        notes.append(
            f"当前达到比较门槛（单组至少 {min_group_trade_count} 笔）的策略不足 2 组，还不能做策略优选结论。"
        )
    if comparison_ready:
        notes.append("已达到初步比较门槛，可以开始做小步参数优化。")

    return {
        "comparisonReady": comparison_ready,
        "closedTradeCount": closed_trade_count,
        "minClosedTradeCount": min_closed_trade_count,
        "minGroupTradeCount": min_group_trade_count,
        "qualifiedStrategyCount": len(qualified_strategy_rows),
        "bestStrategy": qualified_strategy_rows[0] if qualified_strategy_rows else None,
        "weakestStrategy": qualified_strategy_rows[-1] if len(qualified_strategy_rows) >= 2 else None,
        "bestEntryHour": qualified_entry_hour_rows[0] if qualified_entry_hour_rows else None,
        "watchCloseReason": losing_close_reason_rows[0] if losing_close_reason_rows else None,
        "notes": notes,
    }


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _entry_signal_counts(side: str, audit: Any) -> tuple[int | None, int | None]:
    if not isinstance(audit, dict):
        return (None, None)
    same_side_count = _int_or_none(audit.get("candidateCount"))
    opposite_count = _int_or_none(audit.get("oppositeCandidateCount"))
    if side == SHORT:
        return (opposite_count, same_side_count)
    return (same_side_count, opposite_count)


def _exit_signal_counts(side: str, audit: Any) -> tuple[int | None, int | None]:
    if not isinstance(audit, dict):
        return (None, None)
    long_count = _int_or_none(audit.get("exitStrongLongCount"))
    short_count = _int_or_none(audit.get("exitStrongShortCount"))
    if long_count is not None or short_count is not None:
        return (long_count, short_count)
    same_side_count = _int_or_none(audit.get("currentSignalCount"))
    if same_side_count is None:
        same_side_count = _int_or_none(audit.get("exitCandidateCount"))
    opposite_count = _int_or_none(audit.get("currentOppositeSignalCount"))
    if opposite_count is None:
        opposite_count = _int_or_none(audit.get("exitOppositeCandidateCount"))
    if side == SHORT:
        return (opposite_count, same_side_count)
    return (same_side_count, opposite_count)


def _signal_counts_near_timestamp(
    rows: list[dict[str, Any]],
    timestamp: Any,
    *,
    max_gap_seconds: float = 5 * 60,
) -> tuple[int | None, int | None]:
    target = _coerce_timestamp_seconds(timestamp)
    if target is None or not rows:
        return (None, None)
    best_row = min(
        rows,
        key=lambda row: abs(float(row.get("timestamp", 0) or 0) - target),
    )
    best_ts = _coerce_timestamp_seconds(best_row.get("timestamp"))
    if best_ts is None or abs(best_ts - target) > max_gap_seconds:
        return (None, None)
    return (
        _int_or_none(best_row.get("longCount")),
        _int_or_none(best_row.get("shortCount")),
    )


def _signal_count_bucket(count: int) -> tuple[str, int]:
    count = max(int(count or 0), 0)
    if count <= 5:
        return ("0-5 个", 0)
    if count <= 9:
        return ("6-9 个", 1)
    if count <= 12:
        return ("10-12 个", 2)
    if count == 13:
        return ("13 个", 3)
    if count == 14:
        return ("14 个", 4)
    if count == 15:
        return ("15 个", 5)
    if count <= 19:
        return ("16-19 个", 6)
    if count <= 22:
        return ("20-22 个", 7)
    return ("23 个以上", 8)


def _make_signal_density_bucket(label: str, order: int) -> dict[str, Any]:
    bucket = _make_attribution_bucket(label)
    bucket["order"] = order
    bucket["_sameCounts"] = []
    bucket["_oppositeCounts"] = []
    return bucket


def _aggregate_signal_density_rows(
    rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for key, item in rows.items():
        trade_count = int(item["tradeCount"])
        win_count = int(item["winCount"])
        loss_count = int(item["lossCount"])
        return_values = list(item.get("_returnValues", []))
        same_counts = list(item.get("_sameCounts", []))
        opposite_counts = list(item.get("_oppositeCounts", []))
        net_pnl = Decimal(str(item["netRealizedPnlUsdt"]))
        avg_return_pct = (
            sum(return_values, Decimal("0")) / Decimal(len(return_values))
            if return_values
            else None
        )
        avg_net_pnl = net_pnl / Decimal(trade_count) if trade_count else None
        avg_same_count = (
            sum(same_counts, Decimal("0")) / Decimal(len(same_counts))
            if same_counts
            else None
        )
        avg_opposite_count = (
            sum(opposite_counts, Decimal("0")) / Decimal(len(opposite_counts))
            if opposite_counts
            else None
        )
        flat_count = max(trade_count - win_count - loss_count, 0)
        output.append(
            {
                "key": key,
                "label": item.get("label") or key,
                "tradeCount": trade_count,
                "winCount": win_count,
                "lossCount": loss_count,
                "flatCount": flat_count,
                "winRatePct": str((Decimal(win_count) / Decimal(trade_count)) * Decimal("100"))
                if trade_count
                else None,
                "netRealizedPnlUsdt": str(net_pnl),
                "avgNetPnlUsdt": str(avg_net_pnl) if avg_net_pnl is not None else None,
                "avgReturnPct": str(avg_return_pct) if avg_return_pct is not None else None,
                "avgSameSideCount": str(avg_same_count) if avg_same_count is not None else None,
                "avgOppositeSideCount": str(avg_opposite_count)
                if avg_opposite_count is not None
                else None,
                "_order": int(item.get("order") or 0),
            }
        )
    output.sort(key=lambda item: (int(item.get("_order") or 0), item.get("label") or ""))
    for item in output:
        item.pop("_order", None)
    return output


def _build_entry_event_index(all_history: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    rows: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in all_history:
        if event.get("action") not in ENTRY_ACTIONS:
            continue
        side = event.get("side", LONG)
        strategy_id = str(event.get("strategyId") or "-")
        for symbol_key in (
            event.get("contractSymbol"),
            event.get("asset"),
        ):
            if not symbol_key:
                continue
            key = (str(symbol_key), side, strategy_id)
            rows.setdefault(key, []).append(event)
    for values in rows.values():
        values.sort(key=lambda item: float(item.get("timestamp", 0) or 0))
    return rows


def _match_entry_event_for_close(
    event: dict[str, Any],
    entry_index: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    opened_at = event.get("openedAt")
    if opened_at in (None, ""):
        return None
    try:
        opened_at_value = float(opened_at)
    except Exception:
        return None
    side = event.get("side", LONG)
    strategy_id = str(event.get("strategyId") or "-")
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for symbol_key in (
        event.get("contractSymbol"),
        event.get("asset"),
    ):
        if not symbol_key:
            continue
        for candidate in entry_index.get((str(symbol_key), side, strategy_id), []):
            candidate_id = (
                candidate.get("timestamp"),
                candidate.get("contractSymbol"),
                candidate.get("asset"),
                candidate.get("side"),
                candidate.get("strategyId"),
            )
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            candidates.append(candidate)
    best_match = None
    best_diff = None
    for candidate in candidates:
        try:
            candidate_ts = float(candidate.get("timestamp", 0) or 0)
        except Exception:
            continue
        diff = abs(candidate_ts - opened_at_value)
        if diff <= 5 and (best_diff is None or diff < best_diff):
            best_match = candidate
            best_diff = diff
    if best_match is not None:
        return best_match
    prior_candidates = []
    for candidate in candidates:
        try:
            candidate_ts = float(candidate.get("timestamp", 0) or 0)
        except Exception:
            continue
        if candidate_ts <= opened_at_value + 5:
            prior_candidates.append((candidate_ts, candidate))
    if prior_candidates:
        prior_candidates.sort(key=lambda item: item[0], reverse=True)
        return prior_candidates[0][1]
    return None


def _signal_density_side_observation(
    *,
    side: str,
    rows: list[dict[str, Any]],
    current_count: int,
    opposite_current_count: int,
    min_group_trade_count: int,
) -> dict[str, Any]:
    current_bucket_label, _ = _signal_count_bucket(current_count)
    current_bucket_stats = next(
        (row for row in rows if (row.get("label") or row.get("key")) == current_bucket_label),
        None,
    )
    qualified_rows = _rank_attribution_rows(rows, min_trade_count=min_group_trade_count)
    best_bucket = qualified_rows[0] if qualified_rows else None
    signal_label = "强烈看多榜单" if side == LONG else "强烈看空榜单"
    opposite_label = "强烈看空榜单" if side == LONG else "强烈看多榜单"
    notes: list[str] = []
    if current_bucket_stats is None:
        notes.append(
            f"当前{signal_label} {current_count} 个，落在 {current_bucket_label}，历史还没有这个分桶的已平仓样本。"
        )
    elif int(current_bucket_stats.get("tradeCount") or 0) < min_group_trade_count:
        notes.append(
            f"当前{signal_label} {current_count} 个，落在 {current_bucket_label}，该桶目前只有 {current_bucket_stats.get('tradeCount') or 0} 笔已平仓样本，先继续观察。"
        )
    else:
        notes.append(
            f"当前{signal_label} {current_count} 个，落在 {current_bucket_label}；该桶历史 {current_bucket_stats.get('tradeCount') or 0} 笔，胜率 {current_bucket_stats.get('winRatePct') and _fmt_decimal(Decimal(str(current_bucket_stats.get('winRatePct'))), 2) + '%' or '-'}，均收益 {current_bucket_stats.get('avgReturnPct') and _fmt_decimal(Decimal(str(current_bucket_stats.get('avgReturnPct'))), 2) + '%' or '-'}。"
        )
    if best_bucket is not None:
        notes.append(
            f"当前历史表现最好的{signal_label}分桶是 {(best_bucket.get('label') or best_bucket.get('key') or '-')}，样本 {best_bucket.get('tradeCount') or 0} 笔，净收益 {_fmt_decimal(Decimal(str(best_bucket.get('netRealizedPnlUsdt') or '0')), 4)} USDT。"
        )
    else:
        notes.append(
            f"{signal_label}目前还没有达到 {min_group_trade_count} 笔以上样本的有效分桶，暂不下结论。"
        )
    return {
        "side": side,
        "signalLabel": signal_label,
        "oppositeSignalLabel": opposite_label,
        "currentCount": current_count,
        "oppositeCurrentCount": opposite_current_count,
        "currentBucket": current_bucket_label,
        "currentBucketStats": current_bucket_stats,
        "bestBucket": best_bucket,
        "rows": rows,
        "sampleTradeCount": sum(int(row.get("tradeCount") or 0) for row in rows),
        "notes": notes,
    }


def _build_signal_density_observation(
    closed_history: list[dict[str, Any]],
    all_history: list[dict[str, Any]],
    current_signal_counts: dict[str, int],
) -> dict[str, Any]:
    entry_index = _build_entry_event_index(all_history)
    by_side_bucket_rows: dict[str, dict[str, dict[str, Any]]] = {
        LONG: {},
        SHORT: {},
    }
    matched_trade_count = 0
    for event in closed_history:
        net_pnl = _event_net_pnl(event)
        if net_pnl is None:
            continue
        entry_event = _match_entry_event_for_close(event, entry_index)
        if not entry_event:
            continue
        audit = entry_event.get("audit") or {}
        if not isinstance(audit, dict):
            continue
        try:
            same_side_count = int(audit.get("candidateCount", 0) or 0)
        except Exception:
            continue
        try:
            opposite_count = int(audit.get("oppositeCandidateCount", 0) or 0)
        except Exception:
            opposite_count = 0
        side = event.get("side", LONG)
        bucket_label, bucket_order = _signal_count_bucket(same_side_count)
        bucket = by_side_bucket_rows[side].setdefault(
            bucket_label,
            _make_signal_density_bucket(bucket_label, bucket_order),
        )
        bucket["tradeCount"] += 1
        if net_pnl > 0:
            bucket["winCount"] += 1
        elif net_pnl < 0:
            bucket["lossCount"] += 1
        bucket["netRealizedPnlUsdt"] += net_pnl
        return_pct = _event_return_pct(event, net_pnl)
        if return_pct is not None:
            bucket["_returnValues"].append(return_pct)
        bucket["_sameCounts"].append(Decimal(str(same_side_count)))
        bucket["_oppositeCounts"].append(Decimal(str(opposite_count)))
        matched_trade_count += 1

    min_group_trade_count = ATTRIBUTION_COMPARISON_MIN_GROUP_TRADES
    long_rows = _aggregate_signal_density_rows(by_side_bucket_rows[LONG])
    short_rows = _aggregate_signal_density_rows(by_side_bucket_rows[SHORT])
    current_long_count = int(current_signal_counts.get(LONG_STRATEGY_ID, 0) or 0)
    current_short_count = int(current_signal_counts.get(SHORT_STRATEGY_ID, 0) or 0)
    notes = [
        "统计口径：按开仓当时的强烈看多/看空榜单数量分桶，只统计已经完整平仓的样本。",
    ]
    if matched_trade_count < ATTRIBUTION_COMPARISON_MIN_CLOSED_TRADES:
        notes.append(
            f"当前仅回算到 {matched_trade_count} 笔带榜单数量的已平仓样本，结论先作为观察，不做硬规则。"
        )
    return {
        "summary": {
            "matchedTradeCount": matched_trade_count,
            "minGroupTradeCount": min_group_trade_count,
            "currentLongCount": current_long_count,
            "currentShortCount": current_short_count,
        },
        "long": _signal_density_side_observation(
            side=LONG,
            rows=long_rows,
            current_count=current_long_count,
            opposite_current_count=current_short_count,
            min_group_trade_count=min_group_trade_count,
        ),
        "short": _signal_density_side_observation(
            side=SHORT,
            rows=short_rows,
            current_count=current_short_count,
            opposite_current_count=current_long_count,
            min_group_trade_count=min_group_trade_count,
        ),
        "notes": notes,
    }


def _build_attribution_stats(
    closed_history: list[dict[str, Any]],
    all_history: list[dict[str, Any]],
    current_signal_counts: dict[str, int],
) -> dict[str, Any]:
    by_asset: dict[str, dict[str, Any]] = {}
    by_strategy: dict[str, dict[str, Any]] = {}
    by_entry_reason: dict[str, dict[str, Any]] = {}
    by_reason: dict[str, dict[str, Any]] = {}
    by_hour: dict[str, dict[str, Any]] = {}
    total_hold_seconds = Decimal("0")
    hold_count = 0

    for event in closed_history:
        net_pnl = _event_net_pnl(event)
        if net_pnl is None:
            continue
        return_pct = _event_return_pct(event, net_pnl)
        asset_key = event.get("contractSymbol") or event.get("asset") or "-"
        strategy_key = event.get("strategyId") or "-"
        entry_reason_key = event.get("entryReason") or "-"
        reason_key = event.get("reason") or "-"
        opened_at = event.get("openedAt") or event.get("timestamp")
        hour_key = "-"
        if opened_at not in (None, ""):
            try:
                hour_key = time.strftime("%H:00", time.localtime(float(opened_at)))
            except Exception:
                hour_key = "-"
        for rows, key, label in (
            (by_strategy, strategy_key, _strategy_display_name(strategy_key)),
            (by_entry_reason, entry_reason_key, None),
            (by_asset, asset_key, None),
            (by_reason, reason_key, None),
            (by_hour, hour_key, None),
        ):
            row = _touch_attribution_bucket(rows, key, label=label)
            row["tradeCount"] += 1
            if net_pnl > 0:
                row["winCount"] += 1
            elif net_pnl < 0:
                row["lossCount"] += 1
            row["netRealizedPnlUsdt"] += net_pnl
            if return_pct is not None:
                row["_returnValues"].append(return_pct)
        if opened_at not in (None, "") and event.get("timestamp") not in (None, ""):
            try:
                hold_seconds = Decimal(str(float(event["timestamp"]) - float(opened_at)))
            except Exception:
                hold_seconds = Decimal("0")
            if hold_seconds > Decimal("0"):
                total_hold_seconds += hold_seconds
                hold_count += 1

    partial_count = sum(
        1 for event in all_history if event.get("action") in {"partial_exit_long", "partial_exit_short"}
    )
    avg_hold_minutes = (
        total_hold_seconds / Decimal(hold_count) / Decimal("60")
        if hold_count
        else None
    )
    by_strategy_rows = _aggregate_attribution_rows(by_strategy)
    by_entry_reason_rows = _aggregate_attribution_rows(by_entry_reason)
    by_asset_rows = _aggregate_attribution_rows(by_asset)
    by_close_reason_rows = _aggregate_attribution_rows(by_reason)
    by_entry_hour_rows = _aggregate_attribution_rows(by_hour)
    closed_trade_count = len(closed_history)
    return {
        "summary": {
            "closedTradeCount": closed_trade_count,
            "partialTakeProfitCount": partial_count,
            "avgHoldMinutes": str(avg_hold_minutes) if avg_hold_minutes is not None else None,
        },
        "optimality": _build_optimality_summary(
            closed_trade_count=closed_trade_count,
            by_strategy_rows=by_strategy_rows,
            by_entry_hour_rows=by_entry_hour_rows,
            by_close_reason_rows=by_close_reason_rows,
        ),
        "byStrategy": by_strategy_rows,
        "byEntryReason": by_entry_reason_rows,
        "byAsset": by_asset_rows[:12],
        "byCloseReason": by_close_reason_rows,
        "byEntryHour": by_entry_hour_rows,
        "signalDensityObservation": _build_signal_density_observation(
            closed_history,
            all_history,
            current_signal_counts,
        ),
    }


def _build_stop_loss_leaderboard(closed_history: list[dict[str, Any]]) -> dict[str, Any]:
    by_asset: dict[str, dict[str, Any]] = {}
    stop_loss_events = 0
    long_stop_events = 0
    short_stop_events = 0

    for event in closed_history:
        reason = str(event.get("reason") or "").lower()
        stop_loss_mode = str(event.get("stopLossMode") or "").lower()
        if reason != "stop_loss" or stop_loss_mode == "breakeven":
            continue
        stop_loss_events += 1
        side = event.get("side", LONG)
        if side == SHORT:
            short_stop_events += 1
        else:
            long_stop_events += 1

        net_pnl = _event_net_pnl(event)
        return_pct = _event_return_pct(event, net_pnl)
        asset_key = event.get("contractSymbol") or event.get("asset") or "-"
        row = by_asset.setdefault(
            str(asset_key),
            {
                **_make_attribution_bucket(),
                "longStopCount": 0,
                "shortStopCount": 0,
                "latestTimestamp": 0,
            },
        )
        row["label"] = str(asset_key)
        row["tradeCount"] += 1
        if net_pnl is not None:
            if net_pnl > 0:
                row["winCount"] += 1
            elif net_pnl < 0:
                row["lossCount"] += 1
            row["netRealizedPnlUsdt"] += net_pnl
        if return_pct is not None:
            row["_returnValues"].append(return_pct)
        if side == SHORT:
            row["shortStopCount"] += 1
        else:
            row["longStopCount"] += 1
        try:
            row["latestTimestamp"] = max(
                float(row.get("latestTimestamp") or 0),
                float(event.get("timestamp") or 0),
            )
        except Exception:
            pass

    rows = _aggregate_attribution_rows(by_asset)
    latest_by_key = {
        key: value.get("latestTimestamp", 0)
        for key, value in by_asset.items()
    }
    extra_by_key = {
        key: {
            "longStopCount": value.get("longStopCount", 0),
            "shortStopCount": value.get("shortStopCount", 0),
        }
        for key, value in by_asset.items()
    }
    for row in rows:
        extra = extra_by_key.get(str(row.get("key")), {})
        row["longStopCount"] = int(extra.get("longStopCount", 0) or 0)
        row["shortStopCount"] = int(extra.get("shortStopCount", 0) or 0)
        row["stopLossCount"] = int(row.get("tradeCount") or 0)
        row["latestTimestamp"] = latest_by_key.get(str(row.get("key")), 0)
    rows.sort(
        key=lambda item: (
            -int(item.get("stopLossCount") or 0),
            Decimal(str(item.get("netRealizedPnlUsdt") or "0")),
            -float(item.get("latestTimestamp") or 0),
            item.get("label") or item.get("key") or "",
        )
    )
    for row in rows:
        row.pop("latestTimestamp", None)

    return {
        "summary": {
            "stopLossEventCount": stop_loss_events,
            "longStopEventCount": long_stop_events,
            "shortStopEventCount": short_stop_events,
            "assetCount": len(rows),
        },
        "rows": rows[:20],
    }


def _build_trade_history_from_close_events(
    close_events: list[dict[str, Any]],
    realized_events: list[dict[str, Any]] | None = None,
    all_history: list[dict[str, Any]] | None = None,
    signal_count_history: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    merged_events = list(close_events)
    entry_index = _build_entry_event_index(all_history or [])
    for event in realized_events or []:
        if event.get("action") not in PARTIAL_EXIT_ACTIONS:
            continue
        merged_events.append(event)
    for event in sorted(
        merged_events,
        key=lambda item: item.get("closedAtMs") or item.get("timestamp", 0),
        reverse=True,
    ):
        side = event.get("side", LONG)
        entry_event = _match_entry_event_for_close(event, entry_index)
        entry_long_count, entry_short_count = _entry_signal_counts(
            side,
            (entry_event or {}).get("audit"),
        )
        exit_long_count, exit_short_count = _exit_signal_counts(
            side,
            event.get("audit"),
        )
        if exit_long_count is None or exit_short_count is None:
            fallback_long_count, fallback_short_count = _signal_counts_near_timestamp(
                signal_count_history or [],
                event.get("closedAtMs") or event.get("timestamp"),
            )
            if exit_long_count is None:
                exit_long_count = fallback_long_count
            if exit_short_count is None:
                exit_short_count = fallback_short_count
        reason = str(event.get("reason") or "").lower()
        order_type = "MARKET"
        action_label = "平仓"
        if reason in {"liquidation", "force_order"}:
            order_type = "强制平仓"
        elif reason == "adl":
            order_type = "自动减仓(ADL)"
        elif event.get("action") in PARTIAL_EXIT_ACTIONS or reason == "partial_take_profit":
            order_type = "部分减仓"
            action_label = "分批止盈"
        realized_pnl = (
            event.get("netRealizedPnlUsdt")
            if event.get("netRealizedPnlUsdt") not in (None, "")
            else event.get("realizedPnlUsdt")
        )
        realized_pnl_pct = None
        if realized_pnl not in (None, ""):
            basis = None
            for key in ("returnBasisUsdt", "entryNotionalUsdt"):
                raw_value = event.get(key)
                if raw_value in (None, "", 0, "0"):
                    continue
                try:
                    candidate_basis = abs(Decimal(str(raw_value)))
                except Exception:
                    continue
                if candidate_basis != Decimal("0"):
                    basis = candidate_basis
                    break
            if basis not in (None, Decimal("0")):
                try:
                    realized_pnl_pct = str((Decimal(str(realized_pnl)) / basis) * Decimal("100"))
                except Exception:
                    realized_pnl_pct = None
        items.append(
            {
                "timestamp": event.get("timestamp"),
                "timeMs": event.get("closedAtMs"),
                "openedAt": event.get("openedAt") or (entry_event or {}).get("timestamp"),
                "symbol": event.get("contractSymbol"),
                "asset": event.get("asset"),
                "side": "SELL" if side == LONG else "BUY",
                "direction": "做多" if side == LONG else "做空",
                "action": action_label,
                "type": order_type,
                "price": event.get("exitPrice"),
                "quantity": event.get("exitQty") or event.get("quantity"),
                "status": event.get("status"),
                "orderId": str(event.get("orderId", "")),
                "isClose": True,
                "closeReason": event.get("reason"),
                "stopLossStatus": event.get("stopLossStatus")
                or (event.get("audit") or {}).get("stopLossStatus"),
                "stopLossMode": event.get("stopLossMode"),
                "breakevenActivatedAt": event.get("breakevenActivatedAt"),
                "realizedPnlUsdt": realized_pnl,
                "realizedPnlPct": realized_pnl_pct,
                "entryStrongLongCount": entry_long_count,
                "entryStrongShortCount": entry_short_count,
                "exitStrongLongCount": exit_long_count,
                "exitStrongShortCount": exit_short_count,
            }
        )
    return items


def _build_force_order_summary_from_close_events(
    close_events: list[dict[str, Any]],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    total_loss = Decimal("0")
    for event in sorted(
        close_events,
        key=lambda item: item.get("closedAtMs") or item.get("timestamp", 0),
        reverse=True,
    ):
        reason = str(event.get("reason") or "").lower()
        if reason not in {"liquidation", "adl", "force_order"}:
            continue
        realized_pnl = Decimal(
            str(
                event.get("realizedPnlUsdt")
                if event.get("realizedPnlUsdt") not in (None, "")
                else event.get("netRealizedPnlUsdt") or "0"
            )
        )
        total_loss += realized_pnl
        items.append(
            {
                "timestamp": event.get("timestamp"),
                "timeMs": event.get("closedAtMs"),
                "symbol": event.get("contractSymbol"),
                "asset": event.get("asset"),
                "direction": "做多" if event.get("side") == LONG else "做空",
                "type": "强制平仓" if reason != "adl" else "自动减仓(ADL)",
                "autoCloseType": reason.upper(),
                "price": event.get("exitPrice"),
                "quantity": event.get("exitQty") or event.get("quantity"),
                "realizedPnl": str(realized_pnl),
            }
        )
    return {
        "count": len(items),
        "totalLossUsdt": str(total_loss),
        "items": items,
    }


def _history_cache_path(workdir: Path) -> Path:
    return workdir / "runtime" / "history_cache.json"


def _reset_marker_path(workdir: Path) -> Path:
    return workdir / "runtime" / "reset_marker.json"


def _reset_cutoff_ms(workdir: Path) -> int | None:
    marker = _load_json(_reset_marker_path(workdir), {})
    if not isinstance(marker, dict):
        return None
    reset_at_ms = marker.get("resetAtMs")
    if reset_at_ms not in (None, ""):
        try:
            value = int(float(reset_at_ms))
            return value if value > 0 else None
        except Exception:
            pass
    reset_at = marker.get("resetAt")
    if reset_at not in (None, ""):
        try:
            value = int(float(reset_at) * 1000)
            return value if value > 0 else None
        except Exception:
            pass
    return None


def _load_history_cache(workdir: Path) -> dict[str, Any]:
    payload = _load_json(_history_cache_path(workdir), {})
    return payload if isinstance(payload, dict) else {}


def _save_history_cache(workdir: Path, payload: dict[str, Any]) -> None:
    path = _history_cache_path(workdir)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    temp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temp_path.replace(path)


def _event_time_ms(item: dict[str, Any]) -> int:
    for key in ("timeMs", "closedAtMs"):
        value = item.get(key)
        if value not in (None, ""):
            return int(value)
    timestamp = item.get("timestamp")
    if timestamp not in (None, ""):
        return int(float(timestamp) * 1000)
    return 0


REALIZED_EXIT_ACTIONS = {
    "exit_long",
    "exit_short",
    "partial_exit_long",
    "partial_exit_short",
}

FULL_CLOSE_ACTIONS = {
    "exit_long",
    "exit_short",
}

PARTIAL_EXIT_ACTIONS = {
    "partial_exit_long",
    "partial_exit_short",
}


def _event_net_pnl_value(event: dict[str, Any]) -> Decimal | None:
    raw_value = event.get("netRealizedPnlUsdt")
    if raw_value in (None, ""):
        raw_value = event.get("realizedPnlUsdt")
    try:
        return Decimal(str(raw_value)) if raw_value not in (None, "") else None
    except Exception:
        return None


def _event_gross_pnl_value(event: dict[str, Any]) -> Decimal | None:
    raw_value = event.get("realizedPnlUsdt")
    try:
        return Decimal(str(raw_value)) if raw_value not in (None, "") else None
    except Exception:
        return None


def _event_return_basis_value(event: dict[str, Any]) -> Decimal | None:
    for key in ("returnBasisUsdt", "entryNotionalUsdt"):
        raw_value = event.get(key)
        if raw_value in (None, "", 0, "0"):
            continue
        try:
            basis = abs(Decimal(str(raw_value)))
        except Exception:
            continue
        if basis != Decimal("0"):
            return basis
    return None


def _realized_trade_key(event: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(event.get("contractSymbol") or event.get("asset") or ""),
        str(event.get("side") or ""),
        str(event.get("openedAt") or ""),
        str(event.get("strategyId") or ""),
    )


def _aggregate_closed_trade_history(
    closed_events: list[dict[str, Any]],
    realized_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    partials_by_key: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for event in realized_events:
        if event.get("action") not in PARTIAL_EXIT_ACTIONS:
            continue
        partials_by_key.setdefault(_realized_trade_key(event), []).append(event)

    aggregated: list[dict[str, Any]] = []
    for event in closed_events:
        merged = dict(event)
        related_events = sorted(
            partials_by_key.get(_realized_trade_key(event), []) + [event],
            key=lambda item: _event_time_ms(item),
        )
        gross_realized = sum(
            (
                value
                for value in (_event_gross_pnl_value(item) for item in related_events)
                if value is not None
            ),
            Decimal("0"),
        )
        net_realized = sum(
            (
                value
                for value in (_event_net_pnl_value(item) for item in related_events)
                if value is not None
            ),
            Decimal("0"),
        )
        return_basis = sum(
            (
                value
                for value in (_event_return_basis_value(item) for item in related_events)
                if value is not None
            ),
            Decimal("0"),
        )
        partial_events = related_events[:-1]
        partial_realized = sum(
            (
                value
                for value in (_event_net_pnl_value(item) for item in partial_events)
                if value is not None
            ),
            Decimal("0"),
        )

        merged["realizedPnlUsdt"] = str(gross_realized)
        merged["netRealizedPnlUsdt"] = str(net_realized)
        if return_basis > Decimal("0"):
            merged["returnBasisUsdt"] = str(return_basis)
            merged["entryNotionalUsdt"] = str(return_basis)
        merged["partialTakeProfitCount"] = len(partial_events)
        merged["partialRealizedPnlUsdt"] = str(partial_realized)
        if net_realized > Decimal("0"):
            merged["closeSide"] = "win"
        elif net_realized < Decimal("0"):
            merged["closeSide"] = "loss"
        else:
            merged["closeSide"] = "flat"
        aggregated.append(merged)
    return aggregated


def _build_side_summary(
    side: str,
    positions: list[dict[str, Any]],
    closed_history: list[dict[str, Any]],
    realized_history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    scoped_positions = [row for row in positions if row.get("side") == side]
    scoped_history = [event for event in closed_history if event.get("side") == side]
    scoped_realized_history = [
        event for event in (realized_history or closed_history) if event.get("side") == side
    ]
    unrealized = sum(
        (row.get("unrealizedProfit") or Decimal("0")) for row in scoped_positions
    )
    position_value = sum(
        (row.get("currentValueUsdt") or Decimal("0")) for row in scoped_positions
    )
    realized = sum(
        (
            value
            for value in (_event_net_pnl_value(event) for event in scoped_realized_history)
            if value is not None
        ),
        Decimal("0"),
    )
    return {
        "side": side,
        "label": "鍋氬" if side == LONG else "鍋氱┖",
        "openPositions": len(scoped_positions),
        "currentValueUsdt": str(position_value),
        "unrealizedProfit": str(unrealized),
        "closedCount": len(scoped_history),
        "closedWinCount": sum(1 for event in scoped_history if event.get("closeSide") == "win"),
        "closedLossCount": sum(1 for event in scoped_history if event.get("closeSide") == "loss"),
        "closedFlatCount": sum(1 for event in scoped_history if event.get("closeSide") == "flat"),
        "realizedPnlUsdt": str(realized),
        "partialTakeProfitCount": sum(
            1 for event in scoped_realized_history if event.get("action") in PARTIAL_EXIT_ACTIONS
        ),
    }


def _merge_force_order_items(
    *groups: list[dict[str, Any]],
    limit: int = 200,
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, int, str, str], dict[str, Any]] = {}
    for items in groups:
        for item in items:
            key = (
                str(item.get("symbol") or item.get("contractSymbol") or ""),
                _event_time_ms(item),
                str(item.get("type") or item.get("autoCloseType") or ""),
                str(item.get("realizedPnl") or item.get("realizedPnlUsdt") or ""),
            )
            if key not in merged:
                merged[key] = item
    ordered = sorted(
        merged.values(),
        key=lambda item: _event_time_ms(item),
        reverse=True,
    )
    return ordered[:limit]


def _filter_items_after_cutoff(
    items: list[dict[str, Any]],
    reset_cutoff_ms: int | None,
) -> list[dict[str, Any]]:
    if not reset_cutoff_ms:
        return items
    return [item for item in items if _event_time_ms(item) >= reset_cutoff_ms]


def _build_cached_force_order_summary(
    workdir: Path,
    broker: Any,
    local_force_summary: dict[str, Any],
    reset_cutoff_ms: int | None = None,
) -> dict[str, Any]:
    cache = _load_history_cache(workdir)
    now = time.time()
    now_ms = int(now * 1000)
    ttl_seconds = int(os.getenv("HISTORY_SYNC_INTERVAL_SECONDS", "60"))
    overlap_ms = int(os.getenv("HISTORY_SYNC_OVERLAP_SECONDS", "600")) * 1000
    sync_due = now - float(cache.get("syncedAt", 0) or 0) >= ttl_seconds

    cached_force_items = cache.get("forceOrderItems", [])
    if not isinstance(cached_force_items, list):
        cached_force_items = []
    cached_force_items = _filter_items_after_cutoff(cached_force_items, reset_cutoff_ms)

    if sync_due:
        last_force_sync_ms = int(cache.get("lastForceSyncMs", 0) or 0)
        start_time_ms = (
            max(0, last_force_sync_ms - overlap_ms)
            if last_force_sync_ms > 0
            else now_ms - 30 * 24 * 60 * 60 * 1000
        )
        if reset_cutoff_ms:
            start_time_ms = max(start_time_ms, reset_cutoff_ms)
        try:
            api_force_summary = _build_force_order_summary(
                broker=broker,
                start_time_ms=start_time_ms,
                end_time_ms=now_ms,
            )
            cached_force_items = _merge_force_order_items(
                cached_force_items,
                api_force_summary.get("items", []),
            )
            cached_force_items = _filter_items_after_cutoff(cached_force_items, reset_cutoff_ms)
            cache["forceOrderItems"] = cached_force_items
            cache["lastForceSyncMs"] = now_ms
            cache["syncedAt"] = now
            _save_history_cache(workdir, cache)
        except Exception as exc:
            logging.warning("force_order_incremental_sync_failed: %s", exc)

    merged_items = _merge_force_order_items(
        _filter_items_after_cutoff(local_force_summary.get("items", []), reset_cutoff_ms),
        cached_force_items,
    )
    total_loss = sum(
        (Decimal(str(item.get("realizedPnl") or "0")) for item in merged_items),
        Decimal("0"),
    )
    return {
        "count": len(merged_items),
        "totalLossUsdt": str(total_loss),
        "items": merged_items,
    }


def _fetch_income_history_chunked(
    *,
    broker: Any,
    start_time_ms: int,
    end_time_ms: int,
    window_ms: int = 24 * 60 * 60 * 1000,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, int, str]] = set()
    window_start = start_time_ms
    while window_start < end_time_ms:
        window_end = min(window_start + window_ms, end_time_ms)
        try:
            batch = broker.get_income_history(
                start_time_ms=window_start,
                end_time_ms=window_end,
                limit=limit,
            )
        except Exception:
            batch = []
        for row in batch:
            key = (
                str(row.get("symbol") or ""),
                str(row.get("incomeType") or ""),
                str(row.get("tradeId") or row.get("tranId") or ""),
                int(row.get("time") or 0),
                str(row.get("income") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
        window_start = window_end
    rows.sort(key=lambda item: int(item.get("time") or 0), reverse=True)
    return rows


def _fetch_user_trades_chunked(
    *,
    broker: Any,
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
    window_ms: int = 24 * 60 * 60 * 1000,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    seen: set[str] = set()
    window_start = start_time_ms
    while window_start < end_time_ms:
        window_end = min(window_start + window_ms, end_time_ms)
        try:
            batch = broker.get_user_trades(
                symbol=symbol,
                start_time_ms=window_start,
                end_time_ms=window_end,
                limit=limit,
            )
        except Exception:
            batch = []
        for trade in batch:
            trade_id = str(trade.get("id") or "")
            dedupe_key = trade_id or json.dumps(trade, sort_keys=True, ensure_ascii=False)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            trades.append(trade)
        window_start = window_end
    trades.sort(key=lambda item: int(item.get("time") or 0), reverse=True)
    return trades


def _build_recent_closes_from_api(
    *,
    broker: Any,
    closed_history: list[dict[str, Any]],
    limit: int | None = 20,
    reset_cutoff_ms: int | None = None,
) -> list[dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - (7 * 24 * 60 * 60 * 1000)
    if reset_cutoff_ms:
        start_time_ms = max(start_time_ms, reset_cutoff_ms)

    income_rows = _fetch_income_history_chunked(
        broker=broker,
        start_time_ms=start_time_ms,
        end_time_ms=now_ms,
        limit=1000,
    )
    realized_rows = [
        row
        for row in income_rows
        if row.get("incomeType") == "REALIZED_PNL"
        and row.get("symbol")
        and row.get("tradeId") not in (None, "", 0, "0")
    ]
    commission_rows = [
        row
        for row in income_rows
        if row.get("incomeType") == "COMMISSION"
        and row.get("symbol")
        and row.get("tradeId") not in (None, "", 0, "0")
    ]

    commission_map: dict[tuple[str, str], Decimal] = {}
    for row in commission_rows:
        key = (str(row.get("symbol")), str(row.get("tradeId")))
        commission_map[key] = commission_map.get(key, Decimal("0")) + Decimal(
            str(row.get("income", "0"))
        )

    symbols = sorted({str(row.get("symbol")) for row in realized_rows})
    trades_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    if symbols:
        max_workers = min(8, len(symbols))

        def _fetch_trades(symbol: str) -> tuple[str, list[dict[str, Any]]]:
            return (
                symbol,
                _fetch_user_trades_chunked(
                    broker=broker,
                    symbol=symbol,
                    start_time_ms=start_time_ms,
                    end_time_ms=now_ms,
                    limit=1000,
                ),
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_fetch_trades, symbol) for symbol in symbols]
            for future in as_completed(futures):
                try:
                    symbol, trades = future.result()
                except Exception:
                    continue
                for trade in trades:
                    key = (str(symbol), str(trade.get("id")))
                    trades_by_key[key] = trade

    force_map: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        for row in broker.get_force_orders(
            start_time_ms=start_time_ms,
            end_time_ms=now_ms,
            limit=100,
        ):
            order_id = row.get("orderId")
            symbol = row.get("symbol")
            if order_id in (None, "") or symbol in (None, ""):
                continue
            force_map[(str(symbol), str(order_id))] = row
    except Exception:
        force_map = {}

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in realized_rows:
        symbol = str(row.get("symbol"))
        trade_id = str(row.get("tradeId"))
        trade = trades_by_key.get((symbol, trade_id))
        if trade is None:
            continue
        realized = Decimal(str(row.get("income", "0")))
        commission = commission_map.get((symbol, trade_id), Decimal("0"))
        net_realized = realized + commission
        trade_time_ms = int(trade.get("time", row.get("time", 0)) or 0)
        trade_side = str(trade.get("side", "")).upper()
        side = SHORT if trade_side == "BUY" else LONG
        order_id = str(trade.get("orderId"))
        group_key = (symbol, order_id, side)
        item = grouped.setdefault(
            group_key,
            {
                "timestamp": trade_time_ms / 1000 if trade_time_ms else None,
                "closedAtMs": trade_time_ms or None,
                "confirmedClosed": True,
                "closeRetryCount": 0,
                "asset": symbol.replace("USDT", ""),
                "contractSymbol": symbol,
                "side": side,
                "exitQty": Decimal("0"),
                "exitNotional": Decimal("0"),
                "realizedPnlDecimal": Decimal("0"),
                "orderId": order_id,
            },
        )
        item["realizedPnlDecimal"] += net_realized
        qty = Decimal(str(trade.get("qty", "0") or "0"))
        price = Decimal(str(trade.get("price", "0") or "0"))
        item["exitQty"] += qty
        item["exitNotional"] += price * qty
        if trade_time_ms and (
            item.get("closedAtMs") is None or trade_time_ms > item["closedAtMs"]
        ):
            item["closedAtMs"] = trade_time_ms
            item["timestamp"] = trade_time_ms / 1000

    items: list[dict[str, Any]] = []
    for (symbol, order_id, side), item in grouped.items():
        force_row = force_map.get((symbol, order_id))
        exchange_reason = None
        if force_row:
            auto_close_type = str(force_row.get("autoCloseType", "")).upper()
            if auto_close_type == "LIQUIDATION":
                exchange_reason = "liquidation"
            elif auto_close_type == "ADL":
                exchange_reason = "adl"
            else:
                exchange_reason = "force_order"
        net_realized = item["realizedPnlDecimal"]
        local_reason = _match_local_close_reason(
            closed_history=closed_history,
            contract_symbol=symbol,
            side=side,
            trade_time_ms=item.get("closedAtMs"),
            net_realized_pnl=net_realized,
        )
        exit_price = None
        if item["exitQty"] != Decimal("0"):
            exit_price = str(item["exitNotional"] / item["exitQty"])
        items.append(
            {
                "timestamp": item.get("timestamp"),
                "closedAtMs": item.get("closedAtMs"),
                "confirmedClosed": True,
                "closeRetryCount": 0,
                "asset": item["asset"],
                "contractSymbol": symbol,
                "side": side,
                "orderId": order_id,
                "exitQty": str(item["exitQty"]) if item["exitQty"] != Decimal("0") else None,
                "exitPrice": exit_price,
                "realizedPnlUsdt": str(net_realized),
                "closeSide": "win" if net_realized > 0 else ("loss" if net_realized < 0 else "flat"),
                "reason": exchange_reason or local_reason or "exchange_trade",
                "status": "FILLED",
            }
        )

    items.sort(key=lambda item: item.get("closedAtMs") or 0, reverse=True)
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, Any, str]] = set()
    for item in items:
        key = (
            str(item.get("contractSymbol")),
            item.get("closedAtMs"),
            str(item.get("realizedPnlUsdt")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
        if limit is not None and len(deduped) >= limit:
            break
    return deduped


def _build_trade_history_from_api(
    *,
    broker: Any,
    positions: list[dict[str, Any]],
    closed_history: list[dict[str, Any]],
    reset_cutoff_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Build a complete order history from the allOrders API endpoint."""
    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - (30 * 24 * 60 * 60 * 1000)  # 30 days
    if reset_cutoff_ms:
        start_time_ms = max(start_time_ms, reset_cutoff_ms)

    all_symbols: set[str] = set()
    for pos in positions:
        sym = pos.get("contractSymbol")
        if sym:
            all_symbols.add(sym)
    for event in closed_history:
        sym = event.get("contractSymbol")
        if sym:
            all_symbols.add(sym)

    # Also discover symbols from income history (covers cases where
    # positions are all closed and closed_history is empty/lost)
    try:
        income_rows = broker.get_income_history(
            start_time_ms=start_time_ms,
            end_time_ms=now_ms,
            limit=1000,
        )
        for row in income_rows:
            sym = row.get("symbol")
            if sym:
                all_symbols.add(str(sym))
    except Exception:
        pass

    if not all_symbols:
        return []

    all_orders: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []  # userTrades for PnL
    max_workers = min(8, len(all_symbols))
    seven_days_ms = 7 * 24 * 60 * 60 * 1000

    def _fetch_orders_and_trades(symbol: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch orders (7-day windows) and userTrades for a symbol."""
        orders: list[dict[str, Any]] = []
        window_start = start_time_ms
        while window_start < now_ms:
            window_end = min(window_start + seven_days_ms, now_ms)
            try:
                batch = broker.get_all_orders(
                    symbol=symbol,
                    start_time_ms=window_start,
                    end_time_ms=window_end,
                    limit=500,
                )
                if batch:
                    orders.extend(batch)
            except Exception:
                pass
            window_start = window_end
        trades: list[dict[str, Any]] = []
        window_start = start_time_ms
        while window_start < now_ms:
            window_end = min(window_start + seven_days_ms, now_ms)
            try:
                batch = broker.get_user_trades(
                    symbol=symbol,
                    start_time_ms=window_start,
                    end_time_ms=window_end,
                    limit=1000,
                )
                if batch:
                    trades.extend(batch)
            except Exception:
                pass
            window_start = window_end
        return orders, trades

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_orders_and_trades, sym): sym for sym in sorted(all_symbols)}
        for future in as_completed(futures):
            orders, trades = future.result()
            all_orders.extend(orders)
            all_trades.extend(trades)

    # Build orderId -> total realizedPnl from userTrades
    order_pnl_map: dict[str, Decimal] = {}
    for t in all_trades:
        oid = str(t.get("orderId", ""))
        rpnl = t.get("realizedPnl")
        if oid and rpnl is not None:
            pnl_val = Decimal(str(rpnl))
            if pnl_val != 0:
                order_pnl_map[oid] = order_pnl_map.get(oid, Decimal("0")) + pnl_val
    # Also get commission from userTrades
    order_commission_map: dict[str, Decimal] = {}
    for t in all_trades:
        oid = str(t.get("orderId", ""))
        comm = t.get("commission")
        if oid and comm is not None:
            comm_val = Decimal(str(comm))
            if comm_val != 0:
                order_commission_map[oid] = order_commission_map.get(oid, Decimal("0")) + comm_val

    items: list[dict[str, Any]] = []
    for order in all_orders:
        status = str(order.get("status", "")).upper()
        if status not in ("FILLED", "PARTIALLY_FILLED"):
            continue
        symbol = str(order.get("symbol", ""))
        side = str(order.get("side", "")).upper()
        order_type = str(order.get("type", "")).upper()
        reduce_only = order.get("reduceOnly", False)
        close_position = order.get("closePosition", False)
        avg_price = order.get("avgPrice") or order.get("price") or "0"
        qty = order.get("executedQty") or order.get("origQty") or "0"
        order_time_ms = int(order.get("updateTime") or order.get("time") or 0)

        if reduce_only or close_position:
            action = "平仓"
        elif side == "BUY":
            action = "开多" if not reduce_only else "平空"
        else:
            action = "开空" if not reduce_only else "平多"

        position_side = order.get("positionSide", "BOTH")
        if position_side == "LONG":
            direction = "做多"
            if side == "SELL":
                action = "平多"
            else:
                action = "开多"
        elif position_side == "SHORT":
            direction = "做空"
            if side == "BUY":
                action = "平空"
            else:
                action = "开空"
        else:
            direction = "做多" if side == "BUY" and not reduce_only else "做空"

        is_close = action in ("平仓", "平多", "平空")
        close_reason = None
        realized_pnl = None
        if is_close:
            close_reason = _match_local_close_reason(
                closed_history=closed_history,
                contract_symbol=symbol,
                side="LONG" if direction == "做多" else "SHORT",
                trade_time_ms=order_time_ms,
                net_realized_pnl=None,
            )
            # Get realizedPnl directly from userTrades (matched by orderId)
            order_id = str(order.get("orderId", ""))
            pnl = order_pnl_map.get(order_id)
            commission = order_commission_map.get(order_id, Decimal("0"))
            if pnl is not None:
                realized_pnl = str(pnl - abs(commission))

        items.append({
            "timestamp": order_time_ms / 1000 if order_time_ms else None,
            "timeMs": order_time_ms,
            "symbol": symbol,
            "asset": symbol.replace("USDT", ""),
            "side": side,
            "direction": direction,
            "action": action,
            "type": order_type,
            "price": avg_price,
            "quantity": qty,
            "status": status,
            "orderId": str(order.get("orderId", "")),
            "isClose": is_close,
            "closeReason": close_reason,
            "realizedPnlUsdt": realized_pnl,
        })

    items.sort(key=lambda x: x.get("timeMs") or 0, reverse=True)
    return items


def _build_force_order_summary(
    *,
    broker: Any,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
) -> dict[str, Any]:
    """Build force liquidation summary from the forceOrders API endpoint."""
    now_ms = int(time.time() * 1000)
    if end_time_ms is None:
        end_time_ms = now_ms
    if start_time_ms is None:
        start_time_ms = end_time_ms - (30 * 24 * 60 * 60 * 1000)

    try:
        force_orders = broker.get_force_orders(
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=100,
        )
    except Exception:
        force_orders = []

    items: list[dict[str, Any]] = []
    total_loss = Decimal("0")
    for order in force_orders:
        symbol = str(order.get("symbol", ""))
        side = str(order.get("side", "")).upper()
        auto_close_type = str(order.get("autoCloseType", "")).upper()
        client_order_id = str(order.get("clientOrderId", ""))
        avg_price = str(order.get("avgPrice") or order.get("price") or "0")
        qty = str(order.get("executedQty") or order.get("origQty") or "0")
        order_time_ms = int(order.get("updateTime") or order.get("time") or 0)

        # Type detection: autoCloseType may be missing on testnet,
        # fall back to clientOrderId prefix
        if auto_close_type == "LIQUIDATION":
            type_label = "强制平仓"
        elif auto_close_type == "ADL":
            type_label = "自动减仓(ADL)"
        elif client_order_id.startswith("autoclose-"):
            type_label = "强制平仓"
        else:
            type_label = auto_close_type or "强制平仓"

        position_side = order.get("positionSide", "BOTH")
        if position_side == "LONG":
            direction = "做多"
        elif position_side == "SHORT":
            direction = "做空"
        else:
            direction = "做空" if side == "BUY" else "做多"

        # --- Get actual realized PnL from userTrades API (most accurate) ---
        actual_pnl = Decimal("0")
        pnl_found = False
        try:
            trades = broker.get_user_trades(
                symbol=symbol,
                start_time_ms=order_time_ms - 60 * 1000,
                end_time_ms=order_time_ms + 60 * 1000,
                limit=50,
            )
            for t in trades:
                rpnl = t.get("realizedPnl")
                if rpnl is not None and Decimal(str(rpnl)) != 0:
                    trade_time = int(t.get("time", 0) or 0)
                    if abs(trade_time - order_time_ms) <= 60 * 1000:
                        actual_pnl += Decimal(str(rpnl))
                        pnl_found = True
        except Exception:
            pass

        # Fallback: use income API (REALIZED_PNL + INSURANCE_CLEAR + COMMISSION)
        if not pnl_found:
            try:
                income_rows = broker.get_income_history(
                    start_time_ms=order_time_ms - 5 * 60 * 1000,
                    end_time_ms=order_time_ms + 5 * 60 * 1000,
                    limit=100,
                )
                for row in income_rows:
                    if row.get("incomeType") in (
                        "REALIZED_PNL", "COMMISSION", "INSURANCE_CLEAR",
                    ):
                        inc_sym = str(row.get("symbol", ""))
                        if inc_sym == symbol:
                            actual_pnl += Decimal(str(row.get("income", "0")))
            except Exception:
                pass

        total_loss += actual_pnl

        items.append({
            "timestamp": order_time_ms / 1000 if order_time_ms else None,
            "timeMs": order_time_ms,
            "symbol": symbol,
            "asset": symbol.replace("USDT", ""),
            "direction": direction,
            "type": type_label,
            "autoCloseType": auto_close_type,
            "price": avg_price,
            "quantity": qty,
            "realizedPnl": str(actual_pnl),
        })

    items.sort(key=lambda x: x.get("timeMs") or 0, reverse=True)
    return {
        "count": len(items),
        "totalLossUsdt": str(total_loss),
        "items": items,
    }


def _extract_account_equity(account_snapshot: dict[str, Any] | None) -> Decimal | None:
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


def _record_equity_snapshot(
    workdir: Path, account_snapshot: dict[str, Any] | None
) -> list[dict[str, Any]]:
    history_path = workdir / "runtime" / "account_equity_history.json"
    existing = _load_json(history_path, [])
    if isinstance(existing, list):
        return existing
    return []


def _print_terminal_report(report: dict[str, Any]) -> None:
    print(f"source: {report['source']}")
    account = report.get("account") or {}
    if account:
        print(
            "account:"
            f" wallet={account.get('totalWalletBalance', '-')}"
            f" available={account.get('availableBalance', '-')}"
            f" unrealized={account.get('totalUnrealizedProfit', '-')}"
            f" margin={account.get('totalMarginBalance', '-')}"
        )
    print(f"open_positions: {report['summary']['openPositions']}")
    for side in (LONG, SHORT):
        item = report["sideSummaries"][side]
        print(
            f"{item['label']}:"
            f" open={item['openPositions']}"
            f" unrealized={item['unrealizedProfit']}"
            f" realized={item['realizedPnlUsdt']}"
        )


def build_report(workdir: Path, dotenv_file: str = ".env") -> dict[str, Any]:
    load_dotenv(workdir / dotenv_file)
    config = build_config(workdir)
    broker = select_broker_adapter()
    state = migrate_state(_load_json(workdir / "runtime/state.json", {"positions": {}, "history": []}))
    reset_cutoff_ms = _reset_cutoff_ms(workdir)
    monitor_summary = _load_json(workdir / "runtime/monitor_summary.json", None)
    all_strategy_statuses = _load_json(workdir / "runtime/strategy_statuses.json", {})
    strategy_statuses = {
        key: value
        for key, value in all_strategy_statuses.items()
        if key in {LONG_STRATEGY_ID, SHORT_STRATEGY_ID}
    }
    signal_snapshots = {
        LONG_STRATEGY_ID: _load_signal_snapshot(workdir / "runtime/strong_positive_snapshot.json"),
        SHORT_STRATEGY_ID: _load_signal_snapshot(workdir / "runtime/strong_negative_snapshot.json"),
    }
    signal_count_history = _load_signal_count_history(workdir)
    signal_count_peak_stats = {
        LONG_STRATEGY_ID: _signal_count_peak_stats(signal_count_history, "longCount"),
        SHORT_STRATEGY_ID: _signal_count_peak_stats(signal_count_history, "shortCount"),
    }
    signal_count_entry_gate_stats = _build_signal_count_entry_gate_stats(workdir, config)
    strategy_statuses = {
        key: {
            **value,
            "signalSnapshotUpdatedAt": signal_snapshots.get(key, {}).get("updatedAt"),
            "signalSnapshotCount": signal_snapshots.get(key, {}).get("count", 0),
            "signalSnapshotItems": signal_snapshots.get(key, {}).get("items", []),
            "signalCountPeak24h": signal_count_peak_stats.get(key, {}),
            "signalCountEntryGate24h": signal_count_entry_gate_stats.get(key, {}),
        }
        for key, value in strategy_statuses.items()
    }
    local_positions_by_key = _local_state_positions_by_symbol_side(state)

    all_history = [
        event
        for event in state.get("history", [])
        if not reset_cutoff_ms or _event_time_ms(event) >= reset_cutoff_ms
    ]
    realized_history = [
        event
        for event in all_history
        if event.get("action") in REALIZED_EXIT_ACTIONS
    ]
    closed_history = _aggregate_closed_trade_history(
        [
            event
            for event in realized_history
            if event.get("action") in FULL_CLOSE_ACTIONS
        ],
        realized_history,
    )

    try:
        account_snapshot = broker.get_account_snapshot()
    except Exception as exc:
        logging.warning("account_snapshot_failed_using_local_state: %s", exc)
        account_snapshot = None
    if account_snapshot and account_snapshot.get("source") == "binance_testnet":
        positions = []
        for item in account_snapshot.get("positions", []):
            side = live_side_from_amount(item.get("positionAmt", "0"))
            if not side:
                continue
            local_position = local_positions_by_key.get((item.get("symbol"), side), {})
            entry_long_count, entry_short_count = _entry_signal_counts(
                side,
                local_position.get("entryAudit"),
            )
            quantity = abs(Decimal(str(item.get("positionAmt", "0"))))
            entry_price = Decimal(str(item.get("entryPrice", "0")))
            mark_price = Decimal(str(item.get("markPrice", "0")))
            entry_notional = abs(entry_price * quantity)
            current_value = abs(mark_price * quantity)
            unrealized_profit = Decimal(str(item.get("unRealizedProfit", "0")))
            pnl_pct = None
            margin_basis = Decimal("0")
            for key in ("positionInitialMargin", "initialMargin", "isolatedWallet", "isolatedMargin"):
                raw_value = item.get(key)
                if raw_value not in (None, "", "0", "0.0"):
                    margin_basis = abs(Decimal(str(raw_value)))
                    if margin_basis != Decimal("0"):
                        break
            if margin_basis != Decimal("0"):
                pnl_pct = (unrealized_profit / margin_basis) * Decimal("100")
            elif entry_notional != Decimal("0"):
                pnl_pct = (unrealized_profit / entry_notional) * Decimal("100")
            positions.append(
                {
                    "asset": item.get("symbol", "").replace("USDT", ""),
                    "contractSymbol": item.get("symbol"),
                    "side": side,
                    "entryPrice": entry_price,
                    "markPrice": mark_price,
                    "quantity": quantity,
                    "positionUsdt": entry_notional,
                    "currentValueUsdt": current_value,
                    "unrealizedProfit": unrealized_profit,
                    "pnlPct": pnl_pct,
                    "returnBasisUsdt": margin_basis if margin_basis != Decimal("0") else entry_notional,
                    "leverage": item.get("leverage"),
                    "marginMode": _normalize_margin_mode(item.get("marginType"))
                    or ("ISOLATED" if item.get("isolated") else None),
                    "status": "LIVE_TESTNET",
                    "openedAt": local_position.get("openedAt"),
                    "entryStrongLongCount": entry_long_count,
                    "entryStrongShortCount": entry_short_count,
                    "stopLossPrice": local_position.get("stopLossPrice"),
                    "stopLossStatus": local_position.get("stopLossStatus"),
                    "stopLossMode": local_position.get("stopLossMode"),
                    "breakevenActivatedAt": local_position.get("breakevenActivatedAt"),
                    "partialTakeProfitDoneAt": local_position.get("partialTakeProfitDoneAt"),
                }
            )
        long_positions = [row for row in positions if row.get("side") == LONG]
        short_positions = [row for row in positions if row.get("side") == SHORT]
        def _pick_common(rows: list[dict[str, Any]], key: str) -> Any:
            values = {
                row.get(key)
                for row in rows
                if row.get(key) not in (None, "", "0")
            }
            if len(values) == 1:
                return next(iter(values))
            if len(values) > 1:
                return "MIXED"
            return None
        account_snapshot = {
            **account_snapshot,
            "totalPositionValueUsdt": str(
                sum((row.get("currentValueUsdt") or Decimal("0")) for row in positions)
            ),
            "longLeverage": _pick_common(long_positions, "leverage"),
            "shortLeverage": _pick_common(short_positions, "leverage"),
            "longMarginMode": _pick_common(long_positions, "marginMode"),
            "shortMarginMode": _pick_common(short_positions, "marginMode"),
        }
        source = "binance_testnet"
    else:
        positions = _build_local_positions(state, broker)
        account_snapshot = {
            "source": "local_state",
            "positionCount": len(positions),
            "totalPositionValueUsdt": str(
                sum((row.get("currentValueUsdt") or Decimal("0")) for row in positions)
            ),
            "totalUnrealizedProfit": str(
                sum((row.get("unrealizedProfit") or Decimal("0")) for row in positions)
            ),
        }
        source = "local_state"

    equity_history = _record_equity_snapshot(workdir, account_snapshot)

    merged_strategy_statuses: dict[str, dict[str, Any]] = {}
    for key, value in strategy_statuses.items():
        current_candidate_items = _normalize_signal_rows(value.get("currentCandidateItems", []))
        snapshot_meta = signal_snapshots.get(key, {})
        snapshot_items = snapshot_meta.get("items", [])
        snapshot_count = int(snapshot_meta.get("count", 0) or 0)
        candidate_count = int(value.get("candidateCount", 0) or 0)
        merged_strategy_statuses[key] = {
            **value,
            "currentCandidateItems": current_candidate_items,
            "currentCandidateUpdatedAt": value.get("updatedAt"),
            "signalSnapshotUpdatedAt": snapshot_meta.get("updatedAt"),
            "signalSnapshotCount": snapshot_count,
            "signalSnapshotItems": snapshot_items,
            "signalCountPeak24h": signal_count_peak_stats.get(key, {}),
            "signalCountEntryGate24h": signal_count_entry_gate_stats.get(key, {}),
            "signalSnapshotIsProtected": _is_signal_snapshot_protected(
                current_candidate_items,
                snapshot_items,
                candidate_count,
                snapshot_count,
            ),
        }
    strategy_statuses = merged_strategy_statuses
    current_signal_counts = {
        LONG_STRATEGY_ID: int(
            (strategy_statuses.get(LONG_STRATEGY_ID) or {}).get("candidateCount", 0) or 0
        ),
        SHORT_STRATEGY_ID: int(
            (strategy_statuses.get(SHORT_STRATEGY_ID) or {}).get("candidateCount", 0) or 0
        ),
    }
    runtime_stats = _build_runtime_stats(
        all_history,
        equity_history,
        positions,
        strategy_statuses,
        reset_cutoff_ms,
    )
    open_frequency = _build_open_frequency(all_history)

    positions.sort(key=lambda row: (row.get("side") != LONG, row.get("contractSymbol") or ""))
    trade_history: list[dict[str, Any]] = []
    local_force_order_summary = _build_force_order_summary_from_close_events(closed_history)
    if source == "binance_testnet":
        force_order_summary = _build_cached_force_order_summary(
            workdir,
            broker,
            local_force_order_summary,
            reset_cutoff_ms=reset_cutoff_ms,
        )
        if False and force_order_summary.get("count", 0) == 0:
            fallback_force_items = []
            total_force_loss = Decimal("0")
            for event in sorted(closed_history, key=lambda item: item.get("timestamp", 0), reverse=True):
                reason = str(event.get("reason") or "").lower()
                if reason not in {"liquidation", "adl", "force_order"}:
                    continue
                realized_pnl = Decimal(
                    str(
                        event.get("realizedPnlUsdt")
                        if event.get("realizedPnlUsdt") not in (None, "")
                        else event.get("netRealizedPnlUsdt") or "0"
                    )
                )
                total_force_loss += realized_pnl
                fallback_force_items.append(
                    {
                        "timestamp": event.get("timestamp"),
                        "timeMs": event.get("closedAtMs"),
                        "symbol": event.get("contractSymbol"),
                        "asset": event.get("asset"),
                        "direction": "做多" if event.get("side") == LONG else "做空",
                        "type": "强制平仓" if reason != "adl" else "自动减仓(ADL)",
                        "autoCloseType": reason.upper(),
                        "price": event.get("exitPrice"),
                        "quantity": event.get("exitQty"),
                        "realizedPnl": str(realized_pnl),
                    }
                )
            if fallback_force_items:
                force_order_summary = {
                    "count": len(fallback_force_items),
                    "totalLossUsdt": str(total_force_loss),
                    "items": sorted(
                        fallback_force_items,
                        key=lambda item: item.get("timeMs") or 0,
                        reverse=True,
                    ),
                }
    else:
        force_order_summary = local_force_order_summary

    history_for_display = closed_history
    trade_history = _build_trade_history_from_close_events(
        closed_history,
        realized_history,
        all_history,
        signal_count_history,
    )[:20]
    total_realized = sum(
        (
            value
            for value in (_event_net_pnl_value(event) for event in realized_history)
            if value is not None
        ),
        Decimal("0"),
    )

    side_summaries = {
        LONG: _build_side_summary(LONG, positions, history_for_display, realized_history),
        SHORT: _build_side_summary(SHORT, positions, history_for_display, realized_history),
    }

    winners = sorted(
        positions,
        key=lambda item: item.get("unrealizedProfit") if item.get("unrealizedProfit") is not None else Decimal("-999999"),
        reverse=True,
    )
    losers = sorted(
        positions,
        key=lambda item: item.get("unrealizedProfit") if item.get("unrealizedProfit") is not None else Decimal("999999"),
    )
    best = winners[0] if winners else None
    worst = losers[0] if losers else None

    recent_closes = [
        {
            "timestamp": event.get("timestamp"),
            "closedAtMs": event.get("closedAtMs"),
            "confirmedClosed": event.get("confirmedClosed"),
            "closeRetryCount": event.get("closeRetryCount", 0),
            "asset": event.get("asset"),
            "contractSymbol": event.get("contractSymbol"),
            "side": event.get("side", LONG),
            "exitPrice": event.get("exitPrice"),
            "realizedPnlUsdt": event.get("netRealizedPnlUsdt"),
            "closeSide": event.get("closeSide"),
            "reason": event.get("reason"),
            "stopLossMode": event.get("stopLossMode"),
            "breakevenActivatedAt": event.get("breakevenActivatedAt"),
            "status": event.get("status"),
        }
        for event in sorted(closed_history, key=lambda item: item.get("timestamp", 0), reverse=True)[:20]
    ]

    unopened_candidates = _build_unopened_candidates(strategy_statuses)
    active_cooldowns = _build_active_cooldowns(state, config.cooldown_minutes, positions)
    cooldown_label = (
        f"{config.cooldown_minutes // 60}小时"
        if config.cooldown_minutes % 60 == 0
        else f"{config.cooldown_minutes}分钟"
    )
    trading_setup = {
        "longLeverage": account_snapshot.get("longLeverage")
        if source == "binance_testnet"
        else str(os.getenv("LEVERAGE", "1")),
        "shortLeverage": account_snapshot.get("shortLeverage")
        if source == "binance_testnet"
        else str(os.getenv("LEVERAGE", "1")),
        "longMarginMode": account_snapshot.get("longMarginMode")
        if source == "binance_testnet"
        else os.getenv("REQUIRED_MARGIN_MODE", "ISOLATED"),
        "shortMarginMode": account_snapshot.get("shortMarginMode")
        if source == "binance_testnet"
        else os.getenv("REQUIRED_MARGIN_MODE", "ISOLATED"),
    }
    rule_summary = _apply_live_trading_setup_to_rules(
        _augment_rule_summary(_build_rule_summary(config), config),
        config,
        trading_setup,
    )

    return {
        "source": source,
        "account": account_snapshot,
        "dryRun": config.dry_run,
        "tradingSetup": trading_setup,
        "ruleSummary": rule_summary,
        "productionReadiness": _build_readiness(
            source,
            account_snapshot,
            state,
            strategy_statuses,
            config,
            positions,
            closed_history,
        ),
        "riskStats": _build_risk_stats(
            positions,
            history_for_display,
            account_snapshot,
            equity_history,
            config,
        ),
        "runtimeStats": runtime_stats,
        "openFrequency": open_frequency,
        "recoveryStats": _build_recovery_stats(history_for_display),
        "attributionStats": _build_attribution_stats(
            history_for_display,
            all_history,
            current_signal_counts,
        ),
        "stopLossLeaderboard": _build_stop_loss_leaderboard(history_for_display),
        "accountCircuitBreaker": state.get("riskState", {}).get("accountCircuitBreaker", {}),
        "summary": {
            "openPositions": len(positions),
            "totalPositionValueUsdt": str(
                sum((row.get("currentValueUsdt") or Decimal("0")) for row in positions)
            ),
            "totalUnrealizedProfit": str(
                sum((row.get("unrealizedProfit") or Decimal("0")) for row in positions)
            ),
            "realizedPnlUsdt": str(total_realized),
            "partialTakeProfitCount": sum(
                1 for event in realized_history if event.get("action") in PARTIAL_EXIT_ACTIONS
            ),
            "closedCount": len(history_for_display),
            "closedWinCount": sum(1 for event in history_for_display if event.get("closeSide") == "win"),
            "closedLossCount": sum(1 for event in history_for_display if event.get("closeSide") == "loss"),
            "closedFlatCount": sum(1 for event in history_for_display if event.get("closeSide") == "flat"),
            "forceOrderCount": force_order_summary["count"],
            "bestSymbol": best.get("contractSymbol") if best else None,
            "bestPnl": str(best.get("unrealizedProfit")) if best else None,
            "worstSymbol": worst.get("contractSymbol") if worst else None,
            "worstPnl": str(worst.get("unrealizedProfit")) if worst else None,
        },
        "sideSummaries": side_summaries,
        "positions": [
            {
                **row,
                "entryPrice": str(row["entryPrice"]) if row.get("entryPrice") is not None else None,
                "markPrice": str(row["markPrice"]) if row.get("markPrice") is not None else None,
                "quantity": str(row["quantity"]) if row.get("quantity") is not None else None,
                "positionUsdt": str(row["positionUsdt"]) if row.get("positionUsdt") is not None else None,
                "currentValueUsdt": str(row["currentValueUsdt"]) if row.get("currentValueUsdt") is not None else None,
                "unrealizedProfit": str(row["unrealizedProfit"]) if row.get("unrealizedProfit") is not None else None,
                "pnlPct": str(row["pnlPct"]) if row.get("pnlPct") is not None else None,
                "returnBasisUsdt": str(row["returnBasisUsdt"]) if row.get("returnBasisUsdt") is not None else None,
                "leverage": str(row["leverage"]) if row.get("leverage") is not None else None,
                "marginMode": row.get("marginMode"),
                "openedAt": row.get("openedAt"),
                "entryStrongLongCount": row.get("entryStrongLongCount"),
                "entryStrongShortCount": row.get("entryStrongShortCount"),
                "stopLossPrice": row.get("stopLossPrice"),
                "stopLossStatus": row.get("stopLossStatus"),
                "stopLossMode": row.get("stopLossMode"),
                "breakevenActivatedAt": row.get("breakevenActivatedAt"),
                "partialTakeProfitDoneAt": row.get("partialTakeProfitDoneAt"),
            }
            for row in positions
        ],
        "recentCloses": recent_closes,
        "tradeHistory": trade_history,
        "forceOrderSummary": force_order_summary,
        "resetMarker": {
            "resetAtMs": reset_cutoff_ms,
            "active": reset_cutoff_ms is not None,
        },
        "monitorSummary": monitor_summary,
        "unopenedCandidates": unopened_candidates,
        "activeCooldowns": active_cooldowns,
        "configToggles": _augment_config_toggles(_build_config_toggles(config), config),
        "cooldownSummary": {
            "count": len(active_cooldowns),
            "minutes": config.cooldown_minutes,
            "label": cooldown_label,
        },
        "strategies": strategy_statuses,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="View current bot/account status.")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of text.")
    parser.add_argument("--dotenv", default=".env", help="Dotenv file path.")
    args = parser.parse_args()

    workdir = Path.cwd()
    report = build_report(workdir, args.dotenv)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    _print_terminal_report(report)


if __name__ == "__main__":
    main()
