import argparse
import datetime as dt
import hashlib
import json
import logging
import os
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ai_select_futures_bot import (
    LONG,
    SHORT,
    LONG_STRATEGY_ID,
    SHORT_STRATEGY_ID,
    active_profit_lock_pct,
    build_config,
    enter_action,
    format_decimal_value,
    load_dotenv,
    migrate_state,
    position_age_hours,
    select_broker_adapter,
    should_trigger_profit_lock,
    should_trigger_profit_protection,
    should_trigger_time_exit,
    side_from_position,
)


EXPECTED_STRATEGIES = {
    LONG_STRATEGY_ID: LONG,
    SHORT_STRATEGY_ID: SHORT,
}

LOG_RECORD_RE = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) (?P<level>[A-Z]+) (?P<message>.*)$"
)
SNAPSHOT_PRESERVED_RE = re.compile(
    r"signal_snapshot_preserved side=(?P<side>LONG|SHORT) previous=(?P<previous>\d+) current=(?P<current>\d+)"
)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _fmt_ts(ts: float | None) -> str | None:
    if ts in (None, ""):
        return None
    try:
        return dt.datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _parse_log_timestamp(raw: str | None) -> float | None:
    if not raw:
        return None
    try:
        return dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S,%f").timestamp()
    except Exception:
        return None


def _tail_lines(path: Path, max_lines: int = 400) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except Exception:
        return []
    if max_lines <= 0:
        return []
    return [line.rstrip("\n") for line in lines[-max_lines:]]


def _read_log_records(path: Path, max_lines: int = 400, max_records: int = 80) -> list[dict[str, Any]]:
    lines = _tail_lines(path, max_lines=max_lines)
    records: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in lines:
        match = LOG_RECORD_RE.match(line)
        if match:
            if current:
                records.append(current)
            raw_ts = match.group("timestamp")
            current = {
                "timestamp": _parse_log_timestamp(raw_ts),
                "timestampText": raw_ts,
                "level": match.group("level").lower(),
                "message": match.group("message"),
                "lines": [line],
            }
            continue
        if current is not None:
            current["lines"].append(line)
    if current:
        records.append(current)
    if max_records > 0:
        return records[-max_records:]
    return records


def _extract_snapshot_preserve_events(
    records: list[dict[str, Any]],
    now: float,
) -> dict[str, dict[str, Any]]:
    latest_by_side: dict[str, dict[str, Any]] = {}
    for record in reversed(records):
        match = SNAPSHOT_PRESERVED_RE.search(str(record.get("message", "")))
        if not match:
            continue
        side = match.group("side")
        if side in latest_by_side:
            continue
        timestamp = record.get("timestamp")
        latest_by_side[side] = {
            "side": side,
            "timestamp": timestamp,
            "timestampText": record.get("timestampText"),
            "ageSeconds": (
                max(0, int(now - float(timestamp))) if timestamp not in (None, "") else None
            ),
            "previousCount": int(match.group("previous")),
            "currentCount": int(match.group("current")),
            "message": record.get("message"),
        }
    return latest_by_side


def _build_log_error_entry(source: str, record: dict[str, Any], now: float) -> dict[str, Any]:
    detail_lines = [line for line in record.get("lines", []) if line]
    return {
        "source": source,
        "timestamp": record.get("timestamp"),
        "timestampText": record.get("timestampText"),
        "ageSeconds": (
            max(0, int(now - float(record["timestamp"])))
            if record.get("timestamp") not in (None, "")
            else None
        ),
        "level": record.get("level"),
        "message": record.get("message"),
        "detail": "\n".join(detail_lines[:12]),
    }


def _load_recent_events(path: Path, limit: int = 20) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except Exception:
        return []
    items: list[dict[str, Any]] = []
    for raw in lines[-limit:]:
        raw = raw.strip()
        if not raw:
            continue
        try:
            items.append(json.loads(raw))
        except Exception:
            continue
    return list(reversed(items))


def _load_monitor_runtime_state(path: Path) -> dict[str, Any]:
    state = _load_json(path, {})
    if not isinstance(state, dict):
        state = {}
    if not isinstance(state.get("fingerprints"), dict):
        state["fingerprints"] = {}
    if not isinstance(state.get("exitCandidates"), dict):
        state["exitCandidates"] = {}
    return state


def _monitor_paths(workdir: Path) -> dict[str, Path]:
    runtime_dir = workdir / "runtime"
    return {
        "summary": runtime_dir / "monitor_summary.json",
        "report_json": runtime_dir / "monitor_report.json",
        "report_md": runtime_dir / "monitor_report.md",
        "events": runtime_dir / "monitor_events.jsonl",
        "state": runtime_dir / "monitor_state.json",
        "log": runtime_dir / "monitor.log",
        "bot_log": runtime_dir / "bot.log",
        "strategy_statuses": runtime_dir / "strategy_statuses.json",
        "state_file": runtime_dir / "state.json",
        "positive_snapshot": runtime_dir / "strong_positive_snapshot.json",
        "negative_snapshot": runtime_dir / "strong_negative_snapshot.json",
        "dashboard_cache": runtime_dir / "dashboard_cache.json",
    }


def _issue_fingerprint(issue: dict[str, Any]) -> str:
    stable = {
        "level": issue.get("level"),
        "rule": issue.get("rule"),
        "title": issue.get("title"),
        "detail": issue.get("detail"),
        "context": issue.get("context") or {},
    }
    raw = json.dumps(stable, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _add_issue(
    issues: list[dict[str, Any]],
    *,
    now: float,
    level: str,
    rule: str,
    title: str,
    detail: str,
    context: dict[str, Any] | None = None,
) -> None:
    issues.append(
        {
            "timestamp": now,
            "level": level,
            "rule": rule,
            "title": title,
            "detail": detail,
            "context": context or {},
        }
    )


def _position_counts(state: dict[str, Any]) -> tuple[int, int, int]:
    total = 0
    long_count = 0
    short_count = 0
    for position in state.get("positions", {}).values():
        side = side_from_position(position)
        total += 1
        if side == LONG:
            long_count += 1
        elif side == SHORT:
            short_count += 1
    return total, long_count, short_count


def _check_strategy_statuses(
    *,
    issues: list[dict[str, Any]],
    now: float,
    strategy_statuses: dict[str, Any],
    poll_interval_seconds: int,
) -> None:
    stale_threshold = max(poll_interval_seconds * 3, 60)
    for strategy_id, side in EXPECTED_STRATEGIES.items():
        status = strategy_statuses.get(strategy_id)
        if not isinstance(status, dict):
            _add_issue(
                issues,
                now=now,
                level="error",
                rule="strategy_status_missing",
                title="策略状态缺失",
                detail=f"{strategy_id} 当前没有状态数据。",
                context={"strategyId": strategy_id, "side": side},
            )
            continue
        updated_at = status.get("updatedAt")
        if updated_at in (None, ""):
            _add_issue(
                issues,
                now=now,
                level="warn",
                rule="strategy_status_missing_updated_at",
                title="策略状态无更新时间",
                detail=f"{strategy_id} 缺少 updatedAt 字段。",
                context={"strategyId": strategy_id},
            )
            continue
        age_seconds = max(0, int(now - float(updated_at)))
        if age_seconds > stale_threshold:
            _add_issue(
                issues,
                now=now,
                level="warn",
                rule="strategy_status_stale",
                title="策略状态过旧",
                detail=f"{strategy_id} 已经 {age_seconds} 秒没有更新。",
                context={
                    "strategyId": strategy_id,
                    "ageSeconds": age_seconds,
                    "staleThresholdSeconds": stale_threshold,
                },
            )


def _check_position_limits(
    *,
    issues: list[dict[str, Any]],
    now: float,
    state: dict[str, Any],
    config: Any,
) -> None:
    total, long_count, short_count = _position_counts(state)
    if total > config.max_total_open_positions:
        _add_issue(
            issues,
            now=now,
            level="error",
            rule="portfolio_limit_violated",
            title="总持仓超过上限",
            detail=f"当前总持仓 {total}，配置上限 {config.max_total_open_positions}。",
            context={"current": total, "limit": config.max_total_open_positions},
        )
    if long_count > config.max_long_open_positions:
        _add_issue(
            issues,
            now=now,
            level="error",
            rule="long_limit_violated",
            title="多仓数量超过上限",
            detail=f"当前多仓 {long_count}，配置上限 {config.max_long_open_positions}。",
            context={"current": long_count, "limit": config.max_long_open_positions},
        )
    if short_count > config.max_short_open_positions:
        _add_issue(
            issues,
            now=now,
            level="error",
            rule="short_limit_violated",
            title="空仓数量超过上限",
            detail=f"当前空仓 {short_count}，配置上限 {config.max_short_open_positions}。",
            context={"current": short_count, "limit": config.max_short_open_positions},
        )


def _check_min_signal_filter(
    *,
    issues: list[dict[str, Any]],
    now: float,
    strategy_statuses: dict[str, Any],
    config: Any,
) -> None:
    if not getattr(config, "enable_min_signal_count_filter", False):
        return
    threshold = int(getattr(config, "min_signal_count_to_open", 0) or 0)
    for strategy_id, status in strategy_statuses.items():
        if not isinstance(status, dict):
            continue
        side = status.get("side", LONG)
        candidate_count = int(status.get("candidateCount", 0) or 0)
        opened_count = int(status.get("openedCount", 0) or 0)
        latest_decisions = status.get("latestDecisions") or []
        open_action = enter_action(side)
        entered_assets = [
            row.get("asset")
            for row in latest_decisions
            if isinstance(row, dict) and row.get("action") == open_action
        ]
        if candidate_count < threshold and (opened_count > 0 or entered_assets):
            _add_issue(
                issues,
                now=now,
                level="error",
                rule="min_signal_count_filter_violated",
                title="最少强信号数过滤被违反",
                detail=(
                    f"{strategy_id} 当前强信号只有 {candidate_count} 个，"
                    f"阈值是 {threshold}，但本轮仍然发生了开仓。"
                ),
                context={
                    "strategyId": strategy_id,
                    "side": side,
                    "candidateCount": candidate_count,
                    "threshold": threshold,
                    "openedCount": opened_count,
                    "enteredAssets": entered_assets,
                },
            )


def _entry_audit_toggle_config_mismatches(
    audit: dict[str, Any],
    *,
    config: Any,
) -> list[str]:
    failures: list[str] = []
    expected_flags = [
        ("minSignalFilterEnabled", bool(config.enable_min_signal_count_filter), "最少强信号数过滤"),
        ("marginModeCheckEnabled", bool(config.skip_if_margin_mode_unavailable), "保证金模式校验"),
        ("marginUsageCapEnabled", bool(config.enable_margin_usage_cap), "保证金占用上限"),
        ("volatilityFilterEnabled", bool(config.enable_volatility_filter), "波动率过滤"),
        ("fundingRateFilterEnabled", bool(config.enable_funding_rate_filter), "资金费过滤"),
        ("correlationFilterEnabled", bool(config.enable_correlation_filter), "相关性过滤"),
        ("trendConfirmationEnabled", bool(config.enable_trend_confirmation), "趋势确认"),
        ("stopLossEnabled", bool(config.enable_stop_loss), "硬止损"),
    ]
    for field, expected, label in expected_flags:
        actual = audit.get(field)
        if actual is None:
            continue
        if bool(actual) != expected:
            failures.append(f"{label} 开关记录为 {actual}，当前配置应为 {expected}")
    stop_loss_pct = _to_decimal(audit.get("stopLossPct"))
    expected_stop_loss_pct = Decimal(str(config.stop_loss_pct))
    if (
        bool(config.enable_stop_loss)
        and stop_loss_pct is not None
        and stop_loss_pct != expected_stop_loss_pct
    ):
        failures.append(
            f"硬止损阈值记录为 {stop_loss_pct}% ，当前配置应为 {expected_stop_loss_pct}%"
        )
    return failures


def _disabled_toggle_decision_failures(decision: dict[str, Any], config: Any) -> list[str]:
    reason = str(decision.get("reason") or "")
    failures: list[str] = []
    if reason == "signal_count_too_low" and not bool(config.enable_min_signal_count_filter):
        failures.append("最少强信号数过滤已关闭，但本轮仍因该规则跳过开仓")
    if reason == "margin_usage_limit" and not bool(config.enable_margin_usage_cap):
        failures.append("保证金占用上限已关闭，但本轮仍因该规则跳过开仓")
    if reason == "high_volatility" and not bool(config.enable_volatility_filter):
        failures.append("波动率过滤已关闭，但本轮仍因该规则跳过开仓")
    if reason == "funding_too_high" and not bool(config.enable_funding_rate_filter):
        failures.append("资金费过滤已关闭，但本轮仍因该规则跳过开仓")
    if reason == "correlated_with_existing" and not bool(config.enable_correlation_filter):
        failures.append("相关性过滤已关闭，但本轮仍因该规则跳过开仓")
    if reason in {"trend_not_confirmed", "trend_data_unavailable"} and not bool(
        config.enable_trend_confirmation
    ):
        failures.append("趋势确认已关闭，但本轮仍因趋势规则拦截开仓")
    if reason == "signal_drop_guard" and not bool(config.enable_signal_drop_guard):
        failures.append("信号骤降保护已关闭，但本轮仍因该规则阻止平仓")
    if reason.startswith("margin_mode_missing:") and not bool(
        config.skip_if_margin_mode_unavailable
    ):
        failures.append("保证金模式校验已关闭，但本轮仍因该规则跳过开仓")
    return failures


def _check_strategy_toggle_enforcement(
    *,
    issues: list[dict[str, Any]],
    now: float,
    strategy_statuses: dict[str, Any],
    config: Any,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    checked_count = 0
    issue_count = 0
    missing_audit_count = 0

    for strategy_id, status in strategy_statuses.items():
        if not isinstance(status, dict):
            continue
        side = status.get("side", LONG)
        open_action = enter_action(side)
        for decision in status.get("latestDecisions") or []:
            if not isinstance(decision, dict):
                continue

            disabled_toggle_failures = _disabled_toggle_decision_failures(decision, config)
            if disabled_toggle_failures:
                checked_count += 1
                issue_count += 1
                detail = "；".join(disabled_toggle_failures[:3])
                records.append(
                    {
                        "strategyId": strategy_id,
                        "asset": decision.get("asset"),
                        "side": side,
                        "action": decision.get("action"),
                        "status": "error",
                        "details": detail,
                    }
                )
                _add_issue(
                    issues,
                    now=now,
                    level="error",
                    rule="strategy_toggle_enforcement_failed",
                    title="策略开关执行异常",
                    detail=f"{strategy_id} {decision.get('asset') or '-'}：{detail}",
                    context={
                        "strategyId": strategy_id,
                        "asset": decision.get("asset"),
                        "side": side,
                        "action": decision.get("action"),
                    },
                )
                continue

            if decision.get("action") != open_action:
                continue

            checked_count += 1
            audit = decision.get("entryAudit")
            if not isinstance(audit, dict):
                missing_audit_count += 1
                records.append(
                    {
                        "strategyId": strategy_id,
                        "asset": decision.get("asset"),
                        "side": side,
                        "action": decision.get("action"),
                        "status": "missing",
                        "details": "当前轮开仓决策缺少 entryAudit",
                    }
                )
                _add_issue(
                    issues,
                    now=now,
                    level="warn",
                    rule="strategy_toggle_audit_missing",
                    title="当前轮开仓缺少开关审计",
                    detail=f"{strategy_id} {decision.get('asset') or '-'} 当前轮开仓缺少 entryAudit。",
                    context={
                        "strategyId": strategy_id,
                        "asset": decision.get("asset"),
                        "side": side,
                    },
                )
                continue

            failures = [
                *_entry_audit_toggle_config_mismatches(audit, config=config),
                *_entry_audit_failures(audit),
            ]
            status_text = "error" if failures else "ok"
            records.append(
                {
                    "strategyId": strategy_id,
                    "asset": decision.get("asset"),
                    "side": side,
                    "action": decision.get("action"),
                    "status": status_text,
                    "details": "；".join(failures[:3]) if failures else "通过",
                }
            )
            if not failures:
                continue

            issue_count += 1
            _add_issue(
                issues,
                now=now,
                level="error",
                rule="strategy_toggle_enforcement_failed",
                title="策略开关执行异常",
                detail=f"{strategy_id} {decision.get('asset') or '-'}：{'；'.join(failures[:3])}",
                context={
                    "strategyId": strategy_id,
                    "asset": decision.get("asset"),
                    "side": side,
                    "action": decision.get("action"),
                },
            )

    return {
        "checked": True,
        "checkedCount": checked_count,
        "issueCount": issue_count,
        "missingAuditCount": missing_audit_count,
        "records": records[:20],
    }


def _check_cooldown_violations(
    *,
    issues: list[dict[str, Any]],
    now: float,
    state: dict[str, Any],
    cooldown_minutes: int,
) -> None:
    default_cooldown_seconds = int(cooldown_minutes * 60)
    last_exit_at: dict[tuple[str, str], float] = {}
    ordered_history = sorted(state.get("history", []), key=lambda item: item.get("timestamp", 0))
    for event in ordered_history:
        if not isinstance(event, dict):
            continue
        asset = event.get("asset")
        side = event.get("side")
        action = event.get("action")
        timestamp = event.get("timestamp")
        if asset in (None, "") or side not in {LONG, SHORT} or timestamp in (None, ""):
            continue
        key = (str(asset), str(side))
        event_ts = float(timestamp)
        if action in {"exit_long", "exit_short"}:
            last_exit_at[key] = event_ts
            continue
        if action not in {"enter_long", "enter_short"}:
            continue
        previous_exit_ts = last_exit_at.get(key)
        if previous_exit_ts is None:
            continue
        delta = event_ts - previous_exit_ts
        audit = event.get("audit") if isinstance(event.get("audit"), dict) else {}
        event_cooldown_minutes = audit.get("cooldownMinutes")
        if event_cooldown_minutes in (None, ""):
            # Legacy history records may not carry entry-audit fields; skip
            # retrospective cooldown judgments for those rows.
            continue
        try:
            cooldown_seconds = (
                int(float(event_cooldown_minutes) * 60)
                if event_cooldown_minutes not in (None, "")
                else default_cooldown_seconds
            )
        except Exception:
            cooldown_seconds = default_cooldown_seconds
        if delta < cooldown_seconds:
            _add_issue(
                issues,
                now=now,
                level="error",
                rule="cooldown_violated",
                title="冷却规则被违反",
                detail=(
                    f"{asset} {side} 在平仓后 {int(delta)} 秒又重新开仓，"
                    f"当前冷却要求是 {cooldown_seconds} 秒。"
                ),
                context={
                    "asset": asset,
                    "side": side,
                    "secondsSinceExit": int(delta),
                    "cooldownSeconds": cooldown_seconds,
                    "enterTimestamp": event_ts,
                    "lastExitTimestamp": previous_exit_ts,
                },
            )


def _normalize_live_positions(account_snapshot: dict[str, Any] | None) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    if not account_snapshot:
        return rows
    for item in account_snapshot.get("positions", []):
        try:
            amount = Decimal(str(item.get("positionAmt", "0")))
        except Exception:
            continue
        if amount == 0:
            continue
        side = LONG if amount > 0 else SHORT
        symbol = item.get("symbol")
        if not symbol:
            continue
        rows[(symbol, side)] = {
            "symbol": symbol,
            "side": side,
            "quantity": abs(amount),
            "entryPrice": item.get("entryPrice"),
        }
    return rows


def _normalize_local_positions(state: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for position in state.get("positions", {}).values():
        symbol = position.get("contractSymbol")
        side = side_from_position(position)
        if not symbol or side not in {LONG, SHORT}:
            continue
        quantity = _to_decimal(position.get("quantity")) or Decimal("0")
        rows[(symbol, side)] = {
            "symbol": symbol,
            "side": side,
            "quantity": abs(quantity),
            "entryPrice": position.get("entryPrice"),
            "asset": position.get("asset"),
        }
    return rows


def _check_live_state_consistency(
    *,
    issues: list[dict[str, Any]],
    now: float,
    state: dict[str, Any],
    config: Any,
) -> dict[str, Any]:
    result = {
        "checked": False,
        "ok": None,
        "error": None,
        "localCount": 0,
        "liveCount": 0,
    }
    if config.dry_run or os.getenv("BROKER_ADAPTER") != "binance_testnet":
        return result
    broker = select_broker_adapter()
    try:
        account_snapshot = broker.get_account_snapshot()
    except Exception as exc:
        _add_issue(
            issues,
            now=now,
            level="warn",
            rule="live_account_snapshot_failed",
            title="巡检时读取交易所持仓失败",
            detail=str(exc),
        )
        result["checked"] = True
        result["ok"] = False
        result["error"] = str(exc)
        return result

    local_positions = _normalize_local_positions(state)
    live_positions = _normalize_live_positions(account_snapshot)
    result["checked"] = True
    result["localCount"] = len(local_positions)
    result["liveCount"] = len(live_positions)

    local_keys = set(local_positions)
    live_keys = set(live_positions)

    for key in sorted(local_keys - live_keys):
        local_row = local_positions[key]
        _add_issue(
            issues,
            now=now,
            level="warn",
            rule="local_position_missing_on_exchange",
            title="本地有持仓但交易所没有",
            detail=f"{local_row['symbol']} {local_row['side']} 在本地存在，但交易所当前查不到。",
            context=local_row,
        )

    for key in sorted(live_keys - local_keys):
        live_row = live_positions[key]
        _add_issue(
            issues,
            now=now,
            level="warn",
            rule="exchange_position_missing_in_local_state",
            title="交易所有持仓但本地没有",
            detail=f"{live_row['symbol']} {live_row['side']} 在交易所存在，但本地状态缺失。",
            context=live_row,
        )

    for key in sorted(local_keys & live_keys):
        local_qty = local_positions[key]["quantity"]
        live_qty = live_positions[key]["quantity"]
        if live_qty == 0:
            continue
        diff = abs(local_qty - live_qty)
        diff_ratio = diff / live_qty if live_qty != 0 else Decimal("0")
        if diff > Decimal("0.000001") and diff_ratio > Decimal("0.05"):
            _add_issue(
                issues,
                now=now,
                level="warn",
                rule="position_quantity_mismatch",
                title="本地与交易所持仓数量不一致",
                detail=(
                    f"{local_positions[key]['symbol']} {local_positions[key]['side']} "
                    f"本地数量 {local_qty}，交易所数量 {live_qty}。"
                ),
                context={
                    "symbol": local_positions[key]["symbol"],
                    "side": local_positions[key]["side"],
                    "localQuantity": str(local_qty),
                    "liveQuantity": str(live_qty),
                    "diffRatio": str(diff_ratio * Decimal('100')),
                },
            )

    result["ok"] = not any(issue["rule"] in {
        "local_position_missing_on_exchange",
        "exchange_position_missing_in_local_state",
        "position_quantity_mismatch",
    } for issue in issues)
    return result


def _check_runtime_artifacts(
    *,
    issues: list[dict[str, Any]],
    now: float,
    workdir: Path,
    poll_interval_seconds: int,
    bot_runtime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    paths = _monitor_paths(workdir)
    stale_threshold = max(poll_interval_seconds * 3, 120)
    snapshot_preserve_by_side = (bot_runtime or {}).get("snapshotPreserveBySide") or {}
    artifact_specs = {
        "botLog": {
            "path": paths["bot_log"],
            "label": "bot.log",
            "staleThresholdSeconds": stale_threshold,
            "required": True,
        },
        "strategyStatuses": {
            "path": paths["strategy_statuses"],
            "label": "strategy_statuses.json",
            "staleThresholdSeconds": stale_threshold,
            "required": True,
        },
        "stateFile": {
            "path": paths["state_file"],
            "label": "state.json",
            "staleThresholdSeconds": stale_threshold,
            "required": True,
        },
        "dashboardCache": {
            "path": paths["dashboard_cache"],
            "label": "dashboard_cache.json",
            "staleThresholdSeconds": 180,
            "required": False,
        },
        "positiveSnapshot": {
            "path": paths["positive_snapshot"],
            "label": "strong_positive_snapshot.json",
            "staleThresholdSeconds": stale_threshold,
            "required": False,
            "side": LONG,
        },
        "negativeSnapshot": {
            "path": paths["negative_snapshot"],
            "label": "strong_negative_snapshot.json",
            "staleThresholdSeconds": stale_threshold,
            "required": False,
            "side": SHORT,
        },
    }
    results: dict[str, Any] = {}
    for key, spec in artifact_specs.items():
        path = spec["path"]
        exists = path.exists()
        age_seconds = None
        stale = None
        status_text = "未生成" if not spec["required"] else "缺失"
        status_class = "" if not spec["required"] else "bad"
        note = None
        if exists:
            age_seconds = max(0, int(now - path.stat().st_mtime))
            stale = age_seconds > int(spec["staleThresholdSeconds"])
            status_text = "过旧" if stale else "正常"
            status_class = "bad" if stale else "good"
            side = spec.get("side")
            preserve_event = snapshot_preserve_by_side.get(side) if side else None
            preserve_age = preserve_event.get("ageSeconds") if preserve_event else None
            if (
                stale
                and preserve_event
                and preserve_age is not None
                and preserve_age <= stale_threshold
                and (bot_runtime or {}).get("ok")
            ):
                stale = False
                status_text = "保留"
                status_class = "good"
                note = (
                    f"Bot 最近一次保留该侧快照: "
                    f"{preserve_event.get('timestampText') or '-'}"
                )
        results[key] = {
            "label": spec["label"],
            "path": path.as_posix(),
            "exists": exists,
            "ageSeconds": age_seconds,
            "staleThresholdSeconds": spec["staleThresholdSeconds"],
            "stale": stale,
            "statusText": status_text,
            "statusClass": status_class,
            "note": note,
        }
        if spec["required"] and not exists:
            _add_issue(
                issues,
                now=now,
                level="warn",
                rule="runtime_artifact_missing",
                title="关键运行文件缺失",
                detail=f"{spec['label']} 当前不存在。",
                context={"path": path.as_posix(), "artifact": key},
            )
        elif exists and stale:
            _add_issue(
                issues,
                now=now,
                level="warn",
                rule="runtime_artifact_stale",
                title="关键运行文件长时间未更新",
                detail=(
                    f"{spec['label']} 已经 {age_seconds} 秒没有更新，"
                    f"阈值是 {spec['staleThresholdSeconds']} 秒。"
                ),
                context={
                    "path": path.as_posix(),
                    "artifact": key,
                    "ageSeconds": age_seconds,
                    "staleThresholdSeconds": spec["staleThresholdSeconds"],
                },
            )
    return results


def _check_dashboard_http(
    *,
    issues: list[dict[str, Any]],
    now: float,
) -> dict[str, Any]:
    port = int(os.getenv("DASHBOARD_PORT", "8787"))
    url = f"http://127.0.0.1:{port}/api/health"
    result = {
        "checked": True,
        "ok": False,
        "url": url,
        "statusCode": None,
        "error": None,
    }
    try:
        request = Request(url, method="GET")
        with urlopen(request, timeout=2) as response:
            result["statusCode"] = int(getattr(response, "status", response.getcode()))
            body = response.read().decode("utf-8", errors="ignore")
        payload = json.loads(body or "{}")
        result["ok"] = result["statusCode"] == 200 and bool(payload.get("ok"))
    except HTTPError as exc:
        result["statusCode"] = exc.code
        result["error"] = str(exc)
    except (URLError, OSError, json.JSONDecodeError) as exc:
        result["error"] = str(exc)
    if not result["ok"]:
        _add_issue(
            issues,
            now=now,
            level="warn",
            rule="dashboard_health_failed",
            title="看板健康检查失败",
            detail=result["error"] or f"Dashboard /api/health 返回状态 {result['statusCode']}。",
            context={"url": url, "statusCode": result["statusCode"]},
        )
    return result


def _inspect_bot_runtime(
    *,
    issues: list[dict[str, Any]],
    now: float,
    workdir: Path,
    poll_interval_seconds: int,
) -> dict[str, Any]:
    paths = _monitor_paths(workdir)
    records = _read_log_records(paths["bot_log"], max_lines=800, max_records=120)
    snapshot_preserve_by_side = _extract_snapshot_preserve_events(records, now)
    success_records = [record for record in records if "cycle_complete" in str(record.get("message", ""))]
    failure_records = [
        record
        for record in records
        if "cycle_failed" in str(record.get("message", ""))
        or str(record.get("level")) in {"error", "critical"}
    ]
    latest_success = success_records[-1] if success_records else None
    latest_failure = failure_records[-1] if failure_records else None
    stale_threshold = max(poll_interval_seconds * 3, 120)
    success_age = (
        max(0, int(now - float(latest_success["timestamp"])))
        if latest_success and latest_success.get("timestamp") not in (None, "")
        else None
    )
    recent_window_seconds = int(os.getenv("MONITOR_LOG_ERROR_WINDOW_SECONDS", "86400"))
    recent_failures = [
        _build_log_error_entry("bot", record, now)
        for record in reversed(failure_records)
        if record.get("timestamp") not in (None, "")
        and now - float(record["timestamp"]) <= recent_window_seconds
    ][:10]
    unresolved_recent_failures = recent_failures
    if latest_success and latest_success.get("timestamp") not in (None, ""):
        latest_success_ts = float(latest_success["timestamp"])
        unresolved_recent_failures = [
            item
            for item in recent_failures
            if item.get("timestamp") not in (None, "")
            and float(item["timestamp"]) >= latest_success_ts
        ]
    result = {
        "checked": paths["bot_log"].exists(),
        "ok": success_age is not None and success_age <= stale_threshold,
        "lastSuccessAt": latest_success.get("timestamp") if latest_success else None,
        "lastSuccessText": latest_success.get("timestampText") if latest_success else None,
        "secondsSinceSuccess": success_age,
        "lastFailureAt": latest_failure.get("timestamp") if latest_failure else None,
        "lastFailureText": latest_failure.get("timestampText") if latest_failure else None,
        "lastFailureMessage": latest_failure.get("message") if latest_failure else None,
        "recentFailureCount": len(recent_failures),
        "recentFailures": recent_failures,
        "snapshotPreserveBySide": snapshot_preserve_by_side,
        "staleThresholdSeconds": stale_threshold,
        "logPath": paths["bot_log"].as_posix(),
    }
    if not paths["bot_log"].exists():
        _add_issue(
            issues,
            now=now,
            level="error",
            rule="bot_log_missing",
            title="Bot 日志不存在",
            detail=f"没有找到 {paths['bot_log'].name}，无法判断机器人最近是否正常轮询。",
            context={"path": paths["bot_log"].as_posix()},
        )
        return result
    if success_age is None:
        _add_issue(
            issues,
            now=now,
            level="error",
            rule="bot_success_missing",
            title="Bot 没有成功轮询记录",
            detail="bot.log 中没有发现 cycle_complete 记录。",
            context={"path": paths["bot_log"].as_posix()},
        )
    elif success_age > stale_threshold:
        _add_issue(
            issues,
            now=now,
            level="error",
            rule="bot_loop_stale",
            title="Bot 轮询长时间未成功",
            detail=f"最近一次成功轮询已经过去 {success_age} 秒，阈值是 {stale_threshold} 秒。",
            context={
                "lastSuccessAt": latest_success.get("timestampText") if latest_success else None,
                "secondsSinceSuccess": success_age,
                "staleThresholdSeconds": stale_threshold,
            },
        )
    if unresolved_recent_failures:
        latest = unresolved_recent_failures[0]
        _add_issue(
            issues,
            now=now,
            level="warn",
            rule="bot_recent_failures",
            title="Bot 最近出现异常",
            detail=f"最近一次成功轮询后仍发现 {len(unresolved_recent_failures)} 条异常，最新一条：{latest['message']}",
            context={
                "recentFailureCount": len(unresolved_recent_failures),
                "latestFailureAt": latest.get("timestampText"),
                "logPath": paths["bot_log"].as_posix(),
            },
        )
    return result


def _inspect_monitor_runtime(
    *,
    now: float,
    workdir: Path,
    interval_seconds: int,
) -> dict[str, Any]:
    paths = _monitor_paths(workdir)
    records = _read_log_records(paths["log"], max_lines=400, max_records=80)
    success_records = [
        record for record in records if "monitor_cycle_complete" in str(record.get("message", ""))
    ]
    failure_records = [
        record for record in records if "monitor_cycle_failed" in str(record.get("message", ""))
    ]
    latest_success = success_records[-1] if success_records else None
    latest_failure = failure_records[-1] if failure_records else None
    stale_threshold = max(interval_seconds * 3, 180)
    success_age = (
        max(0, int(now - float(latest_success["timestamp"])))
        if latest_success and latest_success.get("timestamp") not in (None, "")
        else None
    )
    return {
        "checked": paths["log"].exists(),
        "ok": success_age is not None and success_age <= stale_threshold,
        "lastSuccessAt": latest_success.get("timestamp") if latest_success else None,
        "lastSuccessText": latest_success.get("timestampText") if latest_success else None,
        "secondsSinceSuccess": success_age,
        "lastFailureAt": latest_failure.get("timestamp") if latest_failure else None,
        "lastFailureText": latest_failure.get("timestampText") if latest_failure else None,
        "lastFailureMessage": latest_failure.get("message") if latest_failure else None,
        "staleThresholdSeconds": stale_threshold,
        "logPath": paths["log"].as_posix(),
    }


def _check_dashboard_cache_freshness(
    *,
    issues: list[dict[str, Any]],
    now: float,
    workdir: Path,
) -> None:
    cache_path = _monitor_paths(workdir)["dashboard_cache"]
    if not cache_path.exists():
        return
    age_seconds = int(now - cache_path.stat().st_mtime)
    if age_seconds > 180:
        _add_issue(
            issues,
            now=now,
            level="warn",
            rule="dashboard_cache_stale",
            title="看板缓存过旧",
            detail=f"dashboard_cache.json 已经 {age_seconds} 秒没有更新。",
            context={"ageSeconds": age_seconds},
        )


def _recent_history_events(state: dict[str, Any], limit: int = 160) -> list[dict[str, Any]]:
    history = [item for item in state.get("history", []) if isinstance(item, dict)]
    history.sort(key=lambda item: float(item.get("timestamp", 0) or 0))
    if limit > 0:
        return history[-limit:]
    return history


def _entry_audit_failures(audit: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    if audit.get("minSignalFilterEnabled"):
        candidate_count = int(audit.get("candidateCount", 0) or 0)
        threshold = int(audit.get("minSignalThreshold", 0) or 0)
        if candidate_count < threshold:
            failures.append(f"候选数 {candidate_count} 小于阈值 {threshold}")
    if audit.get("cooldownPassed") is False:
        failures.append("冷却检查未通过")
    if audit.get("marginModeCheckEnabled") and audit.get("marginModePassed") is False:
        failures.append("保证金模式检查未通过")
    if audit.get("marginUsageCapEnabled"):
        margin_usage_pct = _to_decimal(audit.get("marginUsagePct"))
        max_margin_usage_pct = _to_decimal(audit.get("maxMarginUsagePct"))
        if (
            margin_usage_pct is not None
            and max_margin_usage_pct is not None
            and margin_usage_pct >= max_margin_usage_pct
        ):
            failures.append(
                f"保证金占用 {margin_usage_pct}% 达到阈值 {max_margin_usage_pct}%"
            )
    quote_volume = _to_decimal(audit.get("quoteVolume24hUsdt"))
    min_quote_volume = _to_decimal(audit.get("minQuoteVolume24hUsdt"))
    if (
        quote_volume is not None
        and min_quote_volume is not None
        and quote_volume < min_quote_volume
    ):
        failures.append(f"24h 成交额 {quote_volume} 小于阈值 {min_quote_volume}")
    if audit.get("trendConfirmationEnabled") and audit.get("trendConfirmed") is False:
        failures.append("趋势确认未通过")
    if audit.get("volatilityFilterEnabled"):
        max_range_pct = _to_decimal(audit.get("maxRangePct"))
        range_threshold = _to_decimal(audit.get("maxSingleBarRangePct"))
        if (
            max_range_pct is not None
            and range_threshold is not None
            and max_range_pct > range_threshold
        ):
            failures.append(f"波动率 {max_range_pct}% 超过阈值 {range_threshold}%")
    if audit.get("fundingRateFilterEnabled"):
        funding_rate_pct = _to_decimal(audit.get("fundingRatePct"))
        funding_threshold = _to_decimal(audit.get("maxAbsFundingRatePct"))
        if (
            funding_rate_pct is not None
            and funding_threshold is not None
            and funding_rate_pct > funding_threshold
        ):
            failures.append(f"资金费率 {funding_rate_pct}% 超过阈值 {funding_threshold}%")
    if audit.get("correlationFilterEnabled") and audit.get("correlationPassed") is False:
        correlated_symbol = audit.get("correlatedSymbol") or "-"
        failures.append(f"与现有持仓 {correlated_symbol} 相关性过高")
    if audit.get("stopLossEnabled") and not audit.get("stopLossConfigured"):
        failures.append("硬止损未配置成功")
    opened_before = int(audit.get("openedBefore", 0) or 0)
    cycle_limit = int(audit.get("cycleLimit", 0) or 0)
    if cycle_limit > 0 and opened_before >= cycle_limit:
        failures.append(f"本轮已开 {opened_before} 笔，达到阈值 {cycle_limit}")
    side_positions_before = int(audit.get("sidePositionsBefore", 0) or 0)
    side_limit = int(audit.get("sideLimit", 0) or 0)
    if side_limit > 0 and side_positions_before >= side_limit:
        failures.append(f"同向持仓 {side_positions_before} 达到阈值 {side_limit}")
    portfolio_positions_before = int(audit.get("portfolioPositionsBefore", 0) or 0)
    portfolio_limit = int(audit.get("portfolioLimit", 0) or 0)
    if portfolio_limit > 0 and portfolio_positions_before >= portfolio_limit:
        failures.append(f"总持仓 {portfolio_positions_before} 达到阈值 {portfolio_limit}")
    return failures


def _entry_stop_loss_recovered(state: dict[str, Any], event: dict[str, Any]) -> bool:
    asset = event.get("asset")
    side = event.get("side")
    if asset in (None, "") or side not in {LONG, SHORT}:
        return False
    for position in (state.get("positions") or {}).values():
        if not isinstance(position, dict):
            continue
        if position.get("asset") == asset and position.get("side") == side:
            return bool(position.get("stopLossConfigured"))
    return False


def _check_entry_execution_audit(
    *,
    issues: list[dict[str, Any]],
    now: float,
    state: dict[str, Any],
) -> dict[str, Any]:
    enter_actions = {enter_action(LONG), enter_action(SHORT)}
    records: list[dict[str, Any]] = []
    audited_count = 0
    issue_count = 0
    missing_audit_count = 0
    for event in reversed(_recent_history_events(state, limit=120)):
        if event.get("action") not in enter_actions:
            continue
        audit = event.get("audit")
        failures: list[str] = []
        status = "missing"
        if isinstance(audit, dict):
            audited_count += 1
            failures = _entry_audit_failures(audit)
            if "硬止损未配置成功" in failures and _entry_stop_loss_recovered(state, event):
                failures = [failure for failure in failures if failure != "硬止损未配置成功"]
            status = "error" if failures else "ok"
        else:
            missing_audit_count += 1
        record = {
            "timestamp": event.get("timestamp"),
            "timestampText": _fmt_ts(event.get("timestamp")),
            "asset": event.get("asset"),
            "side": event.get("side"),
            "status": status,
            "reason": event.get("reason"),
            "details": "；".join(failures[:3]) if failures else ("缺少审计字段" if status == "missing" else "通过"),
        }
        records.append(record)
        if failures:
            issue_count += 1
            _add_issue(
                issues,
                now=now,
                level="error",
                rule="entry_rule_audit_failed",
                title="开仓执行未通过规则审计",
                detail=f"{event.get('asset')} {event.get('side')} 开仓记录与门槛不一致：{'；'.join(failures[:3])}",
                context={
                    "asset": event.get("asset"),
                    "side": event.get("side"),
                    "timestamp": event.get("timestamp"),
                },
            )
    return {
        "checked": True,
        "auditedCount": audited_count,
        "issueCount": issue_count,
        "missingAuditCount": missing_audit_count,
        "records": records[:20],
    }


def _exit_audit_failures(event: dict[str, Any], audit: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    reason = event.get("reason")
    current_pnl_pct = _to_decimal(audit.get("currentPnlPct"))
    peak_pnl_pct = _to_decimal(audit.get("peakPnlPct"))
    age_hours = audit.get("ageHours")
    if reason == "profit_lock":
        if not audit.get("profitLockEnabled"):
            failures.append("分级锁盈已关闭却仍然触发该平仓")
        active_lock_pct = _to_decimal(audit.get("activeProfitLockPct"))
        if active_lock_pct is None:
            failures.append("未记录有效止盈锁阈值")
        elif current_pnl_pct is None or current_pnl_pct > active_lock_pct:
            failures.append(
                f"当前收益 {audit.get('currentPnlPct') or '-'} 未达到止盈锁阈值 {audit.get('activeProfitLockPct') or '-'}"
            )
        if peak_pnl_pct is None:
            failures.append("未记录峰值收益")
    elif reason == "profit_retrace":
        if not audit.get("profitProtectionEnabled"):
            failures.append("利润保护已关闭却仍然触发该平仓")
        activate_pct = _to_decimal(audit.get("profitProtectionActivatePct"))
        trail_pct = _to_decimal(audit.get("profitProtectionTrailPct"))
        drawdown_ratio_pct = _to_decimal(audit.get("drawdownRatioPct"))
        if peak_pnl_pct is None or activate_pct is None or peak_pnl_pct < activate_pct:
            failures.append("峰值收益未达到保护性止盈启动阈值")
        if drawdown_ratio_pct is None or trail_pct is None or drawdown_ratio_pct < trail_pct:
            failures.append("回撤比例未达到保护性止盈阈值")
    elif reason == "time_exit":
        if not audit.get("timeExitEnabled"):
            failures.append("时间退出已关闭却仍然触发该平仓")
        max_hold_hours = float(audit.get("maxHoldHours", 0) or 0)
        time_exit_min_pnl_pct = _to_decimal(audit.get("timeExitMinPnlPct"))
        if age_hours is None or float(age_hours) < max_hold_hours:
            failures.append(f"持仓时长 {age_hours} 小时未达到阈值 {max_hold_hours} 小时")
        if (
            current_pnl_pct is not None
            and time_exit_min_pnl_pct is not None
            and current_pnl_pct > time_exit_min_pnl_pct
        ):
            failures.append(
                f"当前收益 {current_pnl_pct}% 高于超时平仓阈值 {time_exit_min_pnl_pct}%"
            )
    elif reason == "signal_lost":
        rounds = int(audit.get("signalLostRounds", 0) or 0)
        confirm_rounds = int(audit.get("signalLostConfirmRounds", 0) or 0)
        if confirm_rounds > 0 and rounds < confirm_rounds:
            failures.append(f"信号丢失轮数 {rounds} 小于确认轮数 {confirm_rounds}")
    elif reason == "stop_loss":
        if not audit.get("stopLossEnabled"):
            failures.append("硬止损已关闭却仍然触发该平仓")
        stop_loss_pct = _to_decimal(audit.get("stopLossPct"))
        if stop_loss_pct is None:
            failures.append("未记录硬止损阈值")
        elif current_pnl_pct is None or current_pnl_pct > -stop_loss_pct:
            failures.append(
                f"当前收益 {audit.get('currentPnlPct') or '-'} 未跌破止损阈值 -{audit.get('stopLossPct') or '-'}"
            )
        stop_loss_configured = audit.get("stopLossConfigured")
        if stop_loss_configured is None:
            stop_loss_status = str(audit.get("stopLossStatus") or "").upper()
            stop_loss_configured = stop_loss_status not in {"", "DISABLED", "STOP_LOSS_SETUP_FAILED"}
        if bool(stop_loss_configured) and audit.get("configuredStopLossPrice") in (None, ""):
            failures.append("已配置硬止损但未记录止损价格")
    elif reason == "exchange_position_missing":
        if not audit.get("exchangePositionMissing"):
            failures.append("未记录交易所持仓缺失事实")
    return failures


def _check_exit_execution_audit(
    *,
    issues: list[dict[str, Any]],
    now: float,
    state: dict[str, Any],
) -> dict[str, Any]:
    exit_actions = {"exit_long", "exit_short"}
    records: list[dict[str, Any]] = []
    audited_count = 0
    issue_count = 0
    missing_audit_count = 0
    for event in reversed(_recent_history_events(state, limit=160)):
        if event.get("action") not in exit_actions:
            continue
        audit = event.get("audit")
        failures: list[str] = []
        status = "missing"
        if isinstance(audit, dict):
            audited_count += 1
            failures = _exit_audit_failures(event, audit)
            status = "error" if failures else "ok"
        else:
            missing_audit_count += 1
        record = {
            "timestamp": event.get("timestamp"),
            "timestampText": _fmt_ts(event.get("timestamp")),
            "asset": event.get("asset"),
            "side": event.get("side"),
            "reason": event.get("reason"),
            "status": status,
            "details": "；".join(failures[:3]) if failures else ("缺少审计字段" if status == "missing" else "通过"),
        }
        records.append(record)
        if failures:
            issue_count += 1
            _add_issue(
                issues,
                now=now,
                level="error",
                rule="exit_rule_audit_failed",
                title="平仓执行未通过规则审计",
                detail=f"{event.get('asset')} {event.get('side')} 平仓记录与触发条件不一致：{'；'.join(failures[:3])}",
                context={
                    "asset": event.get("asset"),
                    "side": event.get("side"),
                    "reason": event.get("reason"),
                    "timestamp": event.get("timestamp"),
                },
            )
    return {
        "checked": True,
        "auditedCount": audited_count,
        "issueCount": issue_count,
        "missingAuditCount": missing_audit_count,
        "records": records[:20],
    }


def _pending_exit_key(position: dict[str, Any], reason: str) -> str:
    return f"{position.get('contractSymbol') or '-'}|{side_from_position(position)}|{reason}"


def _pending_exit_candidates_for_position(config: Any, position: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    current_pnl_pct = _to_decimal(position.get("lastPnlPct"))
    peak_pnl_pct = _to_decimal(position.get("maxProfitPct"))
    age_hours = position_age_hours(position)
    if should_trigger_profit_lock(
        config=config,
        position=position,
        current_pnl_pct=current_pnl_pct,
        peak_pnl_pct=peak_pnl_pct,
    ):
        candidates.append(
            {
                "reason": "profit_lock",
                "currentPnlPct": position.get("lastPnlPct"),
                "peakPnlPct": position.get("maxProfitPct"),
                "threshold": format_decimal_value(active_profit_lock_pct(config, peak_pnl_pct)),
            }
        )
    if should_trigger_profit_protection(
        config=config,
        position=position,
        current_pnl_pct=current_pnl_pct,
        peak_pnl_pct=peak_pnl_pct,
    ):
        candidates.append(
            {
                "reason": "profit_retrace",
                "currentPnlPct": position.get("lastPnlPct"),
                "peakPnlPct": position.get("maxProfitPct"),
                "threshold": format_decimal_value(config.profit_protection_trail_pct),
            }
        )
    if should_trigger_time_exit(
        config=config,
        position=position,
        current_pnl_pct=current_pnl_pct,
    ):
        candidates.append(
            {
                "reason": "time_exit",
                "currentPnlPct": position.get("lastPnlPct"),
                "ageHours": round(age_hours, 4) if age_hours is not None else None,
                "threshold": format_decimal_value(config.time_exit_min_pnl_pct),
            }
        )
    signal_lost_rounds = int(position.get("signalLostRounds", 0) or 0)
    if signal_lost_rounds >= int(config.signal_lost_exit_confirm_rounds):
        candidates.append(
            {
                "reason": "signal_lost",
                "signalLostRounds": signal_lost_rounds,
                "threshold": int(config.signal_lost_exit_confirm_rounds),
            }
        )
    return candidates


def _check_pending_exit_execution(
    *,
    issues: list[dict[str, Any]],
    now: float,
    state: dict[str, Any],
    config: Any,
    bot_runtime: dict[str, Any],
    runtime_state: dict[str, Any],
) -> dict[str, Any]:
    stored_candidates = runtime_state.setdefault("exitCandidates", {})
    active_candidates: dict[str, Any] = {}
    records: list[dict[str, Any]] = []
    overdue_count = 0
    maturity_seconds = max(int(config.poll_interval_seconds) * 2, 180)
    bot_last_success = bot_runtime.get("lastSuccessAt")
    for position in state.get("positions", {}).values():
        if not isinstance(position, dict):
            continue
        for candidate in _pending_exit_candidates_for_position(config, position):
            key = _pending_exit_key(position, candidate["reason"])
            previous = stored_candidates.get(key) or {}
            first_seen_at = float(previous.get("firstSeenAt", now) or now)
            seen_count = int(previous.get("seenCount", 0) or 0) + 1
            age_seconds = max(0, int(now - first_seen_at))
            has_recent_bot_cycle = (
                bot_last_success not in (None, "")
                and float(bot_last_success) >= first_seen_at + int(config.poll_interval_seconds)
            )
            overdue = seen_count >= 2 and age_seconds >= maturity_seconds and has_recent_bot_cycle
            active_candidates[key] = {
                **candidate,
                "asset": position.get("asset"),
                "contractSymbol": position.get("contractSymbol"),
                "side": side_from_position(position),
                "firstSeenAt": first_seen_at,
                "lastSeenAt": now,
                "seenCount": seen_count,
            }
            record = {
                "asset": position.get("asset"),
                "contractSymbol": position.get("contractSymbol"),
                "side": side_from_position(position),
                "reason": candidate.get("reason"),
                "status": "overdue" if overdue else "pending",
                "ageSeconds": age_seconds,
                "seenCount": seen_count,
                "currentPnlPct": candidate.get("currentPnlPct"),
                "peakPnlPct": candidate.get("peakPnlPct"),
                "ageHours": candidate.get("ageHours"),
                "signalLostRounds": candidate.get("signalLostRounds"),
                "threshold": candidate.get("threshold"),
            }
            records.append(record)
            if overdue:
                overdue_count += 1
                _add_issue(
                    issues,
                    now=now,
                    level="error",
                    rule="pending_exit_not_executed",
                    title="持仓已满足平仓条件但仍未执行",
                    detail=(
                        f"{position.get('asset')} {side_from_position(position)} 已连续 {age_seconds} 秒满足 "
                        f"{candidate.get('reason')} 条件，但持仓仍然存在。"
                    ),
                    context={
                        "asset": position.get("asset"),
                        "side": side_from_position(position),
                        "reason": candidate.get("reason"),
                        "ageSeconds": age_seconds,
                        "seenCount": seen_count,
                    },
                )
    runtime_state["exitCandidates"] = active_candidates
    records.sort(key=lambda item: (0 if item["status"] == "overdue" else 1, -int(item["ageSeconds"] or 0)))
    return {
        "checked": True,
        "candidateCount": len(records),
        "overdueCount": overdue_count,
        "records": records[:20],
    }


def _collect_recent_log_errors(
    *,
    now: float,
    workdir: Path,
) -> list[dict[str, Any]]:
    paths = _monitor_paths(workdir)
    window_seconds = int(os.getenv("MONITOR_LOG_ERROR_WINDOW_SECONDS", "86400"))
    recent_errors: list[dict[str, Any]] = []
    for source, path in (("bot", paths["bot_log"]), ("monitor", paths["log"])):
        records = _read_log_records(path, max_lines=800, max_records=120)
        for record in reversed(records):
            message = str(record.get("message", ""))
            if (
                str(record.get("level")) not in {"error", "critical"}
                and "cycle_failed" not in message
                and "Traceback" not in message
            ):
                continue
            timestamp = record.get("timestamp")
            if timestamp not in (None, "") and now - float(timestamp) > window_seconds:
                continue
            recent_errors.append(_build_log_error_entry(source, record, now))
            if len(recent_errors) >= 10:
                return recent_errors
    return recent_errors


def _persist_monitor_events(
    workdir: Path,
    issues: list[dict[str, Any]],
    now: float,
    runtime_state: dict[str, Any] | None = None,
) -> None:
    paths = _monitor_paths(workdir)
    state = runtime_state if runtime_state is not None else _load_monitor_runtime_state(paths["state"])
    fingerprints = state.get("fingerprints", {})
    dedup_seconds = int(os.getenv("MONITOR_EVENT_DEDUP_SECONDS", "600"))
    fresh_fingerprints: dict[str, float] = {}
    for issue in issues:
        fingerprint = _issue_fingerprint(issue)
        last_seen = float(fingerprints.get(fingerprint, 0) or 0)
        if now - last_seen >= dedup_seconds:
            _append_jsonl(paths["events"], issue)
        fresh_fingerprints[fingerprint] = now
    retention_seconds = int(os.getenv("MONITOR_FINGERPRINT_RETENTION_SECONDS", "86400"))
    for fingerprint, last_seen in fingerprints.items():
        if now - float(last_seen) <= retention_seconds and fingerprint not in fresh_fingerprints:
            fresh_fingerprints[fingerprint] = float(last_seen)
    state["fingerprints"] = fresh_fingerprints
    state["updatedAt"] = now
    _write_json(paths["state"], state)


def _render_monitor_report_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# 系统巡检报告",
        "",
        f"- 生成时间: {_fmt_ts(summary.get('generatedAt')) or '-'}",
        f"- 巡检状态: {'正常' if summary.get('healthy') else '有异常'}",
        f"- 错误 / 警告 / 信息: {summary.get('errorCount', 0)} / {summary.get('warnCount', 0)} / {summary.get('infoCount', 0)}",
        "",
        "## 自动检查",
    ]

    bot_runtime = summary.get("botRuntime") or {}
    dashboard_health = summary.get("dashboardHealth") or {}
    live_state = (summary.get("checks") or {}).get("liveState") or {}
    report_paths = summary.get("reportPaths") or {}
    trade_audits = summary.get("tradeAudits") or {}
    entry_audit = trade_audits.get("entryExecution") or {}
    exit_audit = trade_audits.get("exitExecution") or {}
    pending_exit_audit = trade_audits.get("pendingExit") or {}

    lines.extend(
        [
            f"- Bot 最近成功轮询: {bot_runtime.get('lastSuccessText') or '-'}",
            f"- Bot 最近异常: {bot_runtime.get('lastFailureText') or '-'}",
            f"- Dashboard 健康检查: {'正常' if dashboard_health.get('ok') else '失败'}",
            f"- 交易所对账: {'正常' if live_state.get('ok') else ('未执行' if not live_state.get('checked') else '有差异')}",
            "",
            "## 报告文件",
            f"- JSON 报告: {report_paths.get('json') or '-'}",
            f"- Markdown 报告: {report_paths.get('markdown') or '-'}",
            f"- 事件流: {report_paths.get('events') or '-'}",
            f"- Bot 日志: {report_paths.get('botLog') or '-'}",
            f"- Monitor 日志: {report_paths.get('monitorLog') or '-'}",
            "",
            "## 交易规则审计",
            f"- 开仓审计异常: {entry_audit.get('issueCount', 0)} 条 / 已审计 {entry_audit.get('auditedCount', 0)} 条",
            f"- 平仓审计异常: {exit_audit.get('issueCount', 0)} 条 / 已审计 {exit_audit.get('auditedCount', 0)} 条",
            f"- 漏执行平仓: {pending_exit_audit.get('overdueCount', 0)} 条 / 当前候选 {pending_exit_audit.get('candidateCount', 0)} 条",
            "",
            "## 当前问题",
        ]
    )

    issues = summary.get("currentIssues") or []
    if not issues:
        lines.append("- 当前没有 error / warn 级别问题。")
    else:
        for issue in issues:
            lines.append(
                f"- [{issue.get('level', '-').upper()}] {issue.get('rule', '-')} | {issue.get('title', '-')} | {issue.get('detail', '-')}"
            )

    recent_incidents = summary.get("recentIncidents") or []
    lines.extend(["", "## 最近异常事件"])
    if not recent_incidents:
        lines.append("- 最近没有新的巡检事件。")
    else:
        for event in recent_incidents:
            lines.append(
                f"- {_fmt_ts(event.get('timestamp')) or '-'} | {event.get('level', '-')} | {event.get('rule', '-')} | {event.get('title', '-')}"
            )

    recent_log_errors = summary.get("recentLogErrors") or []
    lines.extend(["", "## 最近日志异常"])
    if not recent_log_errors:
        lines.append("- 最近没有捕获到日志级异常。")
    else:
        for event in recent_log_errors:
            lines.append(
                f"- {event.get('source', '-')} | {event.get('timestampText') or '-'} | {event.get('message') or '-'}"
            )

    return "\n".join(lines) + "\n"


def _write_monitor_reports(workdir: Path, summary: dict[str, Any]) -> None:
    paths = _monitor_paths(workdir)
    _write_json(paths["report_json"], summary)
    paths["report_md"].write_text(_render_monitor_report_markdown(summary), encoding="utf-8")


def _write_monitor_failure_snapshot(workdir: Path, exc: Exception) -> None:
    now = time.time()
    paths = _monitor_paths(workdir)
    issue = {
        "timestamp": now,
        "level": "error",
        "rule": "monitor_runtime_exception",
        "title": "巡检程序自身失败",
        "detail": str(exc),
        "context": {},
    }
    summary = {
        "generatedAt": now,
        "healthy": False,
        "errorCount": 1,
        "warnCount": 0,
        "infoCount": 0,
        "issueCounts": {"monitor_runtime_exception": 1},
        "currentIssues": [issue],
        "recentIncidents": [issue],
        "recentLogErrors": [],
        "botRuntime": {},
        "monitorRuntime": {
            "checked": True,
            "ok": False,
            "lastFailureAt": now,
            "lastFailureText": _fmt_ts(now),
            "lastFailureMessage": str(exc),
            "logPath": paths["log"].as_posix(),
        },
        "dashboardHealth": {},
        "runtimeArtifacts": {},
        "checks": {"liveState": {"checked": False, "ok": None}, "strategiesPresent": []},
        "positionCounts": {"total": 0, "long": 0, "short": 0},
        "reportPaths": {
            "json": paths["report_json"].as_posix(),
            "markdown": paths["report_md"].as_posix(),
            "events": paths["events"].as_posix(),
            "botLog": paths["bot_log"].as_posix(),
            "monitorLog": paths["log"].as_posix(),
        },
    }
    _append_jsonl(paths["events"], issue)
    _write_json(paths["summary"], summary)
    _write_monitor_reports(workdir, summary)


def _build_monitor_summary(
    *,
    now: float,
    issues: list[dict[str, Any]],
    state: dict[str, Any],
    strategy_statuses: dict[str, Any],
    live_check: dict[str, Any],
    bot_runtime: dict[str, Any],
    monitor_runtime: dict[str, Any],
    dashboard_health: dict[str, Any],
    runtime_artifacts: dict[str, Any],
    recent_log_errors: list[dict[str, Any]],
    recent_incidents: list[dict[str, Any]],
    trade_audits: dict[str, Any],
    workdir: Path,
) -> dict[str, Any]:
    error_count = sum(1 for issue in issues if issue.get("level") == "error")
    warn_count = sum(1 for issue in issues if issue.get("level") == "warn")
    info_count = sum(1 for issue in issues if issue.get("level") == "info")
    issue_counts: dict[str, int] = {}
    for issue in issues:
        rule = issue.get("rule", "unknown")
        issue_counts[rule] = issue_counts.get(rule, 0) + 1
    total_positions, long_count, short_count = _position_counts(state)
    paths = _monitor_paths(workdir)
    return {
        "generatedAt": now,
        "healthy": error_count == 0 and warn_count == 0,
        "errorCount": error_count,
        "warnCount": warn_count,
        "infoCount": info_count,
        "issueCounts": issue_counts,
        "currentIssues": issues,
        "recentIncidents": recent_incidents,
        "recentLogErrors": recent_log_errors,
        "botRuntime": bot_runtime,
        "monitorRuntime": monitor_runtime,
        "dashboardHealth": dashboard_health,
        "runtimeArtifacts": runtime_artifacts,
        "tradeAudits": trade_audits,
        "checks": {
            "liveState": live_check,
            "strategiesPresent": list(strategy_statuses.keys()),
        },
        "positionCounts": {
            "total": total_positions,
            "long": long_count,
            "short": short_count,
        },
        "reportPaths": {
            "json": paths["report_json"].as_posix(),
            "markdown": paths["report_md"].as_posix(),
            "events": paths["events"].as_posix(),
            "botLog": paths["bot_log"].as_posix(),
            "monitorLog": paths["log"].as_posix(),
        },
    }


def run_monitor_once(workdir: Path, dotenv_file: str = ".env") -> dict[str, Any]:
    load_dotenv(workdir / dotenv_file)
    config = build_config(workdir)
    state = migrate_state(_load_json(config.state_file, {"positions": {}, "history": []}))
    strategy_statuses = _load_json(_monitor_paths(workdir)["strategy_statuses"], {})
    runtime_state = _load_monitor_runtime_state(_monitor_paths(workdir)["state"])
    now = time.time()
    issues: list[dict[str, Any]] = []
    monitor_interval_seconds = int(os.getenv("MONITOR_INTERVAL_SECONDS", "60"))

    _check_strategy_statuses(
        issues=issues,
        now=now,
        strategy_statuses=strategy_statuses,
        poll_interval_seconds=config.poll_interval_seconds,
    )
    _check_position_limits(issues=issues, now=now, state=state, config=config)
    _check_min_signal_filter(
        issues=issues,
        now=now,
        strategy_statuses=strategy_statuses,
        config=config,
    )
    _check_cooldown_violations(
        issues=issues,
        now=now,
        state=state,
        cooldown_minutes=config.cooldown_minutes,
    )
    live_check = _check_live_state_consistency(
        issues=issues,
        now=now,
        state=state,
        config=config,
    )
    bot_runtime = _inspect_bot_runtime(
        issues=issues,
        now=now,
        workdir=workdir,
        poll_interval_seconds=config.poll_interval_seconds,
    )
    runtime_artifacts = _check_runtime_artifacts(
        issues=issues,
        now=now,
        workdir=workdir,
        poll_interval_seconds=config.poll_interval_seconds,
        bot_runtime=bot_runtime,
    )
    _check_dashboard_cache_freshness(
        issues=issues,
        now=now,
        workdir=workdir,
    )
    monitor_runtime = _inspect_monitor_runtime(
        now=now,
        workdir=workdir,
        interval_seconds=monitor_interval_seconds,
    )
    dashboard_health = _check_dashboard_http(
        issues=issues,
        now=now,
    )
    recent_log_errors = _collect_recent_log_errors(
        now=now,
        workdir=workdir,
    )
    entry_execution_audit = _check_entry_execution_audit(
        issues=issues,
        now=now,
        state=state,
    )
    strategy_toggle_audit = _check_strategy_toggle_enforcement(
        issues=issues,
        now=now,
        strategy_statuses=strategy_statuses,
        config=config,
    )
    exit_execution_audit = _check_exit_execution_audit(
        issues=issues,
        now=now,
        state=state,
    )
    pending_exit_audit = _check_pending_exit_execution(
        issues=issues,
        now=now,
        state=state,
        config=config,
        bot_runtime=bot_runtime,
        runtime_state=runtime_state,
    )
    trade_audits = {
        "entryExecution": entry_execution_audit,
        "strategyToggleEnforcement": strategy_toggle_audit,
        "exitExecution": exit_execution_audit,
        "pendingExit": pending_exit_audit,
    }

    severity_order = {"error": 0, "warn": 1, "info": 2}
    issues.sort(key=lambda item: (severity_order.get(item.get("level"), 9), item.get("rule", "")))
    _persist_monitor_events(workdir, issues, now, runtime_state=runtime_state)
    recent_incidents = _load_recent_events(_monitor_paths(workdir)["events"], limit=20)
    summary = _build_monitor_summary(
        now=now,
        issues=issues,
        state=state,
        strategy_statuses=strategy_statuses,
        live_check=live_check,
        bot_runtime=bot_runtime,
        monitor_runtime=monitor_runtime,
        dashboard_health=dashboard_health,
        runtime_artifacts=runtime_artifacts,
        recent_log_errors=recent_log_errors,
        recent_incidents=recent_incidents,
        trade_audits=trade_audits,
        workdir=workdir,
    )
    _write_json(_monitor_paths(workdir)["summary"], summary)
    _write_monitor_reports(workdir, summary)
    return summary


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
    parser = argparse.ArgumentParser(description="Run rule-based monitor for the AI Select system.")
    parser.add_argument("--loop", action="store_true", help="Run continuously.")
    parser.add_argument("--dotenv", default=".env", help="Dotenv file path.")
    parser.add_argument(
        "--interval",
        type=int,
        default=int(os.getenv("MONITOR_INTERVAL_SECONDS", "60")),
        help="Loop interval in seconds.",
    )
    args = parser.parse_args()

    workdir = Path.cwd()
    paths = _monitor_paths(workdir)
    setup_logging(paths["log"])

    while True:
        try:
            summary = run_monitor_once(workdir, args.dotenv)
            logging.info(
                "monitor_cycle_complete healthy=%s errors=%s warns=%s positions=%s",
                summary["healthy"],
                summary["errorCount"],
                summary["warnCount"],
                summary["positionCounts"]["total"],
            )
        except Exception as exc:
            logging.exception("monitor_cycle_failed: %s", exc)
            _write_monitor_failure_snapshot(workdir, exc)
        if not args.loop:
            break
        time.sleep(max(5, args.interval))


if __name__ == "__main__":
    main()
