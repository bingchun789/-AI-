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


def _build_rule_summary(config: Any) -> list[dict[str, str]]:
    cooldown_label = (
        f"{config.cooldown_minutes // 60} 小时"
        if config.cooldown_minutes % 60 == 0
        else f"{config.cooldown_minutes} 分钟"
    )
    leverage_text = f"{config.leverage}X {('逐仓' if config.required_margin_mode == 'ISOLATED' else config.required_margin_mode)}"
    activate_pct = config.profit_protection_activate_pct
    trail_pct = config.profit_protection_trail_pct
    return [
        {"title": "多空主逻辑", "value": "在榜继续持有，掉出榜单就平仓"},
        {"title": "杠杆模式", "value": f"做多 {leverage_text}，做空 {leverage_text}"},
        {"title": "冷却时间", "value": f"平仓后 {cooldown_label} 内不重开同方向"},
        {
            "title": "仓位上限",
            "value": f"做多最多 {config.max_long_open_positions} 个，做空最多 {config.max_short_open_positions} 个，总共最多 {config.max_total_open_positions} 个",
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
            "value": f"开仓后立即挂 {config.stop_loss_pct:.0f}% 止损单"
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
            "detail": f"开仓后立即挂 {config.stop_loss_pct}% 硬止损单。",
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
    return [*items[:5], rule, *new_rules, *items[5:]]


def _augment_config_toggles(items: list[dict[str, Any]], config: Any) -> list[dict[str, Any]]:
    toggle = {
        "key": "ENABLE_MIN_SIGNAL_COUNT_FILTER",
        "label": "最少强信号数过滤",
        "enabled": config.enable_min_signal_count_filter,
        "detail": f"同方向强烈看多/看空少于 {config.min_signal_count_to_open} 个时，不开该方向新仓。",
    }
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
            "key": "MAX_NOTIONAL_PER_TRADE_USDT",
            "label": "最大下单金额",
            "type": "number",
            "value": float(config.max_notional_per_trade_usdt),
            "min": 0,
            "step": 1,
            "unit": "USDT",
            "detail": "风险仓位计算后高于这个值会被封顶。",
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
    return [items[0], toggle, *risk_controls, *items[1:]] if items else [toggle, *risk_controls]


def _format_unopened_detail(item: dict[str, Any]) -> str | None:
    reason = item.get("reason")
    if reason == "signal_count_too_low":
        current_count = item.get("currentSignalCount")
        min_required = item.get("minSignalCountToOpen")
        if current_count not in (None, "") and min_required not in (None, ""):
            return f"当前强信号 {current_count} 个，至少需要 {min_required} 个"
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
            if peak > 0:
                strategy_max_drawdown_pct = (drawdown / peak) * Decimal("100")

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
    tracking_started_at = None
    tracking_sample_count = 0

    if equity_history:
        tracking_sample_count = len(equity_history)
        ordered_history = sorted(
            equity_history, key=lambda item: item.get("timestamp", 0)
        )
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
        if (
            current_equity is not None
            and peak_equity is not None
            and peak_equity > Decimal("0")
        ):
            current_drawdown = peak_equity - current_equity
            current_drawdown_pct = (current_drawdown / peak_equity) * Decimal("100")

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
        "equityTrackingStartedAt": tracking_started_at,
        "equityTrackingSamples": tracking_sample_count,
        "strategyMaxDrawdownUsdt": str(strategy_max_drawdown),
        "strategyMaxDrawdownPct": str(strategy_max_drawdown_pct) if strategy_max_drawdown_pct is not None else None,
        "maxConsecutiveLosses": max_loss_streak,
        "maxConsecutiveWins": max_win_streak,
        "currentUnrealizedPnlUsdt": str(current_unrealized),
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


def _aggregate_attribution_rows(
    rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for key, item in rows.items():
        trade_count = int(item["tradeCount"])
        win_count = int(item["winCount"])
        return_values = item.pop("_returnValues", [])
        avg_return_pct = (
            sum(return_values, Decimal("0")) / Decimal(len(return_values))
            if return_values
            else None
        )
        output.append(
            {
                "key": key,
                "tradeCount": trade_count,
                "winCount": win_count,
                "lossCount": int(item["lossCount"]),
                "winRatePct": str((Decimal(win_count) / Decimal(trade_count)) * Decimal("100"))
                if trade_count
                else None,
                "netRealizedPnlUsdt": str(item["netRealizedPnlUsdt"]),
                "avgReturnPct": str(avg_return_pct) if avg_return_pct is not None else None,
            }
        )
    output.sort(key=lambda item: Decimal(str(item["netRealizedPnlUsdt"])), reverse=True)
    return output


def _build_attribution_stats(
    closed_history: list[dict[str, Any]],
    all_history: list[dict[str, Any]],
) -> dict[str, Any]:
    by_asset: dict[str, dict[str, Any]] = {}
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
        reason_key = event.get("reason") or "-"
        opened_at = event.get("openedAt") or event.get("timestamp")
        hour_key = "-"
        if opened_at not in (None, ""):
            try:
                hour_key = time.strftime("%H:00", time.localtime(float(opened_at)))
            except Exception:
                hour_key = "-"
        for rows, key in ((by_asset, asset_key), (by_reason, reason_key), (by_hour, hour_key)):
            row = rows.setdefault(
                str(key),
                {
                    "tradeCount": 0,
                    "winCount": 0,
                    "lossCount": 0,
                    "netRealizedPnlUsdt": Decimal("0"),
                    "_returnValues": [],
                },
            )
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
    return {
        "summary": {
            "closedTradeCount": len(closed_history),
            "partialTakeProfitCount": partial_count,
            "avgHoldMinutes": str(avg_hold_minutes) if avg_hold_minutes is not None else None,
        },
        "byAsset": _aggregate_attribution_rows(by_asset)[:12],
        "byCloseReason": _aggregate_attribution_rows(by_reason),
        "byEntryHour": _aggregate_attribution_rows(by_hour),
    }


def _build_trade_history_from_close_events(
    close_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for event in sorted(
        close_events,
        key=lambda item: item.get("closedAtMs") or item.get("timestamp", 0),
        reverse=True,
    ):
        side = event.get("side", LONG)
        reason = str(event.get("reason") or "").lower()
        order_type = "MARKET"
        if reason in {"liquidation", "force_order"}:
            order_type = "强制平仓"
        elif reason == "adl":
            order_type = "自动减仓(ADL)"
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
                "symbol": event.get("contractSymbol"),
                "asset": event.get("asset"),
                "side": "SELL" if side == LONG else "BUY",
                "direction": "做多" if side == LONG else "做空",
                "action": "平仓",
                "type": order_type,
                "price": event.get("exitPrice"),
                "quantity": event.get("exitQty") or event.get("quantity"),
                "status": event.get("status"),
                "orderId": str(event.get("orderId", "")),
                "isClose": True,
                "closeReason": event.get("reason"),
                "realizedPnlUsdt": realized_pnl,
                "realizedPnlPct": realized_pnl_pct,
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


def _load_history_cache(workdir: Path) -> dict[str, Any]:
    payload = _load_json(_history_cache_path(workdir), {})
    return payload if isinstance(payload, dict) else {}


def _save_history_cache(workdir: Path, payload: dict[str, Any]) -> None:
    path = _history_cache_path(workdir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _event_time_ms(item: dict[str, Any]) -> int:
    for key in ("timeMs", "closedAtMs"):
        value = item.get(key)
        if value not in (None, ""):
            return int(value)
    timestamp = item.get("timestamp")
    if timestamp not in (None, ""):
        return int(float(timestamp) * 1000)
    return 0


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


def _build_cached_force_order_summary(
    workdir: Path,
    broker: Any,
    local_force_summary: dict[str, Any],
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

    if sync_due:
        last_force_sync_ms = int(cache.get("lastForceSyncMs", 0) or 0)
        start_time_ms = (
            max(0, last_force_sync_ms - overlap_ms)
            if last_force_sync_ms > 0
            else now_ms - 30 * 24 * 60 * 60 * 1000
        )
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
            cache["forceOrderItems"] = cached_force_items
            cache["lastForceSyncMs"] = now_ms
            cache["syncedAt"] = now
            _save_history_cache(workdir, cache)
        except Exception as exc:
            logging.warning("force_order_incremental_sync_failed: %s", exc)

    merged_items = _merge_force_order_items(
        local_force_summary.get("items", []),
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
) -> list[dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - (7 * 24 * 60 * 60 * 1000)

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
) -> list[dict[str, Any]]:
    """Build a complete order history from the allOrders API endpoint."""
    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - (30 * 24 * 60 * 60 * 1000)  # 30 days

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
    if not isinstance(existing, list):
        existing = []

    equity = _extract_account_equity(account_snapshot)
    now = time.time()
    if equity is not None:
        last_point = existing[-1] if existing else None
        should_append = True
        if last_point:
            last_ts = float(last_point.get("timestamp", 0) or 0)
            last_equity = Decimal(str(last_point.get("equityUsdt", "0")))
            if now - last_ts < 8 and last_equity == equity:
                should_append = False
        if should_append:
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
            history_path.write_text(
                json.dumps(existing, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
    return existing


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
    monitor_summary = _load_json(workdir / "runtime/monitor_summary.json", None)
    all_strategy_statuses = _load_json(workdir / "runtime/strategy_statuses.json", {})
    strategy_statuses = {
        key: value
        for key, value in all_strategy_statuses.items()
        if key in {LONG_STRATEGY_ID, SHORT_STRATEGY_ID}
    }
    local_positions_by_key = _local_state_positions_by_symbol_side(state)

    closed_history = [
        event
        for event in state.get("history", [])
        if event.get("action") in {"exit_long", "exit_short"}
    ]

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

    positions.sort(key=lambda row: (row.get("side") != LONG, row.get("contractSymbol") or ""))
    trade_history: list[dict[str, Any]] = []
    local_force_order_summary = _build_force_order_summary_from_close_events(closed_history)
    if source == "binance_testnet":
        force_order_summary = _build_cached_force_order_summary(
            workdir,
            broker,
            local_force_order_summary,
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
    trade_history = _build_trade_history_from_close_events(closed_history)[:20]
    total_realized = sum(
        Decimal(
            event.get("netRealizedPnlUsdt")
            if event.get("netRealizedPnlUsdt") not in (None, "")
            else event.get("realizedPnlUsdt")
        )
        for event in history_for_display
        if event.get("netRealizedPnlUsdt") not in (None, "") or event.get("realizedPnlUsdt") not in (None, "")
    )

    side_summaries = {
        LONG: _build_side_summary(LONG, positions, history_for_display),
        SHORT: _build_side_summary(SHORT, positions, history_for_display),
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

    return {
        "source": source,
        "account": account_snapshot,
        "dryRun": config.dry_run,
        "tradingSetup": {
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
        },
        "ruleSummary": _augment_rule_summary(_build_rule_summary(config), config),
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
        "recoveryStats": _build_recovery_stats(history_for_display),
        "attributionStats": _build_attribution_stats(history_for_display, state.get("history", [])),
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
