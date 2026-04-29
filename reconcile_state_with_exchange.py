import argparse
import json
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from ai_select_futures_bot import (
    LONG,
    SHORT,
    build_config,
    exit_action,
    load_dotenv,
    migrate_state,
    position_key,
    select_broker_adapter,
)


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"positions": {}, "history": []}
    return migrate_state(json.loads(path.read_text(encoding="utf-8-sig")))


def write_json_atomic(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def save_state(path: Path, state: dict) -> None:
    write_json_atomic(path, state)


def side_from_trade(trade_side: str) -> str:
    trade_side = (trade_side or "").upper()
    return SHORT if trade_side == "BUY" else LONG


def close_trade_side_for_position(position_side: str) -> str:
    return "SELL" if position_side == LONG else "BUY"


def summarize_close_from_api(
    *,
    broker,
    position: dict,
    income_rows: list[dict],
    force_rows: list[dict],
    now_ms: int,
) -> dict:
    symbol = position["contractSymbol"]
    side = position["side"]
    asset = position["asset"]
    opened_at_ms = int(float(position.get("openedAt") or time.time()) * 1000)
    trade_side = close_trade_side_for_position(side)

    trades = broker.get_user_trades(
        symbol=symbol,
        start_time_ms=max(0, opened_at_ms - 60_000),
        end_time_ms=now_ms,
        limit=1000,
    )
    grouped: dict[str, dict] = {}
    for trade in trades:
        if str(trade.get("side", "")).upper() != trade_side:
            continue
        trade_time = int(trade.get("time", 0) or 0)
        if trade_time < opened_at_ms:
            continue
        order_id = str(trade.get("orderId"))
        item = grouped.setdefault(
            order_id,
            {
                "orderId": order_id,
                "closedAtMs": trade_time,
                "qty": Decimal("0"),
                "notional": Decimal("0"),
                "tradeIds": [],
            },
        )
        qty = Decimal(str(trade.get("qty", "0") or "0"))
        price = Decimal(str(trade.get("price", "0") or "0"))
        item["qty"] += qty
        item["notional"] += price * qty
        item["tradeIds"].append(str(trade.get("id")))
        if trade_time > item["closedAtMs"]:
            item["closedAtMs"] = trade_time

    latest = None
    if grouped:
        latest = max(grouped.values(), key=lambda row: row["closedAtMs"])

    income_by_trade: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in income_rows:
        if str(row.get("symbol")) != symbol:
            continue
        trade_id = str(row.get("tradeId"))
        if trade_id in ("", "0", "None"):
            continue
        income_by_trade[trade_id] += Decimal(str(row.get("income", "0")))

    force_by_order = {
        str(row.get("orderId")): row
        for row in force_rows
        if str(row.get("symbol")) == symbol and row.get("orderId") not in (None, "")
    }

    if latest:
        net_realized = sum(
            (income_by_trade.get(trade_id, Decimal("0")) for trade_id in latest["tradeIds"]),
            Decimal("0"),
        )
        exit_price = None
        if latest["qty"] != Decimal("0"):
            exit_price = str(latest["notional"] / latest["qty"])
        reason = "exchange_trade"
        force_row = force_by_order.get(latest["orderId"])
        if force_row:
            auto_close_type = str(force_row.get("autoCloseType", "")).upper()
            if auto_close_type == "LIQUIDATION":
                reason = "liquidation"
            elif auto_close_type == "ADL":
                reason = "adl"
            else:
                reason = "force_order"
        return {
            "timestamp": latest["closedAtMs"] / 1000,
            "closedAtMs": latest["closedAtMs"],
            "confirmedClosed": True,
            "closeRetryCount": 0,
            "asset": asset,
            "contractSymbol": symbol,
            "side": side,
            "strategyId": position.get("strategyId"),
            "action": exit_action(side),
            "status": "FILLED",
            "reason": reason,
            "exitPrice": exit_price,
            "realizedPnlUsdt": str(net_realized),
            "estimatedFeeUsdt": None,
            "netRealizedPnlUsdt": str(net_realized),
            "closeSide": "win" if net_realized > 0 else ("loss" if net_realized < 0 else "flat"),
        }

    return {
        "timestamp": time.time(),
        "closedAtMs": now_ms,
        "confirmedClosed": True,
        "closeRetryCount": 0,
        "asset": asset,
        "contractSymbol": symbol,
        "side": side,
        "strategyId": position.get("strategyId"),
        "action": exit_action(side),
        "status": "POSITION_MISSING",
        "reason": "exchange_position_missing",
        "exitPrice": None,
        "realizedPnlUsdt": None,
        "estimatedFeeUsdt": None,
        "netRealizedPnlUsdt": None,
        "closeSide": "unknown",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile local state with Binance positions.")
    parser.add_argument("--apply", action="store_true", help="Write changes to runtime/state.json")
    args = parser.parse_args()

    workdir = Path.cwd()
    load_dotenv(workdir / ".env")
    config = build_config(workdir)
    broker = select_broker_adapter()
    state_path = config.state_file
    state = load_state(state_path)

    account = broker.get_account_snapshot()
    live_keys = {
        position_key(
            str(item.get("symbol", "")).replace("USDT", ""),
            LONG if Decimal(str(item.get("positionAmt", "0"))) > 0 else SHORT,
        )
        for item in account.get("positions", [])
    }

    local_positions = state.get("positions", {})
    stale_keys = sorted(set(local_positions.keys()) - live_keys)
    if not stale_keys:
        print("No stale positions found.")
        return

    now_ms = int(time.time() * 1000)
    start_time_ms = now_ms - (14 * 24 * 60 * 60 * 1000)
    income_rows = broker.get_income_history(
        start_time_ms=start_time_ms,
        end_time_ms=now_ms,
        limit=1000,
    )
    force_rows = broker.get_force_orders(
        start_time_ms=start_time_ms,
        end_time_ms=now_ms,
        limit=100,
    )

    appended = []
    for key in stale_keys:
        position = local_positions[key]
        close_event = summarize_close_from_api(
            broker=broker,
            position=position,
            income_rows=income_rows,
            force_rows=force_rows,
            now_ms=now_ms,
        )
        state.setdefault("history", []).append(close_event)
        state["positions"].pop(key, None)
        appended.append(close_event)

    backup_path = workdir / "runtime" / f"state.backup.{now_ms}.json"
    if args.apply:
        write_json_atomic(backup_path, state if False else load_state(state_path))
        save_state(state_path, state)

    print(f"stale_positions={len(stale_keys)} apply={args.apply}")
    if args.apply:
        print(f"backup={backup_path}")
    for item in appended:
        print(
            f"{item['contractSymbol']} {item['side']} reason={item['reason']} "
            f"pnl={item['netRealizedPnlUsdt']}"
        )


if __name__ == "__main__":
    main()
