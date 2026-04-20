import argparse
import json
from pathlib import Path
from typing import Any

from ai_select_futures_bot import (
    LONG,
    SHORT,
    LONG_STRATEGY_ID,
    SHORT_STRATEGY_ID,
    build_config,
    load_dotenv,
    select_broker_adapter,
)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8-sig"))


def live_positions_by_asset(account_snapshot: dict[str, Any] | None) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if not account_snapshot:
        return result
    for item in account_snapshot.get("positions", []) or []:
        symbol = str(item.get("symbol", ""))
        if not symbol.endswith("USDT"):
            continue
        amount = str(item.get("positionAmt", "0"))
        try:
            amt = float(amount)
        except Exception:
            continue
        if amt == 0:
            continue
        side = LONG if amt > 0 else SHORT
        asset = symbol[:-4]
        result[(asset, side)] = item
    return result


def decisions_by_asset(strategy_statuses: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for strategy_id, side in ((LONG_STRATEGY_ID, LONG), (SHORT_STRATEGY_ID, SHORT)):
        for item in strategy_statuses.get(strategy_id, {}).get("latestDecisions", []) or []:
            asset = item.get("asset")
            if not asset:
                continue
            rows[(asset, side)] = item
    return rows


def snapshot_rows(path: Path, side: str) -> list[dict[str, Any]]:
    rows = load_json(path, [])
    if not isinstance(rows, list):
        return []
    for row in rows:
        row["side"] = side
    return rows


def print_section(title: str, rows: list[dict[str, Any]]) -> None:
    print(f"\n=== {title} ({len(rows)}) ===")
    if not rows:
        print("无")
        return
    for row in rows:
        extra = []
        if row.get("scoreLabel"):
            extra.append(f"标签={row['scoreLabel']}")
        if row.get("decisionAction"):
            extra.append(f"动作={row['decisionAction']}")
        if row.get("decisionReason"):
            extra.append(f"原因={row['decisionReason']}")
        if row.get("hasLivePosition"):
            extra.append("交易所持仓=是")
        if row.get("contractSymbol"):
            extra.append(f"合约={row['contractSymbol']}")
        if row.get("quoteVolume24hUsdt"):
            extra.append(f"24h额={row['quoteVolume24hUsdt']}")
        if row.get("note"):
            extra.append(f"备注={row['note']}")
        print(
            f"rank={row.get('rank', '-')} asset={row.get('asset', '-')} side={row.get('side', '-')}"
            + (f" | {'; '.join(extra)}" if extra else "")
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect candidate assets vs decisions and live positions.")
    parser.add_argument("--dotenv", default=".env", help="Dotenv file path.")
    parser.add_argument("--asset", help="Optional asset filter, e.g. BLESS")
    args = parser.parse_args()

    workdir = Path.cwd()
    load_dotenv(workdir / args.dotenv)
    config = build_config(workdir)
    broker = select_broker_adapter()

    strategy_statuses = load_json(workdir / "runtime" / "strategy_statuses.json", {})
    positive_rows = snapshot_rows(config.positive_snapshot_file, LONG)
    negative_rows = snapshot_rows(config.negative_snapshot_file, SHORT)
    snapshot_all = positive_rows + negative_rows

    decisions = decisions_by_asset(strategy_statuses)
    account_snapshot = None
    live_positions = {}
    try:
        account_snapshot = broker.get_account_snapshot()
        live_positions = live_positions_by_asset(account_snapshot)
    except Exception as exc:
        print(f"读取交易所持仓失败: {exc}")

    if args.asset:
        target = args.asset.upper().strip()
        snapshot_all = [row for row in snapshot_all if str(row.get("asset", "")).upper() == target]

    enriched: list[dict[str, Any]] = []
    missing_from_decisions: list[dict[str, Any]] = []

    for row in snapshot_all:
        asset = row.get("asset")
        side = row.get("side")
        decision = decisions.get((asset, side), {})
        live_position = live_positions.get((asset, side))
        item = {
            **row,
            "decisionAction": decision.get("action"),
            "decisionReason": decision.get("reason"),
            "contractSymbol": live_position.get("symbol") if live_position else None,
            "hasLivePosition": live_position is not None,
            "quoteVolume24hUsdt": live_position.get("notional") if live_position else None,
        }
        if not decision:
            item["note"] = "本轮候选里有它，但 strategy_statuses 里没有这条决策"
            missing_from_decisions.append(item)
        enriched.append(item)

    skipped = [row for row in enriched if row.get("decisionAction") == "skip"]
    held = [row for row in enriched if row.get("decisionAction") == "hold"]
    entered = [row for row in enriched if row.get("decisionAction") in {"enter_long", "enter_short"}]

    print(f"数据源目录: {workdir}")
    print(
        f"快照候选: 做多 {len(positive_rows)} / 做空 {len(negative_rows)} / 总计 {len(positive_rows) + len(negative_rows)}"
    )
    print(
        f"策略记录: 做多 {len(strategy_statuses.get(LONG_STRATEGY_ID, {}).get('latestDecisions', []) or [])}"
        f" / 做空 {len(strategy_statuses.get(SHORT_STRATEGY_ID, {}).get('latestDecisions', []) or [])}"
    )
    if account_snapshot:
        print(f"交易所当前持仓数: {account_snapshot.get('positionCount')}")

    print_section("本轮新开仓", entered)
    print_section("本轮继续持有", held)
    print_section("本轮未开仓", skipped)
    print_section("候选存在但决策记录缺失", missing_from_decisions)


if __name__ == "__main__":
    main()
