import json
import time
from pathlib import Path
from typing import Any


STRATEGY_CONFIG_FILE = "strategies.json"
STRATEGY_STATUS_FILE = "runtime/strategy_statuses.json"


def _default_registry() -> dict[str, Any]:
    return {
        "strategies": [
            {
                "id": "ai_select_futures_long",
                "name": "AI 精选做多",
                "enabled": True,
                "category": "sentiment",
                "priority": 1,
                "min_balance_usdt": 0,
                "description": "抓取 Binance AI 精选里的强烈看多列表，存在 USDT 永续合约则做多。",
            },
            {
                "id": "ai_select_futures_short",
                "name": "AI 精选做空",
                "enabled": True,
                "category": "sentiment",
                "priority": 2,
                "min_balance_usdt": 0,
                "description": "抓取 Binance AI 精选里的强烈看空列表，存在 USDT 永续合约则做空。",
            },
        ]
    }


def write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.{time.time_ns()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def ensure_strategy_registry(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8-sig"))
    data = _default_registry()
    write_json_atomic(path, data)
    return data


def get_strategy_config(path: Path, strategy_id: str) -> dict[str, Any]:
    registry = ensure_strategy_registry(path)
    for strategy in registry.get("strategies", []):
        if strategy.get("id") == strategy_id:
            return strategy
    return {
        "id": strategy_id,
        "name": strategy_id,
        "enabled": True,
        "category": "unknown",
        "priority": 999,
        "min_balance_usdt": 0,
        "description": "",
    }


def read_strategy_statuses(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    current = json.loads(path.read_text(encoding="utf-8-sig"))
    current.pop("ai_select_futures", None)
    return current


def write_strategy_status(path: Path, strategy_id: str, status: dict[str, Any]) -> None:
    current = read_strategy_statuses(path)
    current[strategy_id] = {
        **status,
        "updatedAt": time.time(),
    }
    write_json_atomic(path, current)
