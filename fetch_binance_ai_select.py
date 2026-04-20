import argparse
import json
from pathlib import Path
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright


PAGE_URL = "https://www.binance.com/zh-CN/markets/ai-select"
POSITIVE_LABEL_PARTS = ("强烈看多", "寮虹儓鐪嬪", "strong positive")
NEGATIVE_LABEL_PARTS = ("强烈看空", "寮虹儓鐪嬬┖", "strong negative")


def _normalize_label(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    if any(part in text for part in POSITIVE_LABEL_PARTS):
        return "Strong Positive"
    if any(part in text for part in NEGATIVE_LABEL_PARTS):
        return "Strong Negative"
    return None


def _is_address_style_asset(asset: str) -> bool:
    text = str(asset or "").strip().upper()
    return text.startswith("0X") or "@" in text


def _normalize_asset_code(raw: Any) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    if text.endswith("USDT") and len(text) > 4:
        prefix = text[:-4]
        if prefix.replace("_", "").replace("-", "").isalnum():
            return prefix
    return text


def _looks_like_asset_code(raw: Any) -> bool:
    text = _normalize_asset_code(raw)
    if not text or _is_address_style_asset(text):
        return False
    return text.replace("_", "").replace("-", "").isalnum()


def _pick_best_asset(node: dict[str, Any]) -> str:
    preferred_keys = (
        "displayAsset",
        "tokenSymbol",
        "baseAssetSymbol",
        "baseSymbol",
        "symbolName",
        "assetName",
        "baseAsset",
        "asset",
        "symbol",
        "underlyingAsset",
    )
    readable: list[str] = []
    fallback: list[str] = []

    for key in preferred_keys:
        value = node.get(key)
        normalized = _normalize_asset_code(value)
        if not normalized:
            continue
        if _looks_like_asset_code(normalized):
            readable.append(normalized)
        else:
            fallback.append(normalized)

    return readable[0] if readable else (fallback[0] if fallback else "")


def _build_sentiment_item(
    asset: str,
    rank: int | None,
    score_label: str,
    asset_type: str = "SPOT",
) -> dict[str, Any]:
    score_value = "7.0" if score_label == "Strong Positive" else "2.0"
    return {
        "rank": rank,
        "asset": asset,
        "baseAsset": asset,
        "assetType": asset_type,
        "metrics": {
            "sentiment_score": {
                "value": score_value,
                "valueLabel": score_label,
            }
        },
    }


def _extract_sentiment_items_from_payload(payload: Any) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            asset = _pick_best_asset(node)
            metrics = node.get("metrics", {}) if isinstance(node.get("metrics"), dict) else {}
            sentiment_score = (
                metrics.get("sentiment_score", {})
                if isinstance(metrics.get("sentiment_score"), dict)
                else {}
            )
            score_label = _normalize_label(
                sentiment_score.get("valueLabel")
                or node.get("valueLabel")
                or node.get("signalLabel")
                or node.get("label")
            )
            if asset and score_label:
                asset_type = (
                    node.get("assetType")
                    or node.get("symbolType")
                    or ("ALPHA" if "alpha" in json.dumps(node, ensure_ascii=False).lower() else "SPOT")
                )
                collected.append(
                    _build_sentiment_item(
                        asset=asset,
                        rank=node.get("rank"),
                        score_label=score_label,
                        asset_type=str(asset_type).upper(),
                    )
                )
            for value in node.values():
                walk(value)
            return

        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return collected


def _merge_items(
    preferred: list[dict[str, Any]],
    fallback: list[dict[str, Any]],
    score_label: str,
) -> list[dict[str, Any]]:
    preferred_by_rank = {
        item.get("rank"): item
        for item in preferred
        if item.get("rank") not in (None, "")
    }
    merged: dict[str, dict[str, Any]] = {}

    for item in fallback:
        label = _normalize_label(
            item.get("metrics", {}).get("sentiment_score", {}).get("valueLabel")
        )
        if label != score_label:
            continue

        rank = item.get("rank")
        asset = _normalize_asset_code(item.get("asset"))
        if (_is_address_style_asset(asset) or not _looks_like_asset_code(asset)) and rank in preferred_by_rank:
            item = preferred_by_rank[rank]
            asset = _normalize_asset_code(item.get("asset"))

        if not _looks_like_asset_code(asset):
            continue

        normalized_item = dict(item)
        normalized_item["asset"] = asset
        normalized_item["baseAsset"] = asset
        merged[asset] = normalized_item

    for item in preferred:
        asset = _normalize_asset_code(item.get("asset"))
        if not _looks_like_asset_code(asset):
            continue
        normalized_item = dict(item)
        normalized_item["asset"] = asset
        normalized_item["baseAsset"] = asset
        merged[asset] = normalized_item

    rows = list(merged.values())
    if score_label == "Strong Positive":
        rows.sort(key=lambda item: (item.get("rank") or 999999, str(item.get("asset") or "")))
    else:
        rows.sort(key=lambda item: (-(item.get("rank") or 0), str(item.get("asset") or "")))
    return rows


def _collect_string_matches(payload: Any, targets: set[str]) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []

    def walk(node: Any, trail: list[str]) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, [*trail, str(key)])
            return
        if isinstance(node, list):
            for index, value in enumerate(node):
                walk(value, [*trail, str(index)])
            return
        text = str(node or "")
        upper = text.upper()
        for target in targets:
            if target in upper:
                matches.append(
                    {
                        "target": target,
                        "path": ".".join(trail),
                        "value": text[:300],
                    }
                )
                break

    walk(payload, [])
    return matches


def _build_source_map(
    extracted_rows: list[dict[str, Any]],
    response_urls: list[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item, url in zip(extracted_rows, response_urls):
        label = item.get("metrics", {}).get("sentiment_score", {}).get("valueLabel")
        rows.append(
            {
                "asset": item.get("asset"),
                "rank": item.get("rank"),
                "assetType": item.get("assetType"),
                "scoreLabel": label,
                "url": url,
            }
        )
    return rows


def _build_alpha_symbol_map(captured_payloads: list[dict[str, Any]]) -> dict[str, str]:
    symbol_map: dict[str, str] = {}
    for item in captured_payloads:
        if "alpha-trade/aggTicker24" not in item.get("url", ""):
            continue
        payload = item.get("payload", {})
        for row in payload.get("data", []) if isinstance(payload, dict) else []:
            contract = str(row.get("contractAddress") or "").upper()
            chain_id = row.get("chainId")
            symbol = _normalize_asset_code(row.get("symbol"))
            if not contract or chain_id in (None, "") or not symbol:
                continue
            symbol_map[f"{contract}@{chain_id}"] = symbol
    return symbol_map


def _open_page(page) -> None:
    page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(5000)
    try:
        page.wait_for_function(
            """
            () => {
              const bodyText = (document.body && document.body.innerText) || '';
              return bodyText.includes('强烈看多')
                || bodyText.includes('强烈看空')
                || bodyText.includes('寮虹儓鐪嬪')
                || bodyText.includes('寮虹儓鐪嬬┖');
            }
            """,
            timeout=15000,
        )
    except PlaywrightTimeoutError:
        page.wait_for_timeout(3000)


def _scrape_visible_sentiment_rows(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        () => {
          const hasPositive = (text) =>
            text.includes('强烈看多')
            || text.includes('寮虹儓鐪嬪')
            || text.toLowerCase().includes('strong positive');
          const hasNegative = (text) =>
            text.includes('强烈看空')
            || text.includes('寮虹儓鐪嬬┖')
            || text.toLowerCase().includes('strong negative');
          const nodes = Array.from(document.querySelectorAll('tr, [role="row"], div, li'));
          const uniq = new Map();

          for (const node of nodes) {
            const text = (node.innerText || '').replace(/\\s+/g, ' ').trim();
            if (!text || text.length > 500) continue;

            let scoreLabel = null;
            if (hasPositive(text)) scoreLabel = 'Strong Positive';
            else if (hasNegative(text)) scoreLabel = 'Strong Negative';
            if (!scoreLabel) continue;

            const rankMatch = text.match(/(?:^|\\s)(\\d{1,4})(?:\\s|$)/);
            const assetMatches = text.match(/\\b[A-Z][A-Z0-9]{1,19}\\b/g) || [];
            const asset =
              assetMatches.find((value) => value !== 'ALPHA' && !value.startsWith('0X'))
              || assetMatches.find((value) => value !== 'ALPHA')
              || assetMatches[0];
            if (!asset) continue;

            const item = {
              asset,
              rank: rankMatch ? Number(rankMatch[1]) : null,
              scoreLabel,
              assetType: /alpha/i.test(text) ? 'ALPHA' : 'SPOT',
              rawTexts: [text],
            };
            const key = `${item.scoreLabel}|${item.rank ?? 'na'}|${item.asset}`;
            if (!uniq.has(key)) uniq.set(key, item);
          }

          return Array.from(uniq.values());
        }
        """
    )


def _find_last_page_number(page) -> int:
    value = page.evaluate(
        """
        () => {
          const nums = Array.from(document.querySelectorAll('button, a, [role="button"]'))
            .map((el) => (el.textContent || '').trim())
            .filter((text) => /^\\d+$/.test(text))
            .map((text) => Number(text));
          return nums.length ? Math.max(...nums) : 1;
        }
        """
    )
    return int(value or 1)


def _go_to_page(page, target_page: int) -> bool:
    clicked = page.evaluate(
        """
        (targetPage) => {
          const target = String(targetPage);
          const elements = Array.from(document.querySelectorAll('button, a, [role="button"]'));
          const match = elements.find((el) => (el.textContent || '').trim() === target);
          if (!match) return false;
          match.click();
          return true;
        }
        """,
        target_page,
    )
    if clicked:
        page.wait_for_timeout(2000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10000)
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(3000)
    return bool(clicked)


def fetch_rendered_signal_lists(interval: str, debug_targets: list[str] | None = None) -> dict[str, Any]:
    del interval
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            locale="zh-CN",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        captured_payloads: list[dict[str, Any]] = []
        is_closing = False

        def handle_response(response) -> None:
            nonlocal is_closing
            if is_closing:
                return
            url = response.url
            if "binance.com" not in url and "/bapi/" not in url:
                return
            try:
                payload = response.json()
            except BaseException:
                return
            captured_payloads.append({"url": url, "payload": payload})

        page.on("response", handle_response)
        _open_page(page)

        all_page_rows = list(_scrape_visible_sentiment_rows(page))
        debug_targets_upper = {target.strip().upper() for target in (debug_targets or []) if target.strip()}

        last_page = _find_last_page_number(page)
        if last_page > 1:
            for page_number in range(2, last_page + 1):
                if not _go_to_page(page, page_number):
                    continue
                all_page_rows.extend(_scrape_visible_sentiment_rows(page))

        positive_dom_items = [
            _build_sentiment_item(
                row["asset"],
                row.get("rank"),
                "Strong Positive",
                row.get("assetType", "SPOT"),
            )
            for row in all_page_rows
            if row.get("scoreLabel") == "Strong Positive"
        ]

        negative_dom_items = [
            _build_sentiment_item(
                row["asset"],
                row.get("rank"),
                "Strong Negative",
                row.get("assetType", "SPOT"),
            )
            for row in all_page_rows
            if row.get("scoreLabel") == "Strong Negative"
        ]

        extracted_from_api: list[dict[str, Any]] = []
        extracted_urls: list[str] = []
        debug_matches: list[dict[str, Any]] = []
        for item in captured_payloads:
            extracted_items = _extract_sentiment_items_from_payload(item["payload"])
            extracted_from_api.extend(extracted_items)
            extracted_urls.extend([item["url"]] * len(extracted_items))
            if debug_targets_upper:
                for match in _collect_string_matches(item["payload"], debug_targets_upper):
                    debug_matches.append(
                        {
                            "url": item["url"],
                            **match,
                        }
                    )

        alpha_symbol_map = _build_alpha_symbol_map(captured_payloads)

        normalized_extracted: list[dict[str, Any]] = []
        for item in extracted_from_api:
            normalized_item = dict(item)
            asset = _normalize_asset_code(normalized_item.get("asset"))
            if asset in alpha_symbol_map:
                normalized_item["asset"] = alpha_symbol_map[asset]
                normalized_item["baseAsset"] = alpha_symbol_map[asset]
                if str(normalized_item.get("assetType") or "").upper() == "ALPHA":
                    normalized_item["assetType"] = "ALPHA"
            normalized_extracted.append(normalized_item)

        positive_items = _merge_items(positive_dom_items, normalized_extracted, "Strong Positive")
        negative_items = _merge_items(negative_dom_items, normalized_extracted, "Strong Negative")

        dom_matches = []
        if debug_targets_upper:
            for row in all_page_rows:
                row_text = " ".join(row.get("rawTexts", []))
                upper_text = row_text.upper()
                if any(target in upper_text for target in debug_targets_upper):
                    dom_matches.append(row)

        is_closing = True
        page.remove_listener("response", handle_response)
        browser.close()
        return {
            "ok": True,
            "status": 200,
            "json": {
                "success": True,
                "data": {
                    "positiveItems": positive_items,
                    "negativeItems": negative_items,
                    "positiveCount": len(positive_items),
                    "negativeCount": len(negative_items),
                    "source": "rendered_page",
                    "capturedApiCount": len(captured_payloads),
                    "alphaSymbolMapCount": len(alpha_symbol_map),
                    "debugTargets": sorted(debug_targets_upper),
                    "domMatches": dom_matches,
                    "apiStringMatches": debug_matches[:100],
                    "extractedSourceMap": _build_source_map(normalized_extracted, extracted_urls),
                },
            },
        }


def build_api_path(dataset: str, interval: str) -> str:
    if dataset == "assets":
        return f"/bapi/apex/v1/friendly/apex/web/opportunity/assets?interval={interval}&type=sentiment"
    if dataset == "recommended":
        return f"/bapi/apex/v1/friendly/apex/web/opportunity/recommended-assets?type=sentiment&interval={interval}"
    if dataset == "filters":
        return f"/bapi/apex/v1/friendly/apex/web/opportunity/filter-options?type=sentiment&interval={interval}"
    if dataset == "configs":
        return f"/bapi/apex/v1/friendly/apex/web/opportunity/configs?type=sentiment&interval={interval}"
    raise ValueError(f"Unsupported dataset: {dataset}")


def fetch_dataset(dataset: str, interval: str) -> dict[str, Any]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            locale="zh-CN",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3000)
        payload = page.evaluate(
            """
            async (apiPath) => {
              const response = await fetch(apiPath, { credentials: 'include' });
              return {
                ok: response.ok,
                status: response.status,
                json: await response.json(),
              };
            }
            """,
            build_api_path(dataset, interval),
        )
        browser.close()
        return payload


def summarize_alpha_rows_from_assets(interval: str) -> list[dict[str, Any]]:
    payload = fetch_dataset("assets", interval)
    items = payload.get("json", {}).get("data", {}).get("items", [])
    rows: list[dict[str, Any]] = []
    for item in items:
        if str(item.get("assetType", "")).upper() != "ALPHA":
            continue
        rows.append(
            {
                "rank": item.get("rank"),
                "asset": item.get("asset"),
                "baseAsset": item.get("baseAsset"),
                "quoteAsset": item.get("quoteAsset"),
                "alphaId": item.get("alphaId"),
                "assetType": item.get("assetType"),
                "link": item.get("link"),
                "metrics": item.get("metrics"),
            }
        )
    rows.sort(key=lambda item: item.get("rank") or 999999)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Binance AI Select data through a real browser session."
    )
    parser.add_argument(
        "--dataset",
        choices=["assets", "recommended", "filters", "configs", "rendered-signals"],
        default="assets",
        help="Which AI Select dataset to fetch.",
    )
    parser.add_argument(
        "--interval",
        default="1h",
        help="Interval passed to the Binance endpoint. Default: 1h",
    )
    parser.add_argument("--output", help="Optional output JSON file path.")
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON to stdout or file.",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print a compact summary instead of the full JSON payload.",
    )
    parser.add_argument(
        "--targets",
        help="Comma-separated asset symbols to debug inside rendered/API payloads.",
    )
    parser.add_argument(
        "--dump-alpha-assets",
        action="store_true",
        help="Dump raw ALPHA rows from the opportunity/assets payload.",
    )
    args = parser.parse_args()
    debug_targets = [item.strip() for item in (args.targets or "").split(",") if item.strip()]

    if args.dump_alpha_assets:
        payload_to_print = {
            "dataset": "assets",
            "interval": args.interval,
            "alphaRows": summarize_alpha_rows_from_assets(args.interval),
        }
        text = json.dumps(payload_to_print, ensure_ascii=False, indent=2)
        if args.output:
            Path(args.output).write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")
        else:
            print(text)
        return

    if args.dataset == "rendered-signals":
        result = fetch_rendered_signal_lists(args.interval, debug_targets=debug_targets)
    else:
        result = fetch_dataset(args.dataset, args.interval)

    if args.summary:
        if args.dataset == "rendered-signals":
            data = result.get("json", {}).get("data", {})
            summary = {
                "dataset": args.dataset,
                "interval": args.interval,
                "http_status": result.get("status"),
                "success": result.get("ok"),
                "capturedApiCount": data.get("capturedApiCount"),
                "alphaSymbolMapCount": data.get("alphaSymbolMapCount"),
                "positiveCount": data.get("positiveCount"),
                "negativeCount": data.get("negativeCount"),
                "positiveAssets": [item.get("asset") for item in data.get("positiveItems", [])[:20]],
                "negativeAssets": [item.get("asset") for item in data.get("negativeItems", [])[:20]],
                "debugTargets": data.get("debugTargets", []),
                "domMatches": data.get("domMatches", []),
                "apiStringMatches": data.get("apiStringMatches", [])[:20],
                "positiveSources": [
                    row
                    for row in data.get("extractedSourceMap", [])
                    if row.get("scoreLabel") == "Strong Positive"
                ][:30],
                "negativeSources": [
                    row
                    for row in data.get("extractedSourceMap", [])
                    if row.get("scoreLabel") == "Strong Negative"
                ][:30],
            }
            payload_to_print = summary
        else:
            items = result.get("json", {}).get("data", {}).get("items", [])
            payload_to_print = {
                "dataset": args.dataset,
                "interval": args.interval,
                "http_status": result.get("status"),
                "success": result.get("ok"),
                "item_count": len(items),
                "first_assets": [item.get("asset") for item in items[:10]],
            }
    else:
        payload_to_print = result

    text = json.dumps(payload_to_print, ensure_ascii=False, indent=2 if args.pretty or args.summary else None)
    if args.output:
        Path(args.output).write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")
    else:
        print(text)


if __name__ == "__main__":
    main()
