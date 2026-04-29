import argparse
import json
import re
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

try:
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
except ModuleNotFoundError:
    PlaywrightTimeoutError = TimeoutError
    sync_playwright = None


PAGE_URL = "https://www.binance.com/zh-CN/markets/ai-select"
ALPHA_AGGREGATE_URL = (
    "https://www.binance.com/bapi/defi/v1/public/alpha-trade/aggTicker24?dataType=aggregate"
)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
POSITIVE_LABEL_PARTS = ("强烈看多", "寮虹儓鐪嬪", "strong positive")
NEGATIVE_LABEL_PARTS = ("强烈看空", "寮虹儓鐪嬬┖", "strong negative")


POSITIVE_LABEL_PARTS = (*POSITIVE_LABEL_PARTS, "强烈看多")
NEGATIVE_LABEL_PARTS = (*NEGATIVE_LABEL_PARTS, "强烈看空")


def _require_playwright() -> None:
    if sync_playwright is None:
        raise ModuleNotFoundError(
            "playwright is required to fetch live Binance AI Select data. "
            "Install it in the runtime that executes fetch_binance_ai_select.py."
        )


def _normalize_label(raw: Any) -> str | None:
    text = str(raw or "").strip().lower()
    if any(part in text for part in POSITIVE_LABEL_PARTS):
        return "Strong Positive"
    if any(part in text for part in NEGATIVE_LABEL_PARTS):
        return "Strong Negative"
    return None


def _normalize_tone_label(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    lowered = text.lower()
    if text in {"看涨", "看多"} or lowered in {"bullish", "positive"}:
        return "Bullish"
    if text in {"看跌", "看空"} or lowered in {"bearish", "negative"}:
        return "Bearish"
    if text in {"一般", "中性"} or lowered == "neutral":
        return "Neutral"
    return text


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
    *,
    include_metrics: bool = True,
    score_value: Any = None,
    fallback_score: bool = True,
    news_label: Any = None,
    social_label: Any = None,
    kol_label: Any = None,
) -> dict[str, Any]:
    item = {
        "rank": rank,
        "asset": asset,
        "baseAsset": asset,
        "assetType": asset_type,
    }
    if include_metrics:
        normalized_score = score_value
        if normalized_score in (None, "") and fallback_score:
            normalized_score = "7.0" if score_label == "Strong Positive" else "2.0"
        metrics = {
            "sentiment_score": {
                "valueLabel": score_label,
            }
        }
        if normalized_score not in (None, ""):
            metrics["sentiment_score"]["value"] = str(normalized_score)
        if news_label not in (None, ""):
            metrics["sentiment_score_news"] = {"valueLabel": str(news_label)}
        if social_label not in (None, ""):
            metrics["sentiment_score_social"] = {"valueLabel": str(social_label)}
        if kol_label not in (None, ""):
            metrics["sentiment_score_kol"] = {"valueLabel": str(kol_label)}
        item["metrics"] = metrics
    return item


def _merge_sentiment_items(existing: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in overlay.items():
        if value in (None, "", [], {}):
            continue
        if key == "metrics" and isinstance(value, dict):
            current_metrics = merged.get("metrics", {}) if isinstance(merged.get("metrics"), dict) else {}
            next_metrics = dict(current_metrics)
            for metric_key, metric_value in value.items():
                if metric_value in (None, "", [], {}):
                    continue
                if isinstance(metric_value, dict) and isinstance(next_metrics.get(metric_key), dict):
                    next_metrics[metric_key] = {
                        **next_metrics[metric_key],
                        **{k: v for k, v in metric_value.items() if v not in (None, "")},
                    }
                else:
                    next_metrics[metric_key] = metric_value
            merged["metrics"] = next_metrics
            continue
        merged[key] = value
    return merged


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
                        score_value=sentiment_score.get("value"),
                        news_label=metrics.get("sentiment_score_news", {}).get("valueLabel"),
                        social_label=metrics.get("sentiment_score_social", {}).get("valueLabel"),
                        kol_label=metrics.get("sentiment_score_kol", {}).get("valueLabel"),
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
        merged[asset] = _merge_sentiment_items(merged.get(asset, {}), normalized_item)

    for item in preferred:
        asset = _normalize_asset_code(item.get("asset"))
        if not _looks_like_asset_code(asset):
            continue
        normalized_item = dict(item)
        normalized_item["asset"] = asset
        normalized_item["baseAsset"] = asset
        if asset in merged:
            merged[asset] = _merge_sentiment_items(normalized_item, merged[asset])
        else:
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
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except PlaywrightTimeoutError:
        pass
    try:
        _wait_for_rendered_table(page)
    except PlaywrightTimeoutError:
        page.wait_for_timeout(5000)
        _wait_for_rendered_table(page)


def _scrape_visible_sentiment_rows(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        () => {
          const countMatches = (text, pattern) => {
            const matches = text.match(pattern);
            return matches ? matches.length : 0;
          };
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
            const signalCount =
              countMatches(text, /寮虹儓鐪嬪|瀵櫣鍎撻惇瀣樋|strong positive/ig)
              + countMatches(text, /寮虹儓鐪嬬┖|瀵櫣鍎撻惇瀣敄|strong negative/ig);
            if (signalCount !== 1) continue;

            let scoreLabel = null;
            if (hasPositive(text)) scoreLabel = 'Strong Positive';
            else if (hasNegative(text)) scoreLabel = 'Strong Negative';
            if (!scoreLabel) continue;

            const leadMatch = text.match(/^\\s*(?:(\\d{1,4})\\s+)?([A-Z][A-Z0-9]{1,19}|[\\u4e00-\\u9fff]{1,12})\\s+\\$/);
            if (!leadMatch) continue;
            const asset = leadMatch[2];
            if (!asset) continue;
            const scoreMatch = text.match(/([0-9]+(?:\\.[0-9]+)?)\\s+(?:寮虹儓鐪嬪|寮虹儓鐪嬬┖|瀵櫣鍎撻惇瀣樋|瀵櫣鍎撻惇瀣敄|strong positive|strong negative)/i);
            const newsMatch = text.match(/(?:鏂伴椈|新闻|News)\\s+([A-Za-z\\u4e00-\\u9fff]+)/i);
            const socialMatch = text.match(/(?:绀句氦鎯呯华|社交情绪|Social Sentiment)\\s+([A-Za-z\\u4e00-\\u9fff]+)/i);
            const kolMatch = text.match(/(?:KOL)\\s+([A-Za-z\\u4e00-\\u9fff]+)/i);

            const item = {
              asset,
              rank: leadMatch[1] ? Number(leadMatch[1]) : null,
              scoreLabel,
              assetType: /alpha/i.test(text) ? 'ALPHA' : 'SPOT',
              score: scoreMatch ? Number(scoreMatch[1]) : null,
              newsLabel: newsMatch ? newsMatch[1] : null,
              socialLabel: socialMatch ? socialMatch[1] : null,
              kolLabel: kolMatch ? kolMatch[1] : null,
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


def _fetch_alpha_token_map() -> dict[str, dict[str, Any]]:
    request = Request(ALPHA_AGGREGATE_URL, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    token_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        token_id = str(row.get("tokenId") or "").strip().upper()
        if token_id:
            token_map[token_id] = row
    return token_map


def _wait_for_rendered_table(page, previous_first_rank: int | None = None) -> None:
    page.wait_for_function(
        """
        (previousFirstRank) => {
          const rows = Array.from(document.querySelectorAll('table tr'))
            .filter((tr) => tr.querySelectorAll('td').length >= 8);
          if (!rows.length) return false;
          const firstRowCells = Array.from(rows[0].querySelectorAll('td'));
          const firstRank = Number((firstRowCells[1]?.innerText || '').trim());
          const labelText = (firstRowCells[2]?.innerText || '').trim();
          const hasSignalLabel = /强烈看多|强烈看空|看多|看空|strong positive|strong negative/i.test(labelText);
          if (!Number.isFinite(firstRank) || !hasSignalLabel) return false;
          if (previousFirstRank == null) return true;
          return firstRank !== previousFirstRank;
        }
        """,
        arg=previous_first_rank,
        timeout=20000,
    )


def _current_first_rank(page) -> int | None:
    value = page.evaluate(
        """
        () => {
          const row = Array.from(document.querySelectorAll('table tr'))
            .find((tr) => tr.querySelectorAll('td').length >= 8);
          if (!row) return null;
          const rank = Number((row.querySelectorAll('td')[1]?.innerText || '').trim());
          return Number.isFinite(rank) ? rank : null;
        }
        """
    )
    return int(value) if value is not None else None


def _go_to_rendered_page(page, target_page: int) -> bool:
    previous_first_rank = _current_first_rank(page)
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
        try:
            _wait_for_rendered_table(page, previous_first_rank)
        except PlaywrightTimeoutError:
            page.wait_for_timeout(3000)
    return bool(clicked)


def _extract_rendered_table_rows(page) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        () => {
          const tokenPattern = /token\\/logos\\/([A-Za-z0-9]+)\\.png/i;
          return Array.from(document.querySelectorAll('table tr'))
            .filter((tr) => tr.querySelectorAll('td').length >= 8)
            .map((tr) => {
              const cells = Array.from(tr.querySelectorAll('td'))
                .map((cell) => (cell.innerText || '').replace(/\\s+/g, ' ').trim());
              const assetHtml = tr.querySelector('td')?.innerHTML || '';
              const tokenMatch = assetHtml.match(tokenPattern);
              return {
                assetText: cells[0] || '',
                rank: Number(cells[1] || ''),
                scoreLabel: cells[2] || '',
                priceText: cells[3] || '',
                socialVolumeText: cells[4] || '',
                socialLabel: cells[5] || '',
                newsLabel: cells[6] || '',
                kolLabel: cells[7] || '',
                tokenId: tokenMatch ? tokenMatch[1].toUpperCase() : null,
                rowText: (tr.innerText || '').replace(/\\s+/g, ' ').trim(),
              };
            })
            .filter((row) => Number.isFinite(row.rank) && row.assetText && row.scoreLabel);
        }
        """
    )


def _split_rendered_asset_text(raw: Any) -> tuple[str, str]:
    text = str(raw or "").strip()
    if text.endswith(" Alpha"):
        return text[:-6].strip(), "ALPHA"
    return text, "SPOT"


def _resolve_rendered_asset_code(
    display_asset: str,
    asset_type: str,
    token_id: str | None,
    alpha_token_map: dict[str, dict[str, Any]],
) -> str:
    if asset_type != "ALPHA":
        return _normalize_asset_code(display_asset)
    alpha_meta = alpha_token_map.get(str(token_id or "").upper(), {})
    candidates = [
        display_asset,
        alpha_meta.get("cexCoinName"),
        alpha_meta.get("symbol"),
        alpha_meta.get("name"),
    ]
    for candidate in candidates:
        normalized = _normalize_asset_code(candidate)
        if _looks_like_asset_code(normalized):
            return normalized
    return display_asset


def _fetch_assets_payload_from_page(page, interval: str) -> dict[str, Any]:
    return page.evaluate(
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
        build_api_path("assets", interval),
    )


def _build_rendered_score_lookup(payload: dict[str, Any]) -> dict[str, Any]:
    items = payload.get("json", {}).get("data", {}).get("items", [])
    lookup: dict[str, Any] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        metrics = item.get("metrics", {}) if isinstance(item.get("metrics"), dict) else {}
        score = metrics.get("sentiment_score", {}) if isinstance(metrics.get("sentiment_score"), dict) else {}
        value = score.get("value")
        if value in (None, ""):
            continue
        rank = item.get("rank")
        asset = _normalize_asset_code(item.get("baseAsset") or item.get("asset"))
        if rank not in (None, ""):
            lookup[f"rank:{int(rank)}"] = value
            if asset:
                lookup[f"rank_asset:{int(rank)}:{asset}"] = value
        if asset:
            lookup[f"asset:{asset}"] = value
    return lookup


def _lookup_rendered_score(
    *,
    rank: int,
    resolved_asset: str,
    display_asset: str,
    score_lookup: dict[str, Any],
) -> Any:
    resolved = _normalize_asset_code(resolved_asset)
    display = _normalize_asset_code(display_asset)
    for key in (
        f"rank_asset:{rank}:{resolved}",
        f"rank_asset:{rank}:{display}",
        f"rank:{rank}",
        f"asset:{resolved}",
        f"asset:{display}",
    ):
        value = score_lookup.get(key)
        if value not in (None, ""):
            return value
    return None


def _build_item_from_rendered_row(
    row: dict[str, Any],
    alpha_token_map: dict[str, dict[str, Any]],
    score_lookup: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    rank = row.get("rank")
    if rank in (None, ""):
        return None
    rank_int = int(rank)
    score_label = _normalize_label(row.get("scoreLabel"))
    if not score_label:
        return None
    display_asset, asset_type = _split_rendered_asset_text(row.get("assetText"))
    resolved_asset = _resolve_rendered_asset_code(
        display_asset=display_asset,
        asset_type=asset_type,
        token_id=row.get("tokenId"),
        alpha_token_map=alpha_token_map,
    )
    score_value = _lookup_rendered_score(
        rank=rank_int,
        resolved_asset=resolved_asset,
        display_asset=display_asset,
        score_lookup=score_lookup or {},
    )
    item = _build_sentiment_item(
        asset=resolved_asset,
        rank=rank_int,
        score_label=score_label,
        asset_type=asset_type,
        score_value=score_value,
        fallback_score=False,
        news_label=_normalize_tone_label(row.get("newsLabel")),
        social_label=_normalize_tone_label(row.get("socialLabel")),
        kol_label=_normalize_tone_label(row.get("kolLabel")),
    )
    item["sourceRank"] = rank_int
    item["displayAsset"] = display_asset
    if row.get("tokenId"):
        item["tokenId"] = str(row.get("tokenId")).upper()
    return item


def fetch_rendered_signal_lists(interval: str, debug_targets: list[str] | None = None) -> dict[str, Any]:
    _require_playwright()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            locale="zh-CN",
            viewport={"width": 1920, "height": 5000},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        _open_page(page)

        debug_targets_upper = {
            target.strip().upper() for target in (debug_targets or []) if target.strip()
        }
        alpha_token_map = _fetch_alpha_token_map()
        assets_payload = _fetch_assets_payload_from_page(page, interval)
        score_lookup = _build_rendered_score_lookup(assets_payload)
        last_page = _find_last_page_number(page)
        scanned_pages: list[int] = []
        rendered_rows: list[dict[str, Any]] = []

        def collect_current_page(page_number: int) -> list[dict[str, Any]]:
            rows = _extract_rendered_table_rows(page)
            for row in rows:
                row["page"] = page_number
            scanned_pages.append(page_number)
            rendered_rows.extend(rows)
            return rows

        # Strong longs are at the front of the rank table. Stop after the first
        # front page that no longer contains strong-long rows, with a small cap
        # so a bad pagination state cannot make a trading cycle crawl forever.
        current_page_number = 1
        for page_number in range(1, min(last_page, 6) + 1):
            if page_number != current_page_number:
                if not _go_to_rendered_page(page, page_number):
                    continue
                current_page_number = page_number
            rows = collect_current_page(page_number)
            if page_number > 1 and not any(
                _normalize_label(row.get("scoreLabel")) == "Strong Positive" for row in rows
            ):
                break

        # Strong shorts are at the tail of the rank table. Scan backward from
        # the last page and stop once the current tail edge has been covered.
        tail_start = max(1, last_page - 5)
        for page_number in range(last_page, tail_start - 1, -1):
            if page_number != current_page_number:
                if not _go_to_rendered_page(page, page_number):
                    continue
                current_page_number = page_number
            rows = collect_current_page(page_number)
            if page_number < last_page and not any(
                _normalize_label(row.get("scoreLabel")) == "Strong Negative" for row in rows
            ):
                break

        positive_by_key: dict[str, dict[str, Any]] = {}
        negative_by_key: dict[str, dict[str, Any]] = {}
        row_matches: list[dict[str, Any]] = []
        for row in rendered_rows:
            item = _build_item_from_rendered_row(row, alpha_token_map, score_lookup)
            if not item:
                continue
            key = f"{item.get('sourceRank') or item.get('rank')}|{item.get('displayAsset') or item.get('asset')}"
            label = item.get("metrics", {}).get("sentiment_score", {}).get("valueLabel")
            if label == "Strong Positive":
                positive_by_key[key] = item
            elif label == "Strong Negative":
                negative_by_key[key] = item
            if debug_targets_upper:
                text = f"{row.get('assetText', '')} {row.get('rowText', '')}".upper()
                if any(target in text for target in debug_targets_upper):
                    row_matches.append(row)

        positive_items = sorted(
            positive_by_key.values(),
            key=lambda item: (item.get("sourceRank") or item.get("rank") or 999999),
        )
        negative_items = sorted(
            negative_by_key.values(),
            key=lambda item: -(item.get("sourceRank") or item.get("rank") or 0),
        )

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
                    "source": "rendered_table",
                    "lastPage": last_page,
                    "scannedPages": scanned_pages,
                    "scannedRowCount": len(rendered_rows),
                    "alphaTokenMapCount": len(alpha_token_map),
                    "scoreLookupCount": len(score_lookup),
                    "debugTargets": sorted(debug_targets_upper),
                    "rowMatches": row_matches[:100],
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
    _require_playwright()
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
