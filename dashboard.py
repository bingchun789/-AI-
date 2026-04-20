import argparse
import json
import os
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from view_status import build_report


CONFIG_TOGGLE_KEYS = {
    "DRY_RUN",
    "ENABLE_MIN_SIGNAL_COUNT_FILTER",
    "ENABLE_MARGIN_USAGE_CAP",
    "ENABLE_VOLATILITY_FILTER",
    "ENABLE_FUNDING_RATE_FILTER",
    "ENABLE_CORRELATION_FILTER",
    "ENABLE_TREND_CONFIRMATION",
    "ENABLE_TIME_EXIT",
    "ENABLE_PROFIT_LOCK",
    "ENABLE_PROFIT_PROTECTION",
    "ENABLE_SIGNAL_DROP_GUARD",
    "SKIP_IF_MARGIN_MODE_UNAVAILABLE",
}


HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>交易策略看板</title>
  <style>
    :root {
      --bg: #f3ede4;
      --bg-strong: #ebe2d3;
      --card: rgba(255,255,255,.78);
      --card-strong: rgba(255,251,245,.94);
      --text: #182028;
      --muted: #5d6873;
      --line: rgba(24,32,40,.08);
      --line-strong: rgba(24,32,40,.14);
      --accent: #b8683d;
      --accent-soft: rgba(184,104,61,.12);
      --teal: #176b6a;
      --teal-soft: rgba(23,107,106,.12);
      --good: #0c7a43;
      --bad: #b42318;
      --shadow: 0 18px 40px rgba(87,64,38,.12);
      --shadow-soft: 0 10px 24px rgba(87,64,38,.08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Avenir Next","PingFang SC","Microsoft YaHei",sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(183,93,42,.18), transparent 34%),
        radial-gradient(circle at top right, rgba(27,127,121,.16), transparent 28%),
        linear-gradient(180deg, #faf7f1 0%, var(--bg) 100%);
    }
    .wrap {
      max-width: 1360px;
      margin: 0 auto;
      padding: 20px 24px 36px;
    }
    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.55fr) minmax(280px, .72fr);
      align-items: start;
      gap: 14px;
      margin-bottom: 14px;
    }
    .hero-main,
    .hero-side {
      background: linear-gradient(135deg, rgba(255,255,255,.84), rgba(255,248,239,.72));
      border: 1px solid rgba(255,255,255,.76);
      border-radius: 24px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
    }
    .hero-main {
      padding: 18px 22px;
      align-self: start;
    }
    .hero-side {
      padding: 16px 18px;
      align-self: start;
      display: flex;
      flex-direction: column;
      justify-content: flex-start;
      gap: 12px;
    }
    .hero-side-stats {
      display: grid;
      gap: 10px;
    }
    .hero-stat {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,.72);
      border: 1px solid var(--line);
      box-shadow: var(--shadow-soft);
    }
    .hero-stat-label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .04em;
      text-transform: uppercase;
    }
    .hero-stat-value {
      font-size: 16px;
      font-weight: 700;
      color: var(--text);
      text-align: right;
    }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 11px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .08em;
      text-transform: uppercase;
    }
    .hero h1 {
      margin: 12px 0 8px;
      font-size: 28px;
      line-height: 1.12;
    }
    .hero p {
      margin: 0;
      color: var(--muted);
      line-height: 1.6;
      font-size: 14px;
      max-width: 700px;
    }
    .hero-tags {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 12px;
    }
    .soft-pill {
      display: inline-flex;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(255,255,255,.76);
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 12px;
      font-weight: 600;
    }
    .stamp {
      padding: 10px 14px;
      border-radius: 18px;
      background: rgba(255,255,255,.82);
      border: 1px solid var(--line);
      box-shadow: var(--shadow-soft);
      color: var(--muted);
      min-width: 220px;
      text-align: left;
      font-weight: 600;
      font-size: 13px;
    }
    .hero-side-copy {
      display: grid;
      gap: 8px;
    }
    .hero-side-title {
      font-size: 12px;
      font-weight: 700;
      letter-spacing: .04em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .hero-side-note {
      margin: 0;
      color: var(--muted);
      line-height: 1.55;
      font-size: 13px;
    }
    .warn-banner {
      display: none;
      margin-bottom: 18px;
      padding: 14px 16px;
      border-radius: 18px;
      background: rgba(180,35,24,.08);
      border: 1px solid rgba(180,35,24,.18);
      color: var(--bad);
      font-size: 14px;
      box-shadow: var(--shadow-soft);
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px,1fr));
      gap: 14px;
      margin-bottom: 18px;
    }
    .triple,
    .highlights {
      display: grid;
      grid-template-columns: repeat(3,1fr);
      gap: 14px;
      margin-bottom: 18px;
    }
    .panels {
      display: grid;
      grid-template-columns: minmax(0, 1.55fr) minmax(320px, 1fr);
      align-items: start;
      gap: 14px;
    }
    .card {
      background: var(--card);
      border: 1px solid rgba(255,255,255,.7);
      border-radius: 24px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .metric-card {
      background: linear-gradient(180deg, rgba(255,255,255,.86), rgba(255,251,245,.72));
    }
    .label {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 10px;
      letter-spacing: .04em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .value {
      font-size: 30px;
      font-weight: 700;
      line-height: 1.12;
    }
    .sub {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.55;
    }
    .tab-shell {
      display: grid;
      gap: 18px;
    }
    .tab-bar {
      position: sticky;
      top: 10px;
      z-index: 20;
      display: flex;
      gap: 10px;
      padding: 10px;
      border-radius: 24px;
      border: 1px solid rgba(255,255,255,.82);
      background: rgba(249,245,238,.78);
      backdrop-filter: blur(12px);
      box-shadow: var(--shadow-soft);
      overflow-x: auto;
    }
    .tab-btn {
      border: 0;
      min-width: 132px;
      border-radius: 18px;
      padding: 12px 16px;
      background: transparent;
      color: var(--muted);
      cursor: pointer;
      display: grid;
      gap: 3px;
      text-align: left;
      transition: .2s ease;
    }
    .tab-btn:hover {
      background: rgba(255,255,255,.7);
      color: var(--text);
    }
    .tab-btn.active {
      background: linear-gradient(135deg, #182028, #314354);
      color: #fff;
      box-shadow: 0 12px 24px rgba(24,32,40,.18);
    }
    .tab-btn strong {
      font-size: 15px;
      line-height: 1.2;
    }
    .tab-btn small {
      font-size: 12px;
      opacity: .78;
      line-height: 1.3;
    }
    .tab-panel { display: none; }
    .tab-panel.active {
      display: grid;
      gap: 18px;
      animation: fadeIn .24s ease;
    }
    .dashboard-grid {
      display: grid;
      grid-template-columns: repeat(12, minmax(0, 1fr));
      gap: 14px;
      align-items: start;
    }
    .col-2 { grid-column: span 2; }
    .col-3 { grid-column: span 3; }
    .col-4 { grid-column: span 4; }
    .col-5 { grid-column: span 5; }
    .col-6 { grid-column: span 6; }
    .col-7 { grid-column: span 7; }
    .col-8 { grid-column: span 8; }
    .col-12 { grid-column: span 12; }
    .card-equal { min-height: 188px; }
    .card-tall { min-height: 238px; }
    .card-table { min-height: 0; }
    .section-stack {
      display: grid;
      gap: 18px;
    }
    .tip-card {
      background: linear-gradient(180deg, rgba(255,255,255,.86), rgba(245,238,229,.78));
    }
    .tip-list {
      display: grid;
      gap: 10px;
    }
    .tip-row {
      padding: 12px 14px;
      border-radius: 16px;
      background: rgba(255,255,255,.65);
      border: 1px solid var(--line);
    }
    .tip-row strong {
      display: block;
      margin-bottom: 4px;
      font-size: 14px;
    }
    .tab-empty-note {
      font-size: 14px;
      color: var(--muted);
      line-height: 1.7;
    }
    .mini-list { display: grid; gap: 10px; }
    .strategy-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .strategy-card {
      min-height: 100%;
    }
    .mini-item {
      padding: 12px;
      border-radius: 14px;
      background: rgba(255,255,255,.64);
      border: 1px solid var(--line);
    }
    .mini-item.long-card { border-left: 4px solid var(--good); }
    .mini-item.short-card { border-left: 4px solid var(--bad); }
    .mini-item.ok-card { border-left: 4px solid var(--good); }
    .mini-item.warn-card { border-left: 4px solid var(--bad); }
    .mini-title { font-weight: 700; margin-bottom: 4px; }
    .mini-title.long-text, .side-text.long-text { color: var(--good); }
    .mini-title.short-text, .side-text.short-text { color: var(--bad); }
    .mini-title.ok-text { color: var(--good); }
    .mini-title.warn-text { color: var(--bad); }
    .side-text { font-weight: 700; }
    .metric-primary {
      min-height: 162px;
      padding: 18px 18px 16px;
    }
    .metric-primary .value {
      font-size: 28px;
    }
    .overview-block {
      min-height: 210px;
    }
    .overview-highlight {
      min-height: 188px;
    }
    .overview-lines {
      display: grid;
      gap: 8px;
      margin-top: 12px;
    }
    .overview-line {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding-top: 8px;
      border-top: 1px solid var(--line);
      font-size: 13px;
    }
    .overview-line span {
      color: var(--muted);
    }
    .overview-line strong {
      font-size: 14px;
      font-weight: 700;
      text-align: right;
    }
    .inline-note {
      margin-bottom: 14px;
      padding: 10px 12px;
      border-radius: 12px;
      background: rgba(255,255,255,.64);
      border: 1px solid var(--line);
      color: var(--muted);
      font-size: 13px;
    }
    .rule-row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
    }
    .rule-name { color: var(--muted); min-width: 92px; }
    .rule-value { text-align: right; font-weight: 600; }
    .table-wrap { overflow-x: auto; }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td {
      text-align: left;
      padding: 12px 10px;
      border-bottom: 1px solid var(--line);
      white-space: nowrap;
    }
    th { color: var(--muted); font-weight: 600; }
    .good { color: var(--good); font-weight: 700; }
    .bad { color: var(--bad); font-weight: 700; }
    .pill {
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(183,93,42,.10);
      font-size: 12px;
      font-weight: 700;
      color: #b75d2a;
    }
    .pill.long { background: rgba(12,122,67,.12); color: var(--good); }
    .pill.short { background: rgba(180,35,24,.12); color: var(--bad); }
    .filter-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 10px;
    }
    .action-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }
    .action-btn {
      border: 0;
      border-radius: 999px;
      padding: 10px 16px;
      background: linear-gradient(135deg, #182028, #314354);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
      box-shadow: 0 10px 20px rgba(24,32,40,.16);
      transition: .2s ease;
    }
    .action-btn:hover:not(:disabled) {
      transform: translateY(-1px);
    }
    .filter-btn {
      background: rgba(255,255,255,.82);
      color: var(--muted);
      border: 1px solid var(--line);
      box-shadow: none;
    }
    .filter-btn.active {
      background: linear-gradient(135deg, #182028, #314354);
      color: #fff;
      border-color: transparent;
      box-shadow: 0 10px 20px rgba(24,32,40,.16);
    }
    .secondary-btn {
      border: 1px solid var(--line-strong);
      background: rgba(255,255,255,.85);
      color: var(--text);
      border-radius: 999px;
      padding: 8px 18px;
      cursor: pointer;
      font-weight: 700;
      transition: .2s ease;
    }
    .secondary-btn:hover {
      background: rgba(255,255,255,.96);
    }
    .action-btn:disabled {
      opacity: .55;
      cursor: not-allowed;
    }
    .toggle-list { display: grid; gap: 10px; }
    .toggle-item {
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: center;
      padding: 12px;
      border-radius: 14px;
      background: rgba(255,255,255,.64);
      border: 1px solid var(--line);
    }
    .toggle-copy { flex: 1; min-width: 0; }
    .toggle-name { font-weight: 700; margin-bottom: 4px; }
    .toggle-detail { color: var(--muted); font-size: 13px; line-height: 1.5; }
    .switch {
      position: relative;
      width: 52px;
      height: 30px;
      flex: 0 0 auto;
    }
    .switch input {
      opacity: 0;
      width: 0;
      height: 0;
    }
    .slider {
      position: absolute;
      inset: 0;
      border-radius: 999px;
      background: rgba(24,32,40,.16);
      transition: .2s ease;
      cursor: pointer;
    }
    .slider:before {
      content: "";
      position: absolute;
      width: 24px;
      height: 24px;
      left: 3px;
      top: 3px;
      border-radius: 50%;
      background: #fff;
      box-shadow: 0 4px 10px rgba(24,32,40,.18);
      transition: .2s ease;
    }
    .switch input:checked + .slider {
      background: rgba(12,122,67,.78);
    }
    .switch input:checked + .slider:before {
      transform: translateX(22px);
    }
    .empty { color: var(--muted); padding: 24px 0; }
    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(6px); }
      to { opacity: 1; transform: translateY(0); }
    }
    @media (max-width: 980px) {
      .grid, .triple, .highlights, .panels { grid-template-columns: 1fr; }
      .hero { grid-template-columns: 1fr; }
      .dashboard-grid { grid-template-columns: 1fr; }
      .col-2, .col-3, .col-4, .col-5, .col-6, .col-7, .col-8, .col-12 { grid-column: span 1; }
      .strategy-grid { grid-template-columns: 1fr; }
      .hero-main, .hero-side { padding: 16px; }
      .hero h1 { font-size: 24px; }
      .stamp { width: 100%; }
      .tab-bar { top: 8px; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="tab-shell">
      <div class="tab-bar" role="tablist" aria-label="页面内容标签">
        <button class="tab-btn active" type="button" data-tab="overview" role="tab" aria-selected="true">
          <strong>总览</strong>
          <small>顶部概览卡片</small>
        </button>
        <button class="tab-btn" type="button" data-tab="positions" role="tab" aria-selected="false">
          <strong>持仓</strong>
          <small>仓位明细</small>
        </button>
        <button class="tab-btn" type="button" data-tab="strategy" role="tab" aria-selected="false">
          <strong>策略</strong>
          <small>状态、冷却与准入</small>
        </button>
        <button class="tab-btn" type="button" data-tab="records" role="tab" aria-selected="false">
          <strong>记录</strong>
          <small>平仓与强平</small>
        </button>
        <button class="tab-btn" type="button" data-tab="recovery" role="tab" aria-selected="false">
          <strong>回收</strong>
          <small>最大浮亏与回正</small>
        </button>
        <button class="tab-btn" type="button" data-tab="monitor" role="tab" aria-selected="false">
          <strong>巡检</strong>
          <small>系统巡检</small>
        </button>
        <button class="tab-btn" type="button" data-tab="risk" role="tab" aria-selected="false">
          <strong>风控</strong>
          <small>回撤与风险指标</small>
        </button>
        <button class="tab-btn" type="button" data-tab="settings" role="tab" aria-selected="false">
          <strong>设置</strong>
          <small>规则与开关</small>
        </button>
      </div>

      <section class="tab-panel active" data-panel="overview" role="tabpanel">
        <div class="dashboard-grid">
          <div class="hero-main col-7">
            <span class="eyebrow">Trading Dashboard</span>
            <h1>交易策略看板</h1>
            <p>总览页只保留最值得先看的信息。先看账户盈亏和仓位规模，再看做多做空分布，最后看当前最强和最弱仓位。</p>
            <div class="hero-tags">
              <span class="soft-pill">币安模拟盘</span>
              <span class="soft-pill">自动刷新</span>
              <span class="soft-pill">重点卡片优先</span>
            </div>
          </div>
          <div class="hero-side col-5">
            <div class="stamp" id="stamp">加载中...</div>
            <div class="hero-side-copy">
              <div class="hero-side-title">快速判断</div>
              <p class="hero-side-note">这一屏只做快速扫一眼。仓位明细去持仓页，策略判断去策略页，记录和回撤也都拆开了，不再混在总览里。</p>
            </div>
            <div class="hero-side-stats">
              <div class="hero-stat">
                <span class="hero-stat-label">数据来源</span>
                <span class="hero-stat-value" id="source">-</span>
              </div>
              <div class="hero-stat">
                <span class="hero-stat-label">当前总持仓</span>
                <span class="hero-stat-value" id="openPositions">0</span>
              </div>
            </div>
          </div>

          <div class="warn-banner col-12" id="warnBanner"></div>

          <div class="card metric-card metric-primary col-3"><div class="label">当前总浮盈亏</div><div class="value" id="totalPnl">-</div><div class="sub">币安返回的未实现盈亏（USDT）</div></div>
          <div class="card metric-card metric-primary col-3"><div class="label">当前总持仓价值</div><div class="value" id="totalValue">-</div><div class="sub">按标记价格合计（USDT）</div></div>
          <div class="card metric-card metric-primary col-3"><div class="label">可用余额 / 钱包余额</div><div class="value" id="wallet">-</div><div class="sub">币安模拟盘账户余额</div></div>
          <div class="card metric-card metric-primary col-3"><div class="label">累计已实现盈亏</div><div class="value" id="realizedPnl">-</div><div class="sub">做多和做空合计（USDT）</div></div>

          <div class="card overview-block col-4"><div class="label">做多板块</div><div id="longSummary"></div></div>
          <div class="card overview-block col-4"><div class="label">做空板块</div><div id="shortSummary"></div></div>
          <div class="card overview-block col-4"><div class="label">总体统计</div><div id="overallSummary"></div></div>

          <div class="card overview-block overview-highlight col-6"><div class="label">当前最佳仓位</div><div id="bestWrap" class="empty">当前还没有可比较的仓位。</div></div>
          <div class="card overview-block overview-highlight col-6"><div class="label">当前最弱仓位</div><div id="worstWrap" class="empty">当前还没有可比较的仓位。</div></div>
        </div>
      </section>

      <section class="tab-panel" data-panel="positions" role="tabpanel">
        <div class="card">
          <div class="label">持仓明细</div>
          <div id="leverageWrap" class="inline-note">当前持仓规则加载中...</div>
          <div id="positionsWrap"></div>
        </div>
      </section>

      <section class="tab-panel" data-panel="strategy" role="tabpanel">
        <div class="dashboard-grid">
          <div class="card card-table col-12">
            <div class="label">策略状态</div>
            <div class="strategy-grid" id="strategiesWrap"></div>
          </div>

          <div class="card card-table col-12">
            <div class="label">本轮未开仓</div>
            <div id="unopenedWrap"></div>
          </div>

          <div class="card card-table col-12">
            <div class="action-row">
              <div>
                <div class="label">当前冷却中</div>
                <div class="sub" id="cooldownMeta">正在加载冷却状态...</div>
              </div>
              <button class="action-btn" id="resetCooldownBtn" onclick="resetCooldowns()">一键重置冷却</button>
            </div>
            <div id="cooldownWrap"></div>
          </div>
        </div>
      </section>

      <section class="tab-panel" data-panel="records" role="tabpanel">
        <div class="section-stack">
          <div class="card">
            <div class="label">平仓记录（官方 API）</div>
            <div class="sub" style="margin-bottom:8px">来自币安官方接口，最近 30 天已平仓记录</div>
            <div class="filter-row">
              <button class="action-btn filter-btn active" onclick="filterTradeHistory('all')" id="thFilterAll" type="button">全部</button>
              <button class="action-btn filter-btn" onclick="filterTradeHistory('long')" id="thFilterLong" type="button">做多</button>
              <button class="action-btn filter-btn" onclick="filterTradeHistory('short')" id="thFilterShort" type="button">做空</button>
            </div>
            <div id="tradeHistoryWrap"></div>
          </div>

          <div class="panels">
            <div class="card"><div class="label">爆仓 / 强平记录（官方 API）</div><div class="sub" id="forceOrderMeta">来自币安 forceOrders 接口</div><div id="forceOrderWrap"></div></div>
          </div>
        </div>
      </section>

      <section class="tab-panel" data-panel="recovery" role="tabpanel">
        <div class="section-stack">
          <div class="card">
            <div class="label">深跌后回收统计</div>
            <div class="sub" id="recoveryMeta">统计已经记录到最大浮亏轨迹的历史平仓单。</div>
            <div id="recoveryWrap"></div>
          </div>
        </div>
      </section>

      <section class="tab-panel" data-panel="monitor" role="tabpanel">
        <div class="section-stack">
          <div class="card">
            <div class="label">系统巡检</div>
            <div class="sub" id="monitorMeta">巡检会检查规则偏差、冷却违规、持仓不一致等问题。</div>
            <div id="monitorWrap"></div>
          </div>
        </div>
      </section>

      <section class="tab-panel" data-panel="risk" role="tabpanel">
        <div class="panels">
          <div class="card"><div class="label">风险统计</div><div class="mini-list" id="riskStatsWrap"></div></div>
          <div class="card tip-card">
            <div class="label">口径说明</div>
            <div class="mini-list">
              <div class="mini-item">
                <div class="mini-title">账户最大回撤</div>
                <div>按账户总权益从高点回落的最大幅度计算，用来看整体账户曾经最多回撤了多少。</div>
              </div>
              <div class="mini-item">
                <div class="mini-title">策略净值最大回撤</div>
                <div>按已平仓收益曲线计算，用来看策略历史上最难熬的一段回撤。</div>
              </div>
              <div class="mini-item">
                <div class="mini-title">盈亏比 / 利润因子</div>
                <div>盈亏比看平均每笔盈利和亏损的比例，利润因子看总盈利和总亏损的比例。</div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section class="tab-panel" data-panel="settings" role="tabpanel">
        <div class="dashboard-grid">
          <div class="card card-table col-6"><div class="label">基本规则</div><div class="mini-list" id="ruleSummaryWrap"></div></div>
          <div class="card card-table col-6">
            <div class="action-row">
              <div>
                <div class="label">策略开关</div>
                <div class="sub" id="configToggleMeta">保存后机器人下个轮询周期自动生效。</div>
              </div>
              <button class="action-btn" id="saveToggleBtn" onclick="saveConfigToggles()">保存开关</button>
            </div>
            <div id="configToggleWrap"></div>
          </div>

          <div class="card tip-card card-equal col-6">
            <div class="label">操作提醒</div>
            <div class="tip-list">
              <div class="tip-row">
                <strong>修改开关后</strong>
                <div class="tab-empty-note">页面会立即刷新，但策略实际生效要等机器人下一个轮询周期。</div>
              </div>
              <div class="tip-row">
                <strong>刷新较慢时</strong>
                <div class="tab-empty-note">看板会先显示最近一次成功缓存，不会因为单次接口慢就整页卡住。</div>
              </div>
            </div>
          </div>
          <div class="card tip-card card-equal col-6">
            <div class="label">实盘建议</div>
            <div class="mini-list">
              <div class="mini-item">
                <div class="mini-title">怎么看</div>
                <div>准入清单全部满足后，再考虑极小仓位接入实盘。优先看持仓是否对账、平仓是否确认、样本量是否足够。</div>
              </div>
              <div class="mini-item">
                <div class="mini-title">建议顺序</div>
                <div>先继续跑模拟盘，再小仓位实盘，最后再逐步放大，不要直接满额切过去。</div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  </div>

  <script>
    function fmt(value, digits = 4) {
      if (value === null || value === undefined || value === '') return '-';
      const num = Number(value);
      if (Number.isNaN(num)) return String(value);
      return num.toFixed(digits);
    }

    const VALID_TABS = ['overview', 'positions', 'strategy', 'records', 'recovery', 'monitor', 'risk', 'settings'];

    function setActiveTab(tabName, syncHash = true) {
      const target = VALID_TABS.includes(tabName) ? tabName : 'overview';
      document.querySelectorAll('.tab-btn').forEach(btn => {
        const active = btn.dataset.tab === target;
        btn.classList.toggle('active', active);
        btn.setAttribute('aria-selected', active ? 'true' : 'false');
      });
      document.querySelectorAll('.tab-panel').forEach(panel => {
        panel.classList.toggle('active', panel.dataset.panel === target);
      });
      if (syncHash) {
        history.replaceState(null, '', `${window.location.pathname}${window.location.search}#${target}`);
      }
    }

    function initTabs() {
      document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => setActiveTab(btn.dataset.tab));
      });
      const initialTab = VALID_TABS.includes(window.location.hash.slice(1))
        ? window.location.hash.slice(1)
        : 'overview';
      setActiveTab(initialTab, false);
      window.addEventListener('hashchange', () => {
        const hashTab = window.location.hash.slice(1);
        if (VALID_TABS.includes(hashTab)) {
          setActiveTab(hashTab, false);
        }
      });
    }

    function clsByPnl(value) {
      const num = Number(value);
      if (Number.isNaN(num)) return '';
      if (num > 0) return 'good';
      if (num < 0) return 'bad';
      return '';
    }

    function sideLabel(side) {
      return side === 'SHORT' ? '做空' : '做多';
    }

    function sideClass(side) {
      return side === 'SHORT' ? 'short' : 'long';
    }

    function sidePill(side) {
      return `<span class="pill ${sideClass(side)}">${sideLabel(side)}</span>`;
    }

    function sideLeveragePill(side, leverage) {
      const lv = leverage ? `${leverage}X` : '-';
      return `<span class="pill ${sideClass(side)}">${sideLabel(side)} ${lv}</span>`;
    }

    function translateSource(value) {
      if (value === 'local_state') return '本地模拟';
      if (value === 'binance_testnet') return '币安模拟盘';
      return value || '-';
    }

    function translateStatus(value) {
      const map = {
        LIVE_TESTNET: '币安模拟盘持仓',
        FILLED: '已成交',
        TESTNET_ACCEPTED: '测试下单通过',
        TESTNET_CLOSE_ACCEPTED: '测试平仓通过',
        DRY_RUN_ACCEPTED: '本地模拟持仓',
        NO_POSITION: '无持仓',
        POSITION_MISSING: '交易所侧持仓已消失',
        ok: '正常',
        disabled: '已停用',
        liquidation: '强平',
        adl: '自动减仓',
        force_order: '交易所强制平仓',
        exchange_trade: '交易所成交'
      };
      return map[value] || value || '-';
    }

    function translateReason(value) {
      const map = {
        signal_lost: '掉出当前列表',
        snapshot_protection: '快照保护（上轮仍在列表，本轮抓取可能遗漏）',
        signal_drop_guard: '信号骤降保护',
        profit_lock: '分级锁盈平仓',
        profit_retrace: '盈利回撤保护平仓',
        time_exit: '持仓时间过长平仓',
        exchange_position_missing: '交易所侧持仓已消失',
        still_strong_positive: '仍在强烈看多列表',
        still_strong_negative: '仍在强烈看空列表',
        cooldown: '冷却中',
        signal_count_too_low: '强信号数量不足',
        margin_usage_limit: '保证金使用率过高',
        side_limit: '达到该方向持仓上限',
        portfolio_limit: '达到总持仓上限',
        cycle_limit: '达到本轮开仓上限',
        no_usdt_perpetual: '没有可用 USDT 永续合约',
        low_24h_quote_volume: '24 小时成交额过低',
        high_volatility: '波动过大',
        funding_too_high: '资金费率过高',
        trend_not_confirmed: '趋势未确认',
        trend_data_unavailable: '趋势数据不足',
        correlated_with_existing: '与已有持仓走势高度相似',
        liquidation: '强平',
        adl: '自动减仓',
        force_order: '交易所强制平仓',
        exchange_trade: '交易所成交'
      };
      return map[value] || value || '-';
    }

    function translateCloseConfirm(value) {
      if (value === true) return '已确认';
      if (value === false) return '待确认';
      return '-';
    }

    function fmtCloseTime(value) {
      if (value === null || value === undefined || value === '') return '-';
      const num = Number(value);
      if (!Number.isNaN(num) && num > 1000000000) {
        return new Date(num).toLocaleString();
      }
      return String(value);
    }

    function translateStrategyName(id, name) {
      if (id === 'ai_select_futures_long') return 'AI 精选做多';
      if (id === 'ai_select_futures_short') return 'AI 精选做空';
      return name || id || '-';
    }

    function renderBlockSummary(targetId, item) {
      const el = document.getElementById(targetId);
      el.innerHTML = `
        <div class="value ${clsByPnl(item.unrealizedProfit)}">${fmt(item.unrealizedProfit, 4)} USDT</div>
        <div class="sub">当前浮盈亏</div>
        <div class="overview-lines">
          <div class="overview-line"><span>持仓数</span><strong>${item.openPositions} 个</strong></div>
          <div class="overview-line"><span>持仓价值</span><strong>${fmt(item.currentValueUsdt, 2)} USDT</strong></div>
          <div class="overview-line"><span>已实现盈亏</span><strong class="${clsByPnl(item.realizedPnlUsdt)}">${fmt(item.realizedPnlUsdt, 4)} USDT</strong></div>
          <div class="overview-line"><span>已平仓</span><strong>${item.closedCount} 个</strong></div>
        </div>
      `;
    }

    function renderLeverageInfo(tradingSetup) {
      const longLeverage = tradingSetup?.longLeverage || '-';
      const shortLeverage = tradingSetup?.shortLeverage || '-';
      const longMarginMode = tradingSetup?.longMarginMode === 'ISOLATED' ? '逐仓' : (tradingSetup?.longMarginMode || '-');
      const shortMarginMode = tradingSetup?.shortMarginMode === 'ISOLATED' ? '逐仓' : (tradingSetup?.shortMarginMode || '-');
      document.getElementById('leverageWrap').textContent =
        `当前持仓规则（来自币安 API）: 做多 ${longLeverage}X ${longMarginMode}，做空 ${shortLeverage}X ${shortMarginMode}`;
    }

    function renderOverallSummary(summary) {
      document.getElementById('overallSummary').innerHTML = `
        <div class="value ${clsByPnl(summary.totalUnrealizedProfit)}">${fmt(summary.totalUnrealizedProfit, 4)} USDT</div>
        <div class="sub">当前总浮盈亏</div>
        <div class="overview-lines">
          <div class="overview-line"><span>总持仓数</span><strong>${summary.openPositions} 个</strong></div>
          <div class="overview-line"><span>累计已实现</span><strong class="${clsByPnl(summary.realizedPnlUsdt)}">${fmt(summary.realizedPnlUsdt, 4)} USDT</strong></div>
          <div class="overview-line"><span>累计已平仓</span><strong>${summary.closedCount} 个</strong></div>
          <div class="overview-line"><span>爆仓次数</span><strong class="${(summary.forceOrderCount || 0) > 0 ? 'bad' : ''}">${summary.forceOrderCount ?? 0} 次</strong></div>
        </div>
      `;
    }

    function renderPositions(positions) {
      const wrap = document.getElementById('positionsWrap');
      if (!positions.length) {
        wrap.innerHTML = '<div class="empty">当前没有持仓。</div>';
        return;
      }
      const rows = positions.map(row => `
        <tr>
          <td>${sideLeveragePill(row.side, row.leverage)}</td>
          <td>${row.contractSymbol || '-'}</td>
          <td>${fmt(row.entryPrice, 6)}</td>
          <td>${fmt(row.markPrice, 6)}</td>
          <td>${fmt(row.positionUsdt, 2)} U</td>
          <td>${fmt(row.currentValueUsdt, 2)} U</td>
          <td class="${clsByPnl(row.unrealizedProfit)}">${fmt(row.unrealizedProfit, 4)} USDT</td>
          <td class="${clsByPnl(row.pnlPct)}">${fmt(row.pnlPct, 2)}%</td>
          <td>${translateStatus(row.status)}</td>
          <td>${row.openedAt ? fmtCloseTime(Number(row.openedAt) * 1000) : '-'}</td>
        </tr>
      `).join('');
      wrap.innerHTML = `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>方向</th>
                <th>合约</th>
                <th>开仓价</th>
                <th>现价</th>
                <th>开仓 U</th>
                <th>当前价值 U</th>
                <th>浮盈亏</th>
                <th>收益率</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      `;
      const headRow = wrap.querySelector('thead tr');
      if (headRow) {
        const openedAtHead = document.createElement('th');
        openedAtHead.textContent = '开仓时间';
        headRow.appendChild(openedAtHead);
      }
    }

    function summarizeStrategyOpenReasons(item) {
      const decisions = Array.isArray(item?.latestDecisions) ? item.latestDecisions : [];
      const openAction = item?.side === 'SHORT' ? 'enter_short' : 'enter_long';
      const relevant = decisions.filter(row => {
        const action = row?.action || '';
        return action === openAction || action === 'hold' || action === 'skip';
      });
      const reasonCounts = new Map();
      let holdCount = 0;
      let openedCount = 0;
      let noUsdtPerpetualCount = 0;
      for (const row of relevant) {
        const action = row?.action || '';
        const reason = row?.reason || '';
        if (action === openAction) {
          openedCount += 1;
          continue;
        }
        if (!reason) continue;
        if (action === 'hold' && (
          reason === 'still_strong_positive' ||
          reason === 'still_strong_negative' ||
          reason === 'snapshot_protection' ||
          reason === 'signal_drop_guard'
        )) {
          holdCount += 1;
        }
        if (reason === 'no_usdt_perpetual') {
          noUsdtPerpetualCount += 1;
        }
        reasonCounts.set(reason, (reasonCounts.get(reason) || 0) + 1);
      }
      const summaryRows = [];
      if (holdCount > 0) {
        summaryRows.push(`已有持仓继续持有: ${holdCount} 个`);
      }
      if (noUsdtPerpetualCount > 0) {
        summaryRows.push(`没有可用 USDT 永续合约: ${noUsdtPerpetualCount} 个`);
      }
      const orderedReasons = Array.from(reasonCounts.entries())
        .filter(([reason]) => ![
          'still_strong_positive',
          'still_strong_negative',
          'snapshot_protection',
          'signal_drop_guard',
          'no_usdt_perpetual',
        ].includes(reason))
        .sort((a, b) => b[1] - a[1]);
      for (const [reason, count] of orderedReasons) {
        summaryRows.push(`${translateReason(reason)}: ${count} 个`);
      }
      return {
        openedCount,
        blockedCount: Math.max((item?.candidateCount ?? 0) - openedCount, 0),
        noUsdtPerpetualCount,
        holdCount,
        summaryRows,
      };
    }

    function renderStrategies(strategies) {
      const wrap = document.getElementById('strategiesWrap');
      const entries = Object.entries(strategies || {}).sort(([, a], [, b]) => {
        const aPriority = a?.side === 'SHORT' ? 1 : 0;
        const bPriority = b?.side === 'SHORT' ? 1 : 0;
        return aPriority - bPriority;
      });
      if (!entries.length) {
        wrap.innerHTML = '<div class="empty">暂无策略状态。</div>';
        return;
      }
      wrap.innerHTML = entries.map(([id, item]) => {
        const openSummary = summarizeStrategyOpenReasons(item);
        const candidateLabel = item?.side === 'SHORT' ? '当前 AI 强烈看空候选' : '当前 AI 强烈看多候选';
        const blockedHtml = openSummary.summaryRows.length
          ? `<div style="margin-top:8px;padding-top:8px;border-top:1px dashed #e5e7eb;">
               <div><strong>本轮未新开仓:</strong> ${openSummary.blockedCount} 个</div>
               ${openSummary.summaryRows.map(text => `<div class="sub">${text}</div>`).join('')}
             </div>`
          : `<div style="margin-top:8px;padding-top:8px;border-top:1px dashed #e5e7eb;"><div><strong>本轮未新开仓:</strong> ${openSummary.blockedCount} 个</div></div>`;
        return `
          <div class="mini-item strategy-card ${sideClass(item.side)}-card">
            <div class="mini-title ${sideClass(item.side)}-text">${translateStrategyName(id, item.name)}</div>
            <div>方向: <span class="side-text ${sideClass(item.side)}-text">${sideLabel(item.side)}</span></div>
            <div>状态: <span class="pill">${translateStatus(item.status)}</span></div>
            <div>${candidateLabel}: ${item.candidateCount ?? 0} 个</div>
            <div>本轮新开仓: ${item.openedCount ?? 0} 个</div>
            <div>没有可用 USDT 永续合约: ${openSummary.noUsdtPerpetualCount ?? 0} 个</div>
            <div>累计平仓: ${item.closedCount ?? 0} 个</div>
            <div>已实现盈亏: ${fmt(item.realizedPnlUsdt, 4)} USDT</div>
            ${blockedHtml}
          </div>
        `;
      }).join('');
    }

    function renderUnopenedCandidates(items) {
      const wrap = document.getElementById('unopenedWrap');
      if (!items || !items.length) {
        wrap.innerHTML = '<div class="empty">当前这一轮没有被规则拦下的候选币。</div>';
        return;
      }
      const rows = items.map(row => `
        <tr>
          <td>${sidePill(row.side)}</td>
          <td>${row.asset || '-'}</td>
          <td>${translateReason(row.reason)}</td>
          <td>${row.detail || '-'}</td>
        </tr>
      `).join('');
      wrap.innerHTML = `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>方向</th>
                <th>币种</th>
                <th>未开原因</th>
                <th>补充说明</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      `;
    }

    function fmtDuration(seconds) {
      const total = Number(seconds || 0);
      if (!Number.isFinite(total) || total <= 0) return '0分钟';
      const hours = Math.floor(total / 3600);
      const minutes = Math.ceil((total % 3600) / 60);
      if (hours <= 0) return `${minutes}分钟`;
      if (minutes <= 0) return `${hours}小时`;
      return `${hours}小时${minutes}分钟`;
    }

    function renderCooldowns(summary, items) {
      const wrap = document.getElementById('cooldownWrap');
      const meta = document.getElementById('cooldownMeta');
      const resetBtn = document.getElementById('resetCooldownBtn');
      const count = summary?.count ?? 0;
      const label = summary?.label ?? '-';
      meta.textContent = `当前冷却 ${count} 个，默认冷却 ${label}`;
      resetBtn.disabled = count === 0;
      if (!items || !items.length) {
        wrap.innerHTML = '<div class="empty">当前没有处于冷却中的币。</div>';
        return;
      }
      const rows = items.map(row => `
        <tr>
          <td>${sidePill(row.side)}</td>
          <td>${row.contractSymbol || row.asset || '-'}</td>
          <td>${translateReason(row.reason)}</td>
          <td>${fmtCloseTime((row.cooldownUntil || 0) * 1000)}</td>
          <td>${fmtDuration(row.remainingSeconds)}</td>
        </tr>
      `).join('');
      wrap.innerHTML = `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>方向</th>
                <th>合约</th>
                <th>来源</th>
                <th>冷却到</th>
                <th>剩余时间</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      `;
    }

    function renderRuleSummary(items) {
      const wrap = document.getElementById('ruleSummaryWrap');
      if (!items || !items.length) {
        wrap.innerHTML = '<div class="empty">暂时没有规则摘要。</div>';
        return;
      }
      wrap.innerHTML = items.map(item => `
        <div class="mini-item">
          <div class="rule-row">
            <div class="rule-name">${item.title || '-'}</div>
            <div class="rule-value">${item.value || '-'}</div>
          </div>
        </div>
      `).join('');
    }

    function renderConfigToggles(items) {
      const wrap = document.getElementById('configToggleWrap');
      const meta = document.getElementById('configToggleMeta');
      const saveBtn = document.getElementById('saveToggleBtn');
      if (!items || !items.length) {
        wrap.innerHTML = '<div class="empty">暂时没有可调开关。</div>';
        meta.textContent = '当前报表没有返回可调开关。';
        saveBtn.disabled = true;
        return;
      }
      saveBtn.disabled = false;
      meta.textContent = `共 ${items.length} 个开关，保存后机器人下个轮询周期自动生效。`;
      wrap.innerHTML = `
        <div class="toggle-list">
          ${items.map(item => `
            <div class="toggle-item">
              <div class="toggle-copy">
                <div class="toggle-name">${item.label || item.key || '-'}</div>
                <div class="toggle-detail">${item.detail || ''}</div>
              </div>
              <label class="switch">
                <input type="checkbox" data-config-key="${item.key}" ${item.enabled ? 'checked' : ''}>
                <span class="slider"></span>
              </label>
            </div>
          `).join('')}
        </div>
      `;
    }

    async function saveConfigToggles() {
      const token = new URLSearchParams(window.location.search).get('token');
      const apiUrl = token ? `/api/config-toggles?token=${encodeURIComponent(token)}` : '/api/config-toggles';
      const btn = document.getElementById('saveToggleBtn');
      const inputs = Array.from(document.querySelectorAll('#configToggleWrap input[data-config-key]'));
      const toggles = {};
      for (const input of inputs) {
        toggles[input.dataset.configKey] = input.checked;
      }
      btn.disabled = true;
      try {
        const res = await fetch(apiUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ toggles }),
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.error || '保存开关失败');
        }
        await refresh();
      } catch (err) {
        alert(`保存开关失败: ${err.message || err}`);
      } finally {
        btn.disabled = false;
      }
    }

    function renderReadiness(items) {
      const wrap = document.getElementById('readinessWrap');
      if (!wrap) {
        return;
      }
      if (!items || !items.length) {
        wrap.innerHTML = '<div class="empty">暂时没有准入清单。</div>';
        return;
      }
      wrap.innerHTML = items.map(item => `
        <div class="mini-item ${item.ok ? 'ok-card' : 'warn-card'}">
          <div class="mini-title ${item.ok ? 'ok-text' : 'warn-text'}">${item.ok ? '已满足' : '未满足'} · ${item.title || '-'}</div>
          <div>${item.detail || '-'}</div>
        </div>
      `).join('');
    }

    function renderAccountRiskStats(stats) {
      const wrap = document.getElementById('riskStatsWrap');
      if (!stats) {
        wrap.innerHTML = '<div class="empty">暂无风险统计。</div>';
        return;
      }
      const currentDrawdownValue = Number(stats.currentDrawdownUsdt || 0);
      const items = [
        { title: '账户最大回撤', value: `${fmt(stats.accountMaxDrawdownUsdt ?? stats.maxDrawdownUsdt, 4)} USDT` },
        { title: '账户最大回撤率', value: stats.accountMaxDrawdownPct != null ? `${fmt(stats.accountMaxDrawdownPct, 2)}%` : '-' },
        { title: '账户当前回撤', value: `${fmt(stats.currentDrawdownUsdt, 4)} USDT`, cls: currentDrawdownValue > 0 ? 'bad' : '' },
        { title: '账户当前回撤率', value: stats.currentDrawdownPct != null ? `${fmt(stats.currentDrawdownPct, 2)}%` : '-' },
        { title: '账户权益峰值', value: `${fmt(stats.accountPeakEquityUsdt, 4)} USDT` },
        { title: '当前账户权益', value: `${fmt(stats.currentEquityUsdt, 4)} USDT` },
        { title: '策略净值最大回撤', value: `${fmt(stats.strategyMaxDrawdownUsdt, 4)} USDT` },
        { title: '策略净值回撤率', value: stats.strategyMaxDrawdownPct != null ? `${fmt(stats.strategyMaxDrawdownPct, 2)}%` : '-' },
        { title: '胜率', value: stats.winRatePct != null ? `${fmt(stats.winRatePct, 2)}%` : '-' },
        { title: '盈亏比', value: fmt(stats.avgWinLossRatio, 2) },
        { title: '利润因子', value: fmt(stats.profitFactor, 2) },
        { title: '最大连亏', value: `${stats.maxConsecutiveLosses ?? 0} 笔` },
        { title: '最大连赢', value: `${stats.maxConsecutiveWins ?? 0} 笔` },
        { title: '平均盈利', value: `${fmt(stats.avgWinUsdt, 4)} USDT` },
        { title: '平均亏损', value: `${fmt(stats.avgLossUsdtAbs, 4)} USDT` },
        { title: '当前浮动盈亏', value: `${fmt(stats.currentUnrealizedPnlUsdt, 4)} USDT`, cls: clsByPnl(stats.currentUnrealizedPnlUsdt) }
      ];
      wrap.innerHTML = items.map(item => `
        <div class="mini-item">
          <div class="rule-row">
            <div class="rule-name">${item.title}</div>
            <div class="rule-value ${item.cls || ''}">${item.value}</div>
          </div>
        </div>
      `).join('');
    }

    function renderBestWorst(data) {
      const positions = data.positions || [];
      const withPnl = positions.filter(p => p.unrealizedProfit !== null && p.unrealizedProfit !== undefined);
      const bestWrap = document.getElementById('bestWrap');
      const worstWrap = document.getElementById('worstWrap');
      if (!withPnl.length) {
        bestWrap.innerHTML = '当前还没有可比较的仓位。';
        worstWrap.innerHTML = '当前还没有可比较的仓位。';
        return;
      }
      const sorted = [...withPnl].sort((a, b) => Number(b.unrealizedProfit) - Number(a.unrealizedProfit));
      const best = sorted[0];
      const worst = sorted[sorted.length - 1];
      bestWrap.innerHTML = `
        <div class="value">${best.contractSymbol}</div>
        <div class="overview-lines">
          <div class="overview-line"><span>方向</span><strong>${sideLabel(best.side)}</strong></div>
          <div class="overview-line"><span>浮盈亏</span><strong class="${clsByPnl(best.unrealizedProfit)}">${fmt(best.unrealizedProfit, 4)} USDT</strong></div>
          <div class="overview-line"><span>收益率</span><strong class="${clsByPnl(best.pnlPct)}">${fmt(best.pnlPct, 2)}%</strong></div>
        </div>
      `;
      worstWrap.innerHTML = `
        <div class="value">${worst.contractSymbol}</div>
        <div class="overview-lines">
          <div class="overview-line"><span>方向</span><strong>${sideLabel(worst.side)}</strong></div>
          <div class="overview-line"><span>浮盈亏</span><strong class="${clsByPnl(worst.unrealizedProfit)}">${fmt(worst.unrealizedProfit, 4)} USDT</strong></div>
          <div class="overview-line"><span>收益率</span><strong class="${clsByPnl(worst.pnlPct)}">${fmt(worst.pnlPct, 2)}%</strong></div>
        </div>
      `;
    }

    let _tradeHistoryData = [];
    let _tradeHistoryFilter = 'all';

    function syncTradeFilterButtons() {
      const filterMap = {
        thFilterAll: 'all',
        thFilterLong: 'long',
        thFilterShort: 'short',
      };
      Object.entries(filterMap).forEach(([id, value]) => {
        const btn = document.getElementById(id);
        if (btn) {
          btn.classList.toggle('active', _tradeHistoryFilter === value);
        }
      });
    }

    function filterTradeHistory(filter) {
      _tradeHistoryFilter = filter;
      const wrap = document.getElementById('tradeHistoryWrap');
      if (wrap) {
        wrap.dataset.expanded = 'false';
      }
      syncTradeFilterButtons();
      renderTradeHistory(_tradeHistoryData);
    }

    function renderTradeHistory(items) {
      _tradeHistoryData = items;
      const wrap = document.getElementById('tradeHistoryWrap');
      syncTradeFilterButtons();
      if (!items || !items.length) {
        wrap.innerHTML = '<div class="empty">暂无平仓记录。</div>';
        return;
      }
      let filtered = items;
      if (_tradeHistoryFilter === 'long') {
        filtered = items.filter(r => r.direction === '做多');
      } else if (_tradeHistoryFilter === 'short') {
        filtered = items.filter(r => r.direction === '做空');
      }
      if (!filtered.length) {
        wrap.innerHTML = '<div class="empty">该筛选条件下没有记录。</div>';
        return;
      }
      const defaultShow = 20;
      const showAll = wrap.dataset.expanded === 'true';
      const display = showAll ? filtered : filtered.slice(0, defaultShow);
      const rows = display.map(row => {
        const pnlCell = row.isClose && row.realizedPnlUsdt != null
          ? `<td class="${clsByPnl(row.realizedPnlUsdt)}">${fmt(row.realizedPnlUsdt, 4)} USDT</td>`
          : `<td>-</td>`;
        const reasonCell = row.isClose && row.closeReason
          ? `<td>${translateReason(row.closeReason)}</td>`
          : `<td>-</td>`;
        return `
        <tr>
          <td>${fmtCloseTime(row.timeMs)}</td>
          <td><span class="pill ${row.direction === '做空' ? 'short' : 'long'}">${row.direction || '-'}</span></td>
          <td>${row.symbol || '-'}</td>
          <td>${row.action || '-'}</td>
          <td>${row.type || '-'}</td>
          <td>${fmt(row.price, 6)}</td>
          <td>${row.quantity || '-'}</td>
          ${pnlCell}
          ${reasonCell}
          <td>${translateStatus(row.status)}</td>
        </tr>
      `}).join('');
      const expandBtn = filtered.length > defaultShow
        ? `<div style="text-align:center;padding:10px;">
            <button type="button" class="secondary-btn" onclick="toggleTradeHistoryExpand()">
              ${showAll ? '收起 ▲' : '展开全部 (' + filtered.length + ' 条) ▼'}
            </button>
           </div>`
        : '';
      wrap.innerHTML = `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>方向</th>
                <th>合约</th>
                <th>操作</th>
                <th>订单类型</th>
                <th>成交价</th>
                <th>数量</th>
                <th>盈亏</th>
                <th>平仓原因</th>
                <th>状态</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
        ${expandBtn}
      `;
    }

    function toggleTradeHistoryExpand() {
      const wrap = document.getElementById('tradeHistoryWrap');
      wrap.dataset.expanded = wrap.dataset.expanded === 'true' ? 'false' : 'true';
      renderTradeHistory(_tradeHistoryData);
    }

    function renderForceOrders(summary) {
      const wrap = document.getElementById('forceOrderWrap');
      const meta = document.getElementById('forceOrderMeta');
      const count = summary?.count ?? 0;
      const totalLoss = summary?.totalLossUsdt ?? '0';
      meta.innerHTML = `来自币安 forceOrders 接口 · <span class="${count > 0 ? 'bad' : ''}">共 ${count} 次爆仓</span>，实际亏损: <span class="bad">${fmt(totalLoss, 4)} USDT</span>`;
      const items = summary?.items || [];
      if (!items.length) {
        wrap.innerHTML = '<div class="empty">暂无爆仓/强平记录，非常好！</div>';
        return;
      }
      const rows = items.map(row => `
        <tr>
          <td>${fmtCloseTime(row.timeMs)}</td>
          <td><span class="pill ${row.direction === '做空' ? 'short' : 'long'}">${row.direction || '-'}</span></td>
          <td>${row.symbol || '-'}</td>
          <td class="bad">${row.type || '-'}</td>
          <td>${fmt(row.price, 6)}</td>
          <td>${row.quantity || '-'}</td>
          <td class="bad">${fmt(row.realizedPnl, 4)} USDT</td>
        </tr>
      `).join('');
      wrap.innerHTML = `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>时间</th>
                <th>方向</th>
                <th>合约</th>
                <th>类型</th>
                <th>价格</th>
                <th>数量</th>
                <th>实际亏损</th>
              </tr>
            </thead>
            <tbody>${rows}</tbody>
          </table>
        </div>
      `;
    }

    function renderRecoveryStats(stats) {
      const wrap = document.getElementById('recoveryWrap');
      const meta = document.getElementById('recoveryMeta');
      if (!stats || !stats.trackedCloseCount) {
        meta.textContent = '这项统计只计算已经记录到“最大浮亏%”的历史平仓单，爆仓/强平会尽量回补进来。';
        wrap.innerHTML = '<div class="empty">当前还没有足够的深跌回收样本。</div>';
        return;
      }
      meta.textContent = `当前已统计 ${stats.trackedCloseCount} 笔已记录最大浮亏轨迹的历史平仓单，含爆仓/强平回溯样本。`;
      const maxDrawdownCase = stats.maxDrawdownCase;
      const maxRecoveredDrawdownCase = stats.maxRecoveredDrawdownCase;
      const underwaterCases = stats.underwaterCases || [];
      const summaryItems = [
        { title: '已记录样本', value: `${stats.trackedCloseCount} 笔` },
        { title: '曾跌入亏损', value: `${stats.underwaterCloseCount || 0} 笔` },
        { title: '深跌后最终盈利', value: `${stats.recoveredWinCount || 0} 笔`, cls: (stats.recoveredWinCount || 0) > 0 ? 'good' : '' },
        { title: '深跌后最终亏损', value: `${stats.underwaterLossCount || 0} 笔`, cls: (stats.underwaterLossCount || 0) > 0 ? 'bad' : '' },
        { title: '水下后回正概率', value: stats.recoveredWinRatePct != null ? `${fmt(stats.recoveredWinRatePct, 2)}%` : '-' },
        { title: '全样本回正占比', value: stats.overallRecoveredWinRatePct != null ? `${fmt(stats.overallRecoveredWinRatePct, 2)}%` : '-' },
        { title: '平均最大浮亏', value: stats.avgUnderwaterDrawdownPct != null ? `${fmt(stats.avgUnderwaterDrawdownPct, 2)}%` : '-' },
        { title: '回正单平均最大浮亏', value: stats.avgRecoveredDrawdownPct != null ? `${fmt(stats.avgRecoveredDrawdownPct, 2)}%` : '-' },
        {
          title: '水下样本最终盈利合计',
          value: stats.underwaterFinalProfitUsdt != null ? `${fmt(stats.underwaterFinalProfitUsdt, 4)} USDT` : '-',
          cls: clsByPnl(stats.underwaterFinalProfitUsdt),
        },
        {
          title: '水下样本最终亏损合计',
          value: stats.underwaterFinalLossUsdtAbs != null
            ? (Number(stats.underwaterFinalLossUsdtAbs) > 0
              ? `-${fmt(stats.underwaterFinalLossUsdtAbs, 4)} USDT`
              : `${fmt(stats.underwaterFinalLossUsdtAbs, 4)} USDT`)
            : '-',
          cls: (stats.underwaterFinalLossUsdtAbs != null && Number(stats.underwaterFinalLossUsdtAbs) > 0) ? 'bad' : '',
        },
        {
          title: '水下样本最终净结果',
          value: stats.underwaterNetResultUsdt != null ? `${fmt(stats.underwaterNetResultUsdt, 4)} USDT` : '-',
          cls: clsByPnl(stats.underwaterNetResultUsdt),
        },
        {
          title: '水下样本最终收益率',
          value: stats.underwaterNetReturnPct != null ? `${fmt(stats.underwaterNetReturnPct, 2)}%` : '-',
          cls: clsByPnl(stats.underwaterNetReturnPct),
        },
        {
          title: '全样本最大浮亏',
          value: maxDrawdownCase
            ? `${maxDrawdownCase.contractSymbol || maxDrawdownCase.asset || '-'} ${sideLabel(maxDrawdownCase.side)} / ${fmt(maxDrawdownCase.maxDrawdownPct, 2)}%`
            : '-',
          cls: maxDrawdownCase ? 'bad' : '',
        },
        {
          title: '最大浮亏后仍回正',
          value: maxRecoveredDrawdownCase
            ? `${maxRecoveredDrawdownCase.contractSymbol || maxRecoveredDrawdownCase.asset || '-'} ${sideLabel(maxRecoveredDrawdownCase.side)} / ${fmt(maxRecoveredDrawdownCase.maxDrawdownPct, 2)}%`
            : '-',
          cls: maxRecoveredDrawdownCase ? 'good' : '',
        },
      ];

      const casesHtml = underwaterCases.length
        ? `
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>时间</th>
                  <th>方向</th>
                  <th>合约</th>
                  <th>最大浮亏</th>
                  <th>最终净收益</th>
                  <th>最终收益率</th>
                  <th>是否回正</th>
                  <th>平仓原因</th>
                </tr>
              </thead>
              <tbody>
                ${underwaterCases.map(row => `
                  <tr>
                    <td>${fmtCloseTime((row.timestamp || 0) * 1000)}</td>
                    <td>${sidePill(row.side)}</td>
                    <td>${row.contractSymbol || row.asset || '-'}</td>
                    <td class="bad">${fmt(row.maxDrawdownPct, 2)}%</td>
                    <td class="${clsByPnl(row.netRealizedPnlUsdt)}">${fmt(row.netRealizedPnlUsdt, 4)} USDT</td>
                    <td class="${clsByPnl(row.finalReturnPct)}">${row.finalReturnPct != null ? `${fmt(row.finalReturnPct, 2)}%` : '-'}</td>
                    <td class="${row.recoveredToProfit ? 'good' : 'bad'}">${row.recoveredToProfit ? '是' : '否'}</td>
                    <td>${translateReason(row.reason)}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '';

      wrap.innerHTML = `
        <div class="mini-list">
          ${summaryItems.map(item => `
            <div class="mini-item">
              <div class="rule-row">
                <div class="rule-name">${item.title}</div>
                <div class="rule-value ${item.cls || ''}">${item.value}</div>
              </div>
            </div>
          `).join('')}
        </div>
        ${casesHtml}
      `;
    }

    function renderMonitorSummary(summary) {
      const wrap = document.getElementById('monitorWrap');
      const meta = document.getElementById('monitorMeta');
      if (!summary) {
        meta.textContent = '巡检结果还没有生成，通常是 monitor.py 还没启动。';
        wrap.innerHTML = '<div class="empty">当前还没有巡检结果。</div>';
        return;
      }

      const issues = summary.currentIssues || [];
      const healthy = !!summary.healthy;
      const botRuntime = summary.botRuntime || {};
      const monitorRuntime = summary.monitorRuntime || {};
      const dashboardHealth = summary.dashboardHealth || {};
      const recentLogErrors = summary.recentLogErrors || [];
      const recentIncidents = summary.recentIncidents || [];
      const runtimeArtifacts = Object.values(summary.runtimeArtifacts || {});
      const reportPaths = summary.reportPaths || {};
      const tradeAudits = summary.tradeAudits || {};
      const entryAudit = tradeAudits.entryExecution || {};
      const exitAudit = tradeAudits.exitExecution || {};
      const pendingExitAudit = tradeAudits.pendingExit || {};
      meta.textContent = healthy
        ? `当前巡检未发现 error / warn 级别问题。最近巡检: ${fmtCloseTime((summary.generatedAt || 0) * 1000)}`
        : `当前巡检发现 ${summary.errorCount || 0} 个错误、${summary.warnCount || 0} 个警告。最近巡检: ${fmtCloseTime((summary.generatedAt || 0) * 1000)}`;

      const statItems = [
        { title: '巡检状态', value: healthy ? '正常' : '有异常', cls: healthy ? 'good' : 'bad' },
        { title: '错误数', value: `${summary.errorCount || 0} 个`, cls: (summary.errorCount || 0) > 0 ? 'bad' : '' },
        { title: '警告数', value: `${summary.warnCount || 0} 个`, cls: (summary.warnCount || 0) > 0 ? 'bad' : '' },
        { title: '信息数', value: `${summary.infoCount || 0} 个` },
        { title: 'Bot 最近成功', value: botRuntime.lastSuccessText || '-', cls: botRuntime.ok === false ? 'bad' : 'good' },
        { title: 'Bot 最近异常', value: `${botRuntime.recentFailureCount || 0} 条`, cls: (botRuntime.recentFailureCount || 0) > 0 ? 'bad' : '' },
        { title: 'Monitor 最近成功', value: monitorRuntime.lastSuccessText || '-', cls: monitorRuntime.ok === false ? 'bad' : 'good' },
        {
          title: 'Dashboard 健康检查',
          value: dashboardHealth.checked ? (dashboardHealth.ok ? '正常' : '失败') : '未执行',
          cls: dashboardHealth.checked ? (dashboardHealth.ok ? 'good' : 'bad') : '',
        },
        { title: '本地持仓', value: `${summary.positionCounts?.total || 0} 个` },
        {
          title: '交易所对账',
          value: summary.checks?.liveState?.checked
            ? (summary.checks?.liveState?.ok ? '正常' : '有差异')
            : '未执行',
          cls: summary.checks?.liveState?.checked
            ? (summary.checks?.liveState?.ok ? 'good' : 'bad')
            : '',
        },
        {
          title: '关键文件异常',
          value: `${runtimeArtifacts.filter(item => item.statusClass === 'bad').length} 个`,
          cls: runtimeArtifacts.some(item => item.statusClass === 'bad') ? 'bad' : 'good',
        },
        {
          title: '开仓审计异常',
          value: `${entryAudit.issueCount || 0} 个`,
          cls: (entryAudit.issueCount || 0) > 0 ? 'bad' : 'good',
        },
        {
          title: '平仓审计异常',
          value: `${exitAudit.issueCount || 0} 个`,
          cls: (exitAudit.issueCount || 0) > 0 ? 'bad' : 'good',
        },
        {
          title: '漏执行平仓',
          value: `${pendingExitAudit.overdueCount || 0} 个`,
          cls: (pendingExitAudit.overdueCount || 0) > 0 ? 'bad' : 'good',
        },
      ];

      const reportHtml = `
        <div class="mini-list" style="margin-top:12px;">
          <div class="mini-item">
            <div class="mini-title">固定报告位置</div>
            <div class="sub">Markdown: <span style="font-family:Consolas,'Courier New',monospace;">${reportPaths.markdown || '-'}</span></div>
            <div class="sub">JSON: <span style="font-family:Consolas,'Courier New',monospace;">${reportPaths.json || '-'}</span></div>
            <div class="sub">事件流: <span style="font-family:Consolas,'Courier New',monospace;">${reportPaths.events || '-'}</span></div>
            <div class="sub">Bot 日志: <span style="font-family:Consolas,'Courier New',monospace;">${reportPaths.botLog || '-'}</span></div>
            <div class="sub">Monitor 日志: <span style="font-family:Consolas,'Courier New',monospace;">${reportPaths.monitorLog || '-'}</span></div>
          </div>
        </div>
      `;

      const artifactHtml = runtimeArtifacts.length
        ? `
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>文件</th>
                  <th>状态</th>
                  <th>更新时间间隔</th>
                  <th>路径</th>
                </tr>
              </thead>
              <tbody>
                ${runtimeArtifacts.map(item => `
                  <tr>
                    <td>${item.label || '-'}</td>
                    <td class="${item.statusClass || (!item.exists || item.stale ? 'bad' : 'good')}">
                      ${item.statusText || (!item.exists ? '缺失' : (item.stale ? '过旧' : '正常'))}
                      ${item.note ? `<div class="sub" style="margin-top:4px;">${item.note}</div>` : ''}
                    </td>
                    <td>${item.ageSeconds != null ? fmtDuration(item.ageSeconds) : '-'}</td>
                    <td style="font-family:Consolas,'Courier New',monospace;">${item.path || '-'}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '';

      const issueHtml = issues.length
        ? `
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>级别</th>
                  <th>规则</th>
                  <th>标题</th>
                  <th>说明</th>
                </tr>
              </thead>
              <tbody>
                ${issues.map(issue => `
                  <tr>
                    <td class="${issue.level === 'error' ? 'bad' : (issue.level === 'warn' ? 'bad' : '')}">${issue.level || '-'}</td>
                    <td>${issue.rule || '-'}</td>
                    <td>${issue.title || '-'}</td>
                    <td>${issue.detail || '-'}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '<div class="empty">当前巡检没有发现异常。</div>';

      const incidentHtml = recentIncidents.length
        ? `
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>时间</th>
                  <th>级别</th>
                  <th>规则</th>
                  <th>标题</th>
                </tr>
              </thead>
              <tbody>
                ${recentIncidents.map(item => `
                  <tr>
                    <td>${fmtCloseTime((item.timestamp || 0) * 1000)}</td>
                    <td class="${item.level === 'error' ? 'bad' : (item.level === 'warn' ? 'bad' : '')}">${item.level || '-'}</td>
                    <td>${item.rule || '-'}</td>
                    <td>${item.title || '-'}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '<div class="empty">最近没有新的巡检事件。</div>';

      const logHtml = recentLogErrors.length
        ? `
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>来源</th>
                  <th>时间</th>
                  <th>级别</th>
                  <th>摘要</th>
                </tr>
              </thead>
              <tbody>
                ${recentLogErrors.map(item => `
                  <tr>
                    <td>${item.source || '-'}</td>
                    <td>${item.timestampText || '-'}</td>
                    <td class="${item.level === 'error' ? 'bad' : ''}">${item.level || '-'}</td>
                    <td>${item.message || '-'}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '<div class="empty">最近没有捕获到日志异常。</div>';

      const entryAuditHtml = (entryAudit.records || []).length
        ? `
          <div class="sub" style="margin-top:8px;">已审计 ${entryAudit.auditedCount || 0} 条，异常 ${entryAudit.issueCount || 0} 条，缺少审计字段 ${entryAudit.missingAuditCount || 0} 条</div>
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>时间</th>
                  <th>方向</th>
                  <th>合约</th>
                  <th>状态</th>
                  <th>说明</th>
                </tr>
              </thead>
              <tbody>
                ${(entryAudit.records || []).map(item => `
                  <tr>
                    <td>${item.timestampText || '-'}</td>
                    <td>${item.side === 'LONG' ? '做多' : (item.side === 'SHORT' ? '做空' : '-')}</td>
                    <td>${item.asset || '-'}</td>
                    <td class="${item.status === 'error' ? 'bad' : (item.status === 'ok' ? 'good' : '')}">
                      ${item.status === 'error' ? '异常' : (item.status === 'ok' ? '通过' : '缺字段')}
                    </td>
                    <td>${item.details || '-'}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '<div class="empty">最近没有新的开仓审计样本。</div>';

      const exitAuditHtml = (exitAudit.records || []).length
        ? `
          <div class="sub" style="margin-top:8px;">已审计 ${exitAudit.auditedCount || 0} 条，异常 ${exitAudit.issueCount || 0} 条，缺少审计字段 ${exitAudit.missingAuditCount || 0} 条</div>
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>时间</th>
                  <th>方向</th>
                  <th>合约</th>
                  <th>原因</th>
                  <th>状态</th>
                  <th>说明</th>
                </tr>
              </thead>
              <tbody>
                ${(exitAudit.records || []).map(item => `
                  <tr>
                    <td>${item.timestampText || '-'}</td>
                    <td>${item.side === 'LONG' ? '做多' : (item.side === 'SHORT' ? '做空' : '-')}</td>
                    <td>${item.asset || '-'}</td>
                    <td>${item.reason || '-'}</td>
                    <td class="${item.status === 'error' ? 'bad' : (item.status === 'ok' ? 'good' : '')}">
                      ${item.status === 'error' ? '异常' : (item.status === 'ok' ? '通过' : '缺字段')}
                    </td>
                    <td>${item.details || '-'}</td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '<div class="empty">最近没有新的平仓审计样本。</div>';

      const pendingExitHtml = (pendingExitAudit.records || []).length
        ? `
          <div class="sub" style="margin-top:8px;">当前候选 ${pendingExitAudit.candidateCount || 0} 条，确认异常 ${pendingExitAudit.overdueCount || 0} 条</div>
          <div class="table-wrap" style="margin-top:12px;">
            <table>
              <thead>
                <tr>
                  <th>方向</th>
                  <th>合约</th>
                  <th>原因</th>
                  <th>状态</th>
                  <th>持续时间</th>
                  <th>说明</th>
                </tr>
              </thead>
              <tbody>
                ${(pendingExitAudit.records || []).map(item => `
                  <tr>
                    <td>${item.side === 'LONG' ? '做多' : (item.side === 'SHORT' ? '做空' : '-')}</td>
                    <td>${item.asset || '-'}</td>
                    <td>${item.reason || '-'}</td>
                    <td class="${item.status === 'overdue' ? 'bad' : ''}">
                      ${item.status === 'overdue' ? '未执行' : '待确认'}
                    </td>
                    <td>${item.ageSeconds != null ? fmtDuration(item.ageSeconds) : '-'}</td>
                    <td>
                      ${item.reason === 'signal_lost'
                        ? `丢失轮数 ${item.signalLostRounds || 0} / ${item.threshold || '-'}`
                        : `当前 ${item.currentPnlPct || '-'} / 阈值 ${item.threshold || '-'}`
                      }
                    </td>
                  </tr>
                `).join('')}
              </tbody>
            </table>
          </div>
        `
        : '<div class="empty">当前没有待执行的平仓候选。</div>';

      wrap.innerHTML = `
        <div class="mini-list">
          ${statItems.map(item => `
            <div class="mini-item">
              <div class="rule-row">
                <div class="rule-name">${item.title}</div>
                <div class="rule-value ${item.cls || ''}">${item.value}</div>
              </div>
            </div>
          `).join('')}
        </div>
        ${reportHtml}
        ${artifactHtml}
        <div class="label" style="margin-top:16px;">开仓执行审计</div>
        ${entryAuditHtml}
        <div class="label" style="margin-top:16px;">平仓执行审计</div>
        ${exitAuditHtml}
        <div class="label" style="margin-top:16px;">漏执行平仓审计</div>
        ${pendingExitHtml}
        <div class="label" style="margin-top:16px;">当前问题</div>
        ${issueHtml}
        <div class="label" style="margin-top:16px;">最近巡检事件</div>
        ${incidentHtml}
        <div class="label" style="margin-top:16px;">最近日志异常</div>
        ${logHtml}
      `;
    }

    async function resetCooldowns() {
      const token = new URLSearchParams(window.location.search).get('token');
      const apiUrl = token ? `/api/reset-cooldowns?token=${encodeURIComponent(token)}` : '/api/reset-cooldowns';
      const btn = document.getElementById('resetCooldownBtn');
      btn.disabled = true;
      try {
        const res = await fetch(apiUrl, { method: 'POST' });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error(data.error || '重置冷却失败');
        }
        await refresh();
      } catch (err) {
        alert(`重置冷却失败: ${err.message || err}`);
      } finally {
        btn.disabled = false;
      }
    }

    async function refresh() {
      try {
        const token = new URLSearchParams(window.location.search).get('token');
        const apiUrl = token ? `/api/status?token=${encodeURIComponent(token)}` : '/api/status';
        const res = await fetch(apiUrl);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
      document.getElementById('stamp').textContent = '更新时间: ' + new Date().toLocaleString();
      const warnBanner = document.getElementById('warnBanner');
      if (data.dashboardError) {
        warnBanner.style.display = 'block';
        warnBanner.textContent =
          data.dashboardError === 'refreshing'
            ? '正在后台刷新 Binance 官方数据，当前先显示最近一次成功数据。'
            : 'Binance 官方接口临时失败，当前显示的是最近一次成功数据。';
      } else {
        warnBanner.style.display = 'none';
        warnBanner.textContent = '';
      }

      document.getElementById('source').textContent = translateSource(data.source);
      document.getElementById('openPositions').textContent = `${data.summary?.openPositions ?? 0} 个`;
      document.getElementById('totalPnl').textContent = `${fmt(data.summary?.totalUnrealizedProfit, 4)} USDT`;
      document.getElementById('totalValue').textContent = `${fmt(data.summary?.totalPositionValueUsdt, 2)} USDT`;
      document.getElementById('realizedPnl').textContent = `${fmt(data.summary?.realizedPnlUsdt, 4)} USDT`;

      const wallet = data.account?.availableBalance && data.account?.totalWalletBalance
        ? `${fmt(data.account.availableBalance, 2)} / ${fmt(data.account.totalWalletBalance, 2)}`
        : '-';
      document.getElementById('wallet').textContent = wallet;

      renderLeverageInfo(data.tradingSetup || {});
      renderBlockSummary('longSummary', data.sideSummaries?.LONG || { unrealizedProfit: 0, openPositions: 0, currentValueUsdt: 0, realizedPnlUsdt: 0, closedCount: 0 });
      renderBlockSummary('shortSummary', data.sideSummaries?.SHORT || { unrealizedProfit: 0, openPositions: 0, currentValueUsdt: 0, realizedPnlUsdt: 0, closedCount: 0 });
      renderOverallSummary(data.summary || {});
      renderPositions(data.positions || []);
      renderStrategies(data.strategies || {});
      renderUnopenedCandidates(data.unopenedCandidates || []);
      renderCooldowns(data.cooldownSummary || {}, data.activeCooldowns || []);
      renderBestWorst(data);
      renderTradeHistory(data.tradeHistory || []);
      renderForceOrders(data.forceOrderSummary || {});
      renderRecoveryStats(data.recoveryStats || null);
      renderMonitorSummary(data.monitorSummary || null);
      renderRuleSummary(data.ruleSummary || []);
      renderConfigToggles(data.configToggles || []);
      renderReadiness(data.productionReadiness || []);
      renderAccountRiskStats(data.riskStats || null);
      } catch (err) {
        console.error('refresh failed:', err);
        document.getElementById('stamp').textContent = '数据加载失败: ' + (err.message || err) + '，10 秒后重试...';
      }
    }

    initTabs();
    syncTradeFilterButtons();
    refresh();
    setInterval(refresh, 10000);
  </script>
</body>
</html>
"""


class DashboardHandler(BaseHTTPRequestHandler):
    workdir = Path.cwd()
    dotenv_file = ".env"
    last_report: dict | None = None
    last_report_at: float = 0.0
    last_error: str | None = None
    refresh_in_progress = False
    refresh_lock = threading.Lock()
    cache_ttl_seconds = int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "10"))
    reset_cooldown_remaining_seconds = 10 * 60

    @classmethod
    def _access_token(cls) -> str:
        return os.getenv("DASHBOARD_ACCESS_TOKEN", "").strip()

    def _is_authorized(self, parsed) -> bool:
        token = self.__class__._access_token()
        if not token:
            return True
        query = parse_qs(parsed.query or "")
        query_token = (query.get("token") or [None])[0]
        header_token = self.headers.get("X-Dashboard-Token")
        auth_header = self.headers.get("Authorization", "")
        bearer_token = None
        if auth_header.lower().startswith("bearer "):
            bearer_token = auth_header[7:].strip()
        return token in {query_token, header_token, bearer_token}

    @classmethod
    def _cache_path(cls) -> Path:
        return cls.workdir / "runtime" / "dashboard_cache.json"

    @classmethod
    def _load_cached_report(cls) -> dict | None:
        path = cls._cache_path()
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            return None

    @classmethod
    def _save_cached_report(cls, payload: dict) -> None:
        cls._cache_path().write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @classmethod
    def _state_path(cls) -> Path:
        return cls.workdir / "runtime" / "state.json"

    @classmethod
    def _dotenv_path(cls) -> Path:
        return cls.workdir / cls.dotenv_file

    @classmethod
    def _invalidate_cache(cls) -> None:
        cls.last_report = None
        cls.last_report_at = 0.0
        cls.last_error = None

    @classmethod
    def _update_config_toggles(cls, toggles: dict[str, bool]) -> dict[str, str]:
        updates = {
            key: ("true" if bool(value) else "false")
            for key, value in toggles.items()
            if key in CONFIG_TOGGLE_KEYS
        }
        if not updates:
            return {}
        path = cls._dotenv_path()
        lines = path.read_text(encoding="utf-8-sig").splitlines() if path.exists() else []
        pending = dict(updates)
        output: list[str] = []
        for line in lines:
            stripped = line.strip()
            replaced = False
            if stripped and not stripped.startswith("#"):
                for key, value in list(pending.items()):
                    if stripped.startswith(f"{key}="):
                        output.append(f"{key}={value}")
                        pending.pop(key)
                        replaced = True
                        break
            if not replaced:
                output.append(line)
        if pending:
            if output and output[-1] != "":
                output.append("")
            for key, value in pending.items():
                output.append(f"{key}={value}")
        path.write_text("\n".join(output) + "\n", encoding="utf-8")
        cls._invalidate_cache()
        cls._ensure_refresh()
        return updates

    @classmethod
    def _reset_cooldowns(cls) -> int:
        path = cls._state_path()
        if not path.exists():
            return 0
        state = json.loads(path.read_text(encoding="utf-8-sig"))
        history = state.get("history", [])
        updated = 0
        reset_cooldown_until = time.time() + cls.reset_cooldown_remaining_seconds
        for event in history:
            if event.get("action") not in {"exit_long", "exit_short"}:
                continue
            event["cooldownUntilOverride"] = reset_cooldown_until
            updated += 1
        path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        cls._invalidate_cache()
        cls._ensure_refresh()
        return updated

    @classmethod
    def _refresh_report(cls) -> None:
        try:
            payload = build_report(cls.workdir, cls.dotenv_file)
            cls.last_report = payload
            cls.last_report_at = time.time()
            cls.last_error = None
            cls._save_cached_report(payload)
        except Exception as exc:
            cls.last_error = str(exc)
        finally:
            with cls.refresh_lock:
                cls.refresh_in_progress = False

    @classmethod
    def _ensure_refresh(cls) -> None:
        with cls.refresh_lock:
            if cls.refresh_in_progress:
                return
            cls.refresh_in_progress = True
        threading.Thread(target=cls._refresh_report, daemon=True).start()

    def _send(
        self,
        body: bytes,
        content_type: str = "text/html; charset=utf-8",
        status: int = 200,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if not self._is_authorized(parsed):
            self._send(
                "Forbidden".encode("utf-8"),
                content_type="text/plain; charset=utf-8",
                status=403,
            )
            return
        if parsed.path == "/":
            self._send(HTML.encode("utf-8"))
            return

        if parsed.path == "/api/health":
            self._send(
                json.dumps({"ok": True}, ensure_ascii=False).encode("utf-8"),
                content_type="application/json; charset=utf-8",
            )
            return

        if parsed.path == "/api/status":
            cls = self.__class__
            now = time.time()
            if cls.last_report is None:
                cls.last_report = cls._load_cached_report()
                if cls.last_report is not None and cls.last_report_at == 0:
                    cls.last_report_at = now

            report_is_fresh = (
                cls.last_report is not None
                and now - cls.last_report_at < cls.cache_ttl_seconds
            )

            if report_is_fresh:
                payload = dict(cls.last_report)
            else:
                cls._ensure_refresh()
                if cls.last_report is not None:
                    payload = dict(cls.last_report)
                    if cls.last_error:
                        payload["dashboardError"] = cls.last_error
                    elif cls.refresh_in_progress:
                        payload["dashboardError"] = "refreshing"
                else:
                    payload = {
                        "source": "unavailable",
                        "summary": {
                            "openPositions": 0,
                            "totalUnrealizedProfit": "0",
                            "totalPositionValueUsdt": "0",
                            "realizedPnlUsdt": "0",
                            "closedCount": 0,
                        },
                        "sideSummaries": {
                            "LONG": {
                                "unrealizedProfit": "0",
                                "openPositions": 0,
                                "currentValueUsdt": "0",
                                "realizedPnlUsdt": "0",
                                "closedCount": 0,
                            },
                            "SHORT": {
                                "unrealizedProfit": "0",
                                "openPositions": 0,
                                "currentValueUsdt": "0",
                                "realizedPnlUsdt": "0",
                                "closedCount": 0,
                            },
                        },
                        "positions": [],
                        "tradeHistory": [],
                        "forceOrderSummary": {"count": 0, "totalLossUsdt": "0", "items": []},
                        "recoveryStats": None,
                        "monitorSummary": None,
                        "unopenedCandidates": [],
                        "strategies": {},
                        "ruleSummary": [],
                        "configToggles": [],
                        "productionReadiness": [],
                        "riskStats": None,
                        "dashboardError": cls.last_error or "refreshing",
                    }

            self._send(
                json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                content_type="application/json; charset=utf-8",
            )
            return

        self._send(b"Not Found", content_type="text/plain; charset=utf-8", status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not self._is_authorized(parsed):
            self._send(
                json.dumps({"ok": False, "error": "Forbidden"}, ensure_ascii=False).encode("utf-8"),
                content_type="application/json; charset=utf-8",
                status=403,
            )
            return
        if parsed.path == "/api/config-toggles":
            try:
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(content_length) if content_length > 0 else b"{}"
                payload = json.loads(body.decode("utf-8") or "{}")
                raw_toggles = payload.get("toggles")
                if not isinstance(raw_toggles, dict):
                    raise ValueError("Invalid toggles payload")
                normalized = {
                    key: bool(value)
                    for key, value in raw_toggles.items()
                    if key in CONFIG_TOGGLE_KEYS
                }
                updated = self.__class__._update_config_toggles(normalized)
                self._send(
                    json.dumps({"ok": True, "updated": updated}, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                )
            except Exception as exc:
                self._send(
                    json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    status=500,
                )
            return
        if parsed.path == "/api/reset-cooldowns":
            try:
                reset_count = self.__class__._reset_cooldowns()
                self._send(
                    json.dumps({"ok": True, "resetCount": reset_count}, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                )
            except Exception as exc:
                self._send(
                    json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    status=500,
                )
            return
        self._send(
            json.dumps({"ok": False, "error": "Not Found"}, ensure_ascii=False).encode("utf-8"),
            content_type="application/json; charset=utf-8",
            status=404,
        )

    def log_message(self, format: str, *args) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Open the local trading dashboard.")
    parser.add_argument("--host", default=os.getenv("DASHBOARD_HOST", "127.0.0.1"), help="Dashboard bind host.")
    parser.add_argument("--port", type=int, default=8787, help="Local dashboard port.")
    parser.add_argument("--dotenv", default=".env", help="Dotenv file path.")
    parser.add_argument(
        "--no-browser", action="store_true", help="Do not auto-open the browser."
    )
    args = parser.parse_args()

    DashboardHandler.workdir = Path.cwd()
    DashboardHandler.dotenv_file = args.dotenv
    DashboardHandler.last_report = DashboardHandler._load_cached_report()
    if DashboardHandler.last_report is not None:
        DashboardHandler.last_report_at = time.time()
    DashboardHandler._ensure_refresh()

    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    browser_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    url = f"http://{browser_host}:{args.port}/"
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    print(f"dashboard running at {url}")
    server.serve_forever()


if __name__ == "__main__":
    main()
