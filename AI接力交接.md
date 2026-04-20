# AI 接力交接

这份文档是当前项目的最新交接说明，优先级高于旧的 `README.md`、`项目上下文.md`、`服务器部署说明.md`。

目标是让下一个 AI 一进来就能快速搞清楚：

- 这套系统现在跑到哪了
- 服务器实际部署在哪里
- 当前真实规则是什么
- 最近已经改了哪些逻辑
- 哪些地方还没做
- 下一步怎么接着做“自动巡检系统”

## 1. 当前部署状态

### 1.1 本地工作目录

- 本地项目目录：`C:\Users\bingc\Desktop\测试模拟`

### 1.2 服务器部署目录

- 服务器项目目录：`/root/ubuntu_server_package`

### 1.3 服务器服务名

- 机器人服务：`ai-select-bot.service`
- 看板服务：`ai-select-dashboard.service`

### 1.4 当前运行环境

- 交易环境：`binance_testnet`
- 不是实盘，是真实下单到 Binance Futures Testnet
- 不是纯本地模拟

### 1.5 当前部署方式

- 不是自动 CI/CD
- 目前是“本地修改 -> 手工替换服务器文件 -> 重启服务”
- 用户通过服务器终端手动执行替换和重启

## 2. 当前策略主逻辑

### 2.1 信号来源

- Binance AI Select 页面
- `Strong Positive` 做多
- `Strong Negative` 做空

### 2.2 持仓主逻辑

- 只要币还在对应强信号列表里，就继续持有
- 掉出列表后，不是立刻平仓
- 当前配置是：连续 `8` 轮都不在榜上才平仓
- 当前轮询间隔是 `10` 秒
- 所以“掉榜平仓”大致是连续 `80` 秒不在榜上才触发

### 2.3 已加的保护逻辑

- `signal_drop_guard`
  - 如果强信号候选数异常骤降，会暂停因掉榜导致的平仓
- `snapshot_protection`
  - 如果当前轮抓取异常，但上一轮快照里还在榜，会先保留仓位
- `preserve_previous_snapshot`
  - 当怀疑本轮抓取结果异常时，不覆盖上一轮有效快照
  - 这是为了解决“取数异常导致整侧仓位被误平”的问题

### 2.4 冷却逻辑

- 当前默认冷却：`10` 分钟
- 配置键：`COOLDOWN_MINUTES=10`
- 页面上的“一键重置冷却”会把当前冷却统一重置成“还剩 10 分钟”

## 3. 当前实际配置

以下是当前本地 `.env` 的非敏感关键配置。

### 3.1 基本交易参数

- `DRY_RUN=false`
- `POLL_INTERVAL_SECONDS=10`
- `AI_SELECT_INTERVAL=1h`
- `USDT_PER_TRADE=500`
- `LEVERAGE=5`
- `REQUIRED_MARGIN_MODE=ISOLATED`
- `BROKER_ADAPTER=binance_testnet`

### 3.2 仓位限制

- `MAX_NEW_POSITIONS_PER_CYCLE=999`
- `MAX_TOTAL_OPEN_POSITIONS=999`
- `MAX_LONG_OPEN_POSITIONS=999`
- `MAX_SHORT_OPEN_POSITIONS=999`

### 3.3 当前启用/关闭的过滤项

- `ENABLE_MIN_SIGNAL_COUNT_FILTER=true`
- `MIN_SIGNAL_COUNT_TO_OPEN=6`
- `ENABLE_MARGIN_USAGE_CAP=false`
- `ENABLE_VOLATILITY_FILTER=false`
- `ENABLE_FUNDING_RATE_FILTER=false`
- `ENABLE_CORRELATION_FILTER=false`
- `ENABLE_TREND_CONFIRMATION=true`
- `ENABLE_TIME_EXIT=false`
- `ENABLE_PROFIT_LOCK=false`
- `ENABLE_PROFIT_PROTECTION=true`
- `ENABLE_SIGNAL_DROP_GUARD=true`
- `SKIP_IF_MARGIN_MODE_UNAVAILABLE=true`

### 3.4 其他关键参数

- `MIN_QUOTE_VOLUME_24H_USDT=5000000`
- `TREND_INTERVAL=4h`
- `TREND_MA_PERIOD=20`
- `TREND_FALLBACK_INTERVALS=1h,15m,5m`
- `MAX_ABS_FUNDING_RATE_PCT=0.10`
- `PROFIT_PROTECTION_ACTIVATE_PCT=20`
- `PROFIT_PROTECTION_TRAIL_PCT=30`
- `SIGNAL_DROP_GUARD_RATIO=0.7`
- `SIGNAL_DROP_GUARD_MIN_CANDIDATES=5`
- `SIGNAL_LOST_EXIT_CONFIRM_ROUNDS=8`

## 4. 已经做完的主要改动

以下内容都已经在当前本地代码里。

### 4.1 抓榜分页修复

文件：

- `fetch_binance_ai_select.py`

修复内容：

- 之前只抓了部分分页，导致“肉眼看到很多强烈看多/看空，但程序只显示几个”
- 现在改成遍历所有分页后统一合并去重

### 4.2 掉榜误平仓修复

文件：

- `ai_select_futures_bot.py`

修复内容：

- 当信号抓取异常、列表突然变空或暴跌时，不再立刻覆盖上一轮快照
- 避免因为几轮异常数据，把整侧仓位都误判成“掉榜”然后全部平掉

### 4.3 趋势过滤已支持新币回退周期

文件：

- `ai_select_futures_bot.py`
- `view_status.py`

当前逻辑：

- 主趋势判断：`4h MA20`
- 如果新币历史不够，自动回退到：
  - `1h`
  - `15m`
  - `5m`

注意：

- 只影响“是否允许新开仓”
- 不影响已持有仓位的止盈止损和平仓

### 4.4 最少强信号数过滤

文件：

- `ai_select_futures_bot.py`
- `view_status.py`
- `dashboard.py`

当前逻辑：

- 如果做多侧 `Strong Positive` 少于 `6` 个，则不新开多仓
- 如果做空侧 `Strong Negative` 少于 `6` 个，则不新开空仓
- 只拦截“新开仓”
- 不影响已有仓位

配置：

- `ENABLE_MIN_SIGNAL_COUNT_FILTER=true`
- `MIN_SIGNAL_COUNT_TO_OPEN=6`

### 4.5 看板已支持页面开关

文件：

- `dashboard.py`
- `view_status.py`

当前页面可以直接开关这些布尔项：

- `DRY_RUN`
- `ENABLE_MIN_SIGNAL_COUNT_FILTER`
- `ENABLE_MARGIN_USAGE_CAP`
- `ENABLE_VOLATILITY_FILTER`
- `ENABLE_FUNDING_RATE_FILTER`
- `ENABLE_CORRELATION_FILTER`
- `ENABLE_TREND_CONFIRMATION`
- `ENABLE_TIME_EXIT`
- `ENABLE_PROFIT_LOCK`
- `ENABLE_PROFIT_PROTECTION`
- `ENABLE_SIGNAL_DROP_GUARD`
- `SKIP_IF_MARGIN_MODE_UNAVAILABLE`

当前限制：

- 页面只能改“开/关”
- 数值项还没有做成页面可编辑
- 例如 `MIN_SIGNAL_COUNT_TO_OPEN=6`、`COOLDOWN_MINUTES=10`、`SIGNAL_LOST_EXIT_CONFIRM_ROUNDS=8` 目前还要改 `.env`

### 4.6 平仓记录与爆仓记录逻辑已调整

文件：

- `view_status.py`
- `dashboard.py`

当前逻辑：

- 页面平仓记录只显示最近 `20` 条
- 平仓记录优先走本地平仓事件
- 爆仓/强平记录用本地缓存 + 增量同步
- 历史接口不再每次全量猛拉，避免 Binance 限流

### 4.7 实时与历史刷新拆分

当前目标逻辑已经基本落地：

- 实时部分：约 `10` 秒
  - 价格
  - AI 信号
  - 当前持仓
  - 是否满足平仓/利润保护等
- 历史部分：约 `60` 秒
  - 强平记录
  - 历史缓存补数

注意：

- 看板整体缓存 TTL 当前在 `dashboard.py` 里是 `10` 秒
- `forceOrders` 相关历史增量同步在 `view_status.py` 里默认 `60` 秒
- 之前因为把官方报表接口刷新调得太快，服务器 IP 被 Binance `418` 封过一次

## 5. 新增的“深跌后回收统计”

文件：

- `ai_select_futures_bot.py`
- `view_status.py`
- `dashboard.py`

### 5.1 这项统计是干什么的

用户没有设止损，想知道：

- 一笔单在持仓过程中跌得很深
- 但最后有没有“熬回来”
- 这种概率是多少
- 最深能扛回来的是哪一单

### 5.2 当前统计口径

从这次更新开始，每笔持仓会持续记录：

- `maxProfitPct`
- `minPnlPct`
- `lastPnlPct`

平仓时会把这些轨迹一起写进历史。

看板上的“深跌后回收统计”定义如下：

- `曾跌入亏损`
  - `minPnlPct < 0`
- `深跌后最终盈利`
  - `minPnlPct < 0` 且最终 `netRealizedPnlUsdt > 0`
- `水下后回正概率`
  - 深跌后最终盈利 / 曾跌入亏损

### 5.3 当前局限

- 旧历史单没有完整记录 `minPnlPct`
- 所以这项统计从“本次改动之后”开始会越来越准
- 不能完整回补所有旧单

### 5.4 已修的显示 bug

之前页面有个 bug：

- 即使某一单从头到尾都没跌进亏损
- 也可能被错误显示成“全样本最深浮亏”

这个 bug 已经修掉：

- 现在“全样本最深浮亏”只统计真正 `< 0%` 的样本

## 6. 当前页面已经具备的能力

页面主要包括：

- 总体统计
- 多空策略状态
- 当前持仓
- 未开仓原因
- 当前冷却
- 基本规则
- 策略开关
- 平仓记录（最近 20 条）
- 爆仓 / 强平记录
- 深跌后回收统计
- 实盘准入清单
- 风险统计

## 7. 当前仍然存在的限制 / 注意点

### 7.1 当前还是手工部署

- 本地改代码后，需要手工替换服务器文件
- 替换后通常要：
  - `systemctl restart ai-select-bot.service`
  - `systemctl restart ai-select-dashboard.service`

### 7.2 页面开关不是全量配置中心

当前页面只支持布尔开关。

还没做成页面可编辑的数值项包括但不限于：

- `COOLDOWN_MINUTES`
- `MIN_SIGNAL_COUNT_TO_OPEN`
- `SIGNAL_LOST_EXIT_CONFIRM_ROUNDS`
- `MAX_ABS_FUNDING_RATE_PCT`
- `PROFIT_PROTECTION_ACTIVATE_PCT`
- `PROFIT_PROTECTION_TRAIL_PCT`

### 7.3 本地运行验证能力不足

当前这个本地线程环境里：

- `python` 不可用
- `git` 不可用

所以很多改动是代码级修改和静态检查，没法在本地真实跑服务验证。

### 7.4 旧文档很多内容已经过期

尤其旧文档里这些信息很多已经不对：

- 冷却还是 `5` 小时
- 轮询还是 `900` 秒
- 单笔 `100 USDT`
- 趋势过滤默认关闭
- 各种旧风险参数

后续不要再把旧文档当成“当前真实配置”。

## 8. 下一步要做的事：自动巡检系统

这是当前最推荐的下一阶段目标。

当前状态补充：

- 巡检系统第一版代码已经在本地完成
- 新增文件：`monitor.py`
- 看板已经预留并接入“系统巡检”卡片
- `install_ubuntu_services.sh` 和 `manage_ubuntu_services.sh` 已加入 `ai-select-monitor.service`
- 服务器端大概率还没有完成这部分替换和启用，所以当前线上未必已经在跑

### 8.1 为什么要做

现在的问题是：

- 很多 bug 不是肉眼看页面就能及时发现
- 用户只能靠手工观察
- 即使页面有统计，也未必能立刻看出“规则和真实行为是否一致”

### 8.2 推荐架构

不要直接让 AI 24/7 高频盯盘下判断。

推荐两层结构：

#### 第一层：规则巡检器

新增一个本地巡检程序，例如：

- `monitor.py`

它按固定规则检查系统有没有偏离预期。

优先检查这些：

- `强烈看多/看空 < 6` 时却仍然开仓
- 掉榜确认轮数未到却提前平仓
- 冷却期内重复开仓
- 当前持仓数超过上限
- 本地状态与 Binance 实际持仓不一致
- 页面显示与本地状态不一致
- 信号快照异常骤降但仍然触发大面积平仓
- 看板数据长时间不刷新
- 历史缓存异常增长
- Binance API 异常频率过高

巡检器输出：

- `runtime/monitor_events.jsonl`
- `runtime/monitor_summary.json`

#### 第二层：AI 诊断器

AI 不做高频监控，而是读取巡检结果和日志，总结：

- 哪条规则被违反了
- 连续发生了几次
- 影响了哪些币
- 最像是哪段逻辑出问题

#### 第三层：告警

后续再接：

- Telegram
- 企业微信
- 邮件

先做“发现异常 -> 落文件 -> 页面展示”，再接外部告警。

### 8.3 推荐实现顺序

1. 已完成 `monitor.py`
2. 已完成 `systemd` 服务脚本改造
3. 已完成页面“系统巡检”卡片接入
4. 下一步再接 AI 诊断与通知

## 9. 推荐给下一个 AI 的第一任务

如果下一个 AI 接手，建议优先做下面这件事：

### 第一优先级

把本地已经完成的巡检系统第一版真正部署到服务器并跑起来

当前产物已经有：

- `monitor.py`
- `runtime/monitor_events.jsonl`
- `runtime/monitor_summary.json`
- `ai-select-monitor.service` 对应的安装/管理脚本改造
- dashboard 页面“系统巡检”区域

### 巡检第一版建议至少覆盖

- 最少强信号数过滤是否被违反
- 冷却规则是否被违反
- 掉榜确认轮数是否被违反
- 本地状态与 Binance 持仓是否一致
- 当前页面展示与本地状态是否一致
- 最近 5 分钟是否出现异常批量平仓

## 10. 当前部署 / 替换的常用命令

### 10.1 看项目目录

```bash
systemctl show -p WorkingDirectory -p ExecStart ai-select-bot.service
systemctl show -p WorkingDirectory -p ExecStart ai-select-dashboard.service
```

### 10.2 重启服务

```bash
cd /root/ubuntu_server_package
systemctl restart ai-select-bot.service
systemctl restart ai-select-dashboard.service
```

### 10.3 清理看板缓存

```bash
cd /root/ubuntu_server_package
rm -f runtime/dashboard_cache.json
```

### 10.4 看日志

```bash
journalctl -u ai-select-bot.service -n 100 --no-pager
journalctl -u ai-select-dashboard.service -n 100 --no-pager
```

## 11. 当前建议优先看的文件

- `ai_select_futures_bot.py`
- `view_status.py`
- `dashboard.py`
- `.env`
- `fetch_binance_ai_select.py`
- `runtime/state.json`
- `runtime/strategy_statuses.json`
- `runtime/dashboard_cache.json`
- `runtime/history_cache.json`
- `runtime/bot.log`

## 12. 结论

当前项目已经不是“从零开始”的状态，而是：

- 双向强信号轮动策略已跑起来
- Testnet 下单已接通
- 页面已能看当前状态、历史、风险和部分配置
- 关键误平仓问题已经补过保护
- 深跌回收统计已经接入，但样本需要继续积累
- 下一步最该做的是“规则巡检系统”，而不是继续纯靠人眼盯页面
