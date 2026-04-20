# Ubuntu 服务器部署

这台服务器是 Ubuntu，建议用 `systemd` 跑两个常驻服务：

- `ai-select-bot.service`
- `ai-select-dashboard.service`

## 1. 上传项目

把整个目录上传到服务器，比如：

```bash
/root/CryptoRadar-root/测试模拟
```

## 2. 安装环境

```bash
apt update
apt install -y python3 python3-venv python3-pip
cd /root/CryptoRadar-root/测试模拟
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install --with-deps chromium
```

## 3. 检查配置

确认这些参数已经填好：

- `BROKER_ADAPTER=binance_testnet`
- `DRY_RUN=false`
- `BINANCE_API_KEY=...`
- `BINANCE_API_SECRET=...`

可选：

- `DASHBOARD_ACCESS_TOKEN=你自己设置的口令`

如果设置了这个口令，访问地址要带上：

```text
http://服务器IP:8787/?token=你的口令
```

## 4. 安装 systemd 自启动

```bash
cd /root/CryptoRadar-root/测试模拟
bash ./install_ubuntu_services.sh /root/CryptoRadar-root/测试模拟 root 8787
```

## 5. 放行端口

如果服务器开了防火墙：

```bash
ufw allow 8787/tcp
```

如果你用云服务器安全组，也要在安全组里放行 `8787`。

## 6. 常用命令

```bash
cd /root/CryptoRadar-root/测试模拟
bash ./manage_ubuntu_services.sh status
bash ./manage_ubuntu_services.sh restart
bash ./manage_ubuntu_services.sh logs
```

## 7. 访问地址

```text
http://服务器IP:8787/
```

如果设置了口令：

```text
http://服务器IP:8787/?token=你的口令
```
