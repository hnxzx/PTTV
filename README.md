# PTTV · 直播 / 点播聚合订阅

> 融合「影视仓聚合」与「Collect-IPTV」优点。每 4 小时自动采集 → 测速 → 排序 → 生成 → commit 回仓库。

## 核心特性

| # | 特性 | 实现 |
|---|---|---|
| 1 | **央视严格 CCTV-1 → CCTV-N 数字排序** | 排序键 `rank_cctv`，数字升序；`5+` 排第 5；带日期后缀异常频道剔除 |
| 2 | **多通道备选 + 5 秒自动切换** | 每频道保留最多 5 条可用 URL，按测速 ms 升序，最优在前；网页播放器通道 1 失败 5 秒未出画面 → 自动切通道 2 → 通道 3… |
| 3 | **固定时间自动采集/测试/排序/生成/回仓库** | GitHub Actions 每 4 小时定时触发，commit 回 `main` |
| 4 | **直播 + 点播双类型** | 直播 `live.txt` / `live.json`；点播多仓 `cang.json`；网页双 tab 切换 |

## 使用方式

- **网页播放**：访问 GitHub Pages 托管的 `index.html`
- **影视仓订阅**（直播地址）：`https://<你的仓库域名>/web/live.txt`
- **影视仓多仓**（点播地址）：`https://<你的仓库域名>/web/cang.json`

## 输出文件

| 文件 | 格式 | 用途 |
|---|---|---|
| `web/live.txt` | `台名,URL` 多行（影视仓格式） | 同频道多通道多行，最优在前 |
| `web/live.json` | `[{num,name,group,channels:[{url,ms}]}]` | 网页播放器读取 |
| `web/cang.json` | `{urls:[{name,url,ms}]}` | 点播多仓 |
| `out/state.json` | 运行状态 | 频道数/测速/上游源状态 |

## 采集器说明

- **位置**：`collector/collector.py`
- **引擎**：Python 3.11+ + `aiohttp`（并发抓取）
- **复用**：老薛现有 `pool_live.json` / `pool_cang.json` / 双探针 `live_verdicts.json` / `cang_verdicts.json` 作为冷启动种子
- **两级深度探测**：清单 → 分片，杜绝"清单还在但分片失效"的死链
- **原子写**：临时文件 → `fsync` → `os.replace`，避免半截文件
- **上游大面积失败**：保留旧缓存，不输出空仓

## 维护

- 修改上游源：编辑 `collector/collector.py` 里的 `LIVE_SRC` / `CANG_SRC` 列表
- 手动触发：Actions → `PT-IPTV Auto Update` → `Run workflow`
- 频道数骤降保护：新频道数 < 旧频道数 40% 时自动保留旧缓存

## 免责

所有频道源均来自公开网络源，本服务仅做聚合与连通性检测，内容版权归源站所有。上游源可能含广告/赞助/扫码提示，与本项目无关。
