#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PT-IPTV 采集器（融合「影视仓聚合」与「Collect-IPTV」优点）
=========================================================
能力（对应老薛 4 大改善点）：
  1. 按频道排序，央视严格 CCTV-1 → CCTV-N 数字升序
  2. 多通道备选：每频道保留多个可用 URL，按测速 ms 升序，最优在前；
     网页播放器 5 秒内自动 1→2→3 切换
  3. 固定时间自动采集 / 自动测速 / 自动排序 / 生成连接 / commit 回仓库
     （由 GitHub Actions 定时任务驱动本脚本）
  4. 直播 + 点播（多仓）双类型，独立订阅入口

设计原则（继承两个前身）：
  - 多源抓取（aiohttp 并发） + 主机分组 + 两级深度探测（清单→分片，杜绝死链）
  - 复用老薛现有 pool_live.json / pool_cang.json / 双探针 verdicts 作为冷启动种子
  - 原子写（临时文件 → fsync → rename）
  - 上游大面积失败时保留旧缓存，不输出空仓

输出文件（写到 out/，由 Actions 复制/commit 回仓库）：
  - live.txt   影视仓文本格式（台名,URL 多行，同频道多通道多行，最优在前）
  - live.json  网页播放器读取的结构化数据（含多通道数组、排序号、分组）
  - cang.json  点播多仓（urls: [{name,url,ms}]，按实测 ms 升序）
  - state.json 运行状态
"""

import asyncio
import json
import os
import re
import time
import hashlib
import urllib.parse
import contextlib
from datetime import datetime, timezone, timedelta

import aiohttp
from aiohttp import ClientTimeout, TCPConnector

# ============================================================
# 路径与配置
# ============================================================
BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED = os.path.join(BASE, "seed")
OUT  = os.path.join(BASE, "out")
os.makedirs(OUT, exist_ok=True)

CST = timezone(timedelta(hours=8))

CONFIG = {
    "max_parallel": 30,        # 并发上限
    "timeout": 10,            # 单请求总超时(秒)
    "connect_timeout": 5,     # 连接超时
    "max_channels_per_chan": 5,  # 每频道最多保留几条通道（含最优）
    "fresh_days": 3,          # 新鲜度闸门：仅输出 N 天内实测通过
    "deep_normal": 7 * 86400, # 普通线路深测周期
    "deep_token": 12 * 3600,  # 带令牌 URL 深测周期
    "src_refresh": 12 * 3600, # 上游抓取周期
    "verdict_ttl": 36 * 3600, # 国内探针 verdicts 有效期
}

# 上游直播源（原始地址，抓取自动走国内镜像）
LIVE_SRC = [
    "https://raw.githubusercontent.com/DataShare-duo/MovieLiveUrl/refs/heads/main/movie_live.txt",
    "https://raw.githubusercontent.com/yoursmile66/TVBox/main/live.txt",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.m3u",
    "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "https://raw.githubusercontent.com/BurningC4/Chinese-IPTV/master/TV-IPV4.m3u",
]
MIRRORS = ["https://ghproxy.net/", "https://gh-proxy.com/", ""]

# 点播多仓上游
CANG_SRC = [
    "https://jihulab.com/ygbh1/box/-/raw/main/dcang/dc.json",
    "http://ztha.top/TVBox/GYCK.json",
    "http://xhztv.top/DC.txt",
    "http://xmbjm.fh4u.org/dc.txt",
]
# 保底线路（首次引导为可用，随后按周期复检）
FALLBACK = [
    {"n": "精选线路一〔保底〕", "u": "https://9280.kstore.vip/newwex.json"},
    {"n": "精选线路二〔保底〕", "u": "https://9280.kstore.space/wex.json"},
    {"n": "精选线路三〔保底〕", "u": "https://dxawi.github.io/0/0.json"},
]

# ============================================================
# 日志
# ============================================================
_log_path = os.path.join(OUT, "collector.log")
def wlog(m):
    line = "[%s] %s\n" % (datetime.now(CST).strftime("%m-%d %H:%M:%S"), m)
    with open(_log_path, "a", encoding="utf-8") as f:
        f.write(line)
    print(line, end="")

# ============================================================
# 原子写
# ============================================================
def atomic_put(path, content, is_json=False):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        if is_json:
            json.dump(content, f, ensure_ascii=False, indent=2)
        else:
            f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

# ============================================================
# 工具函数
# ============================================================
def norm_name(n):
    n = (n or "").strip()
    n = re.sub(r"^CCTV[\s\-]*(\d+)", r"CCTV-\1", n, flags=re.I)
    return n.strip()

def is_cctv(n):
    return bool(re.search(r"CCTV|CETV", n, re.I))

def is_sat(n):
    return "卫视" in n

def rank_cctv(name):
    """央视排序键：CCTV-N 按 N 升序；CETV→100；5+→6；其余→90"""
    m = re.search(r"CCTV-?(\d+)", name, re.I)
    if m:
        return int(m.group(1))
    if "5+" in name:
        return 6
    if "CETV" in name:
        return 100
    return 90

def cang_key(u):
    p = urllib.parse.urlparse(u)
    if not p or not p.netloc:
        return u.strip().lower()
    host = p.hostname.lower()
    port = p.port
    if port and port not in (80, 443):
        host += ":%d" % port
    path = (p.path or "").rstrip("/")
    if path == "":
        path = "/"
    k = host + path
    if p.query:
        k += "?" + p.query
    return k

def deep_ttl(u):
    return CONFIG["deep_token"] if re.search(
        r"[?&](_upt|txSecret|txTime|token|sign|signature|auth|exp|expire|key)=", u, re.I
    ) else CONFIG["deep_normal"]

# 主机分组测试键
def group_key(u, host_count):
    p = urllib.parse.urlparse(u)
    host = p.hostname or ""
    port = p.port or (443 if p.scheme == "https" else 80)
    if re.match(r"^\d+\.\d+\.\d+\.\d+$", host):
        return "IP|%s:%s" % (host, port)
    if re.search(r"aliyun|myali|kwimgs|qcloud|tencent|myqcloud|cntv|cctv\.com|hntv|cztv|chinanet|unicom|cucc|189\.cn|migu|wasu|gtimg|bdstatic|hdslb|ksyun|wscdns", host, re.I):
        return "U|%s" % u
    segs = (p.path or "/").strip("/").split("/")
    d1 = segs[0] if segs and segs[0] else ""
    if host_count.get(host, 0) >= 3:
        return "G|%s:%s/%s" % (host, port, d1)
    return "U|%s" % u

# ============================================================
# HTTP 抓取（带镜像故障转移）
# ============================================================
async def http_get(session, url, timeout=12):
    try:
        async with session.get(url, timeout=ClientTimeout(total=timeout)) as r:
            if r.status == 200:
                return await r.text()
    except Exception:
        pass
    return ""

async def fetch_src(session, url, timeout=7):
    """GitHub 地址依次走国内镜像，最后直连。"""
    if "githubusercontent.com" in url or "github.com" in url:
        for px in MIRRORS:
            if left_budget() < 5:
                return ""
            r = await http_get(session, px + url, timeout)
            if r and "404: Not Found" not in r and "just a moment" not in r.lower() \
               and "<html" not in r[:200].lower() and len(r) > 200:
                return r
        return ""
    return await http_get(session, url, timeout)

# 简易时间预算（单次运行 200 秒，留足给 Actions 的 600 秒 timeout）
_T0 = time.time()
_BUDGET = 200
def left_budget():
    return _BUDGET - (time.time() - _T0)

# ============================================================
# 解析直播源
# ============================================================
def parse_live(raw):
    """解析 M3U / txt，返回 [(name, url), ...]"""
    out = []
    raw = raw.lstrip("\ufeff")
    if "#EXTM3U" in raw or "#EXTINF" in raw:
        pending = ""
        for ln in raw.splitlines():
            ln = ln.strip()
            if ln.startswith("#EXTINF"):
                name = ""
                m = re.search(r'tvg-name="([^"]*)"', ln, re.I)
                if m and m.group(1):
                    name = m.group(1)
                if "," in ln:
                    name = ln.rsplit(",", 1)[-1].strip() or name
                pending = norm_name(name)
            elif ln and not ln.startswith("#") and ln.lower().startswith("http"):
                u = ln.split()[0]
                # 非法字符过滤（与 PHP 一致）
                if re.search(r"[\x80-\xff\"'<>\\{}|^\`[]", u):
                    pending = ""
                    continue
                if pending:
                    out.append((pending, u))
                pending = ""
    else:
        for ln in raw.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#") or "#genre#" in ln:
                continue
            p = ln.find(",")
            if p == -1:
                continue
            n = norm_name(ln[:p])
            u = ln[p + 1:].strip().split()[0]
            if re.search(r"[\x80-\xff\"'<>\\{}|^\`[]", u):
                continue
            if n and u.lower().startswith("http"):
                out.append((n, u))
    return out

# ============================================================
# 两级深度探测（清单→分片）
# ============================================================
def resolve_rel(base, ref):
    ref = ref.split()[0]
    if ref.startswith("http"):
        return ref
    p = urllib.parse.urlparse(base)
    host = p.netloc
    path = p.path or "/"
    d = path[:path.rfind("/") + 1]
    if ref.startswith("//"):
        return p.scheme + ":" + ref
    if ref.startswith("/"):
        return p.scheme + "://" + host + ref
    return p.scheme + "://" + host + d + ref

async def probe_head(session, semaphore, url, timeout=3, referer="", rng="0-2047"):
    """一级探测：抓前 2KB，返回 dict(ok, ms, code, ct, body, eff)"""
    res = {"ok": 0, "ms": 0, "code": 0, "ct": "", "body": "", "eff": url}
    t0 = time.time()
    hdrs = {}
    if referer:
        hdrs["Referer"] = referer
    if rng:
        hdrs["Range"] = rng
    try:
        async with semaphore:
            async with session.get(
                url,
                timeout=ClientTimeout(total=timeout),
                allow_redirects=True,
                headers=hdrs,
            ) as r:
                res["code"] = r.status
                res["ct"] = r.headers.get("Content-Type", "")
                res["eff"] = str(r.url)
                if r.status >= 400 or r.status == 0:
                    return res
                if "text/html" in res["ct"].lower() or "application/json" in res["ct"].lower():
                    return res
                body = await r.text()
                bad = ["<html", "<!doctype", "just a moment", '"error"', "404 not found", "no such"]
                if any(b in body[:2000].lower() for b in bad):
                    return res
                res["ok"] = 1
                res["ms"] = max(1, int(round((time.time() - t0) * 1000)))
                res["body"] = body
    except asyncio.TimeoutError:
        pass
    except Exception:
        pass
    return res

async def probe_deep(session, semaphore, head, timeout=4):
    """二级探测：HLS 主清单→子清单→真实分片。返回 1=可播 0=死链 -1=时间不足"""
    if not head["ok"]:
        return 0
    body, eff = head["body"], head["eff"]
    if "#EXTM3U" not in body and "#EXTINF" not in body:
        return 1  # 非 HLS 直连流，一级过即算
    lines = body.splitlines()
    is_master = any(l.startswith("#EXT-X-STREAM-INF") for l in lines)
    if is_master:
        for ln in lines:
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            if left_budget() < 10:
                return -1
            sub = await probe_head(session, semaphore, resolve_rel(eff, ln), timeout)
            if not sub["ok"]:
                return 0
            body, eff = sub["body"], sub["eff"]
            break
    for ln in body.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        if left_budget() < 8:
            return -1
        seg_url = resolve_rel(eff, ln)
        seg = await probe_head(session, semaphore, seg_url, 3)
        if not seg["ok"] and left_budget() >= 8:
            seg = await probe_head(session, semaphore, seg_url, 3, referer=eff)
        if not seg["ok"] and left_budget() >= 8:
            seg = await probe_head(session, semaphore, seg_url, 3, referer=eff, rng="")
        return 1 if seg["ok"] else 0
    return 1

# ============================================================
# 主流程
# ============================================================
# 鲁棒 JSON 解析（去 BOM、去 // 与 /* 注释行、截取首个 { 到末个 }，尾部杂数据可容忍）
def json_loose(raw):
    raw = re.sub(r"^\xEF\xBB\xBF", "", (raw or "").lstrip("\ufeff"))
    keep = []
    for l in raw.splitlines():
        t = l.strip()
        if t in ("",) or t.startswith("//") or t.startswith("/*") or t.startswith("*"):
            continue
        keep.append(l)
    raw = "\n".join(keep)
    a = raw.find("{")
    b = raw.rfind("}")
    if a == -1 or b == -1 or b <= a:
        return None
    with contextlib.suppress(Exception):
        return json.loads(raw[a:b + 1])
    return None

async def main():
    global _T0
    _T0 = time.time()
    now = int(time.time())
    wlog("======== PT-IPTV 采集开始 ========")

    conn = TCPConnector(limit=CONFIG["max_parallel"] * 2, ssl=False)
    sem = asyncio.Semaphore(CONFIG["max_parallel"])
    async with aiohttp.ClientSession(connector=conn) as session:

        # ---------- 加载种子池 ----------
        pool_live = {"built": now, "channels": {}}
        pl_path = os.path.join(SEED, "pool_live.json")
        if os.path.exists(pl_path):
            old = json.load(open(pl_path, encoding="utf-8"))
            if isinstance(old, dict) and old.get("channels"):
                pool_live = old
        pool_cang = {"built": now, "sites": []}
        pc_path = os.path.join(SEED, "pool_cang.json")
        if os.path.exists(pc_path):
            oldc = json.load(open(pc_path, encoding="utf-8"))
            if isinstance(oldc, dict) and oldc.get("sites"):
                pool_cang = oldc

        # ---------- 加载国内探针 verdicts（36h 内有效） ----------
        lv_ok, c_ok = {}, {}
        lvf = os.path.join(SEED, "live_verdicts.json")
        if os.path.exists(lvf):
            d = json.load(open(lvf, encoding="utf-8"))
            if isinstance(d, dict) and d.get("built") and now - int(d["built"]) < CONFIG["verdict_ttl"]:
                lv_ok = d.get("ok", {})
        cvf = os.path.join(SEED, "cang_verdicts.json")
        if os.path.exists(cvf):
            d = json.load(open(cvf, encoding="utf-8"))
            if isinstance(d, dict) and d.get("built") and now - int(d["built"]) < CONFIG["verdict_ttl"]:
                c_ok = d.get("ok", {})
        wlog("国内探针加载：直播分组 %d，多仓 %d" % (len(lv_ok), len(c_ok)))

        # ---------- 一、直播候选池：抓取上游 ----------
        # PHP 时钟漂移修复：last_src 与本机 now 偏差超 10h 视为脏数据，强制重新抓取
        last_src = int(pool_live.get("last_src", 0))
        drift = abs(last_src - now)
        do_src = not last_src or drift > 10 * 3600 or (now - last_src) > CONFIG["src_refresh"]
        src_ok = -1
        seen_urls = set()
        if do_src and left_budget() > 40:
            for src in LIVE_SRC:
                if left_budget() < 24:
                    break
                raw = await fetch_src(session, src)
                if not raw or "just a moment" in raw.lower():
                    continue
                rows = parse_live(raw)
                if not rows:
                    continue
                src_ok = max(1, src_ok + 1)
                for n, u in rows:
                    if len(n) > 40:
                        continue
                    seen_urls.add(u)
                    ch = pool_live["channels"].setdefault(n, [])
                    if not any(e["u"] == u for e in ch):
                        ch.append({"u": u, "s": -1, "t": 0, "ms": 0, "d": 0, "gone": 0})
            pool_live["last_src"] = now
            pool_live["last_src_ok"] = max(0, src_ok)
            wlog("直播上游抓取：可用 %d/%d 源，新入池 URL %d" % (max(0, src_ok), len(LIVE_SRC), len(seen_urls)))
        else:
            wlog("跳过直播上游抓取（12h 内已抓 / 预算不足），全力测速")

        # 消失节点判定
        if do_src:
            for n, es in pool_live["channels"].items():
                keep = []
                for e in es:
                    if not e.get("warm") and e["u"] not in seen_urls:
                        if not e.get("gone"):
                            e["gone"] = now
                        elif now - e["gone"] > 7 * 86400:
                            continue  # 清理
                    keep.append(e)
                pool_live["channels"][n] = keep

        # ---------- 二、多仓候选池：抓取 ----------
        last_csrc = int(pool_cang.get("last_src", 0))
        cang_drift = abs(last_csrc - now)
        do_cang = not last_csrc or cang_drift > 10 * 3600 or (now - last_csrc) > CONFIG["src_refresh"]
        cs = {}
        for f in FALLBACK:
            ck = cang_key(f["u"])
            if ck not in cs:
                cs[ck] = {"n": f["n"], "u": f["u"], "fb": 1}
        if do_cang and left_budget() > 30:
            for csrc in CANG_SRC:
                if left_budget() < 24:
                    break
                raw = await fetch_src(session, csrc)
                if not raw:
                    continue
                j = json_loose(raw)
                if isinstance(j, dict) and isinstance(j.get("urls"), list):
                    for it in j["urls"]:
                        if not it.get("url"):
                            continue
                        uu = it["url"].strip()
                        nn = it.get("name", "未命名线路").strip()
                        ck = cang_key(uu)
                        if ck not in cs:
                            cs[ck] = {"n": nn, "u": uu, "fb": 0}
            pool_cang["last_src"] = now
        # 继承旧多仓
        for old in pool_cang.get("sites", []):
            ck = cang_key(old["u"])
            if ck not in cs:
                cs[ck] = {"n": old.get("n", "未命名"), "u": old["u"], "fb": 1}
        cang_cands = []
        for ck, e in cs.items():
            e["s"] = -1; e["t"] = 0; e["cnt"] = 0
            for old in pool_cang.get("sites", []):
                if old["u"] == e["u"] or cang_key(old["u"]) == ck:
                    e["s"], e["t"], e["cnt"] = old.get("s", -1), old.get("t", 0), old.get("cnt", 0)
                    if e["u"].startswith("http://") and old["u"].startswith("https://"):
                        e["u"] = old["u"]
                    break
            if e["fb"] and e["s"] == -1:
                e["s"] = 1  # 保底引导
            if ck in c_ok:
                cv = c_ok[ck]
                e["s"] = 1
                e["t"] = cv.get("t", now)
                e["cnt"] = cv.get("cnt", 0)
                if e["u"].startswith("http://") and cv.get("u", "").startswith("https://"):
                    e["u"] = cv["u"]
            cang_cands.append(e)

        # ---------- 三、直播测速 + 两级深测 ----------
        host_count = {}
        for n, es in pool_live["channels"].items():
            for e in es:
                h = urllib.parse.urlparse(e["u"]).hostname
                if h:
                    host_count[h] = host_count.get(h, 0) + 1

        members, key_url = {}, {}
        for n, es in pool_live["channels"].items():
            for i, e in enumerate(es):
                gk = group_key(e["u"], host_count)
                members.setdefault(gk, []).append((n, i))
                if gk not in key_url:
                    key_url[gk] = e["u"]

        # 国内探针整组升级
        if lv_ok:
            for gk, mem in members.items():
                if gk not in lv_ok:
                    continue
                for n, i in mem:
                    pool_live["channels"][n][i]["s"] = 1
                    pool_live["channels"][n][i]["t"] = now
                    pool_live["channels"][n][i]["d"] = now
                    pool_live["channels"][n][i]["ms"] = lv_ok[gk].get("ms", 0)
                    pool_live["channels"][n][i]["lv"] = 1
            wlog("直播国内探针合并 %d 组" % len([g for g in members if g in lv_ok]))

        # 每频道新鲜线路数
        fresh = {}
        for n, es in pool_live["channels"].items():
            c = sum(1 for e in es
                    if e["s"] == 1 and e["t"] > 0 and now - e["t"] <= CONFIG["fresh_days"] * 86400
                    and e.get("d") and now - e["d"] <= deep_ttl(e["u"]))
            fresh[n] = c

        # 构建测试队列（优先级：零新鲜>复检，央视>特色>普通）
        q_zp, q_zm, q_zr, q_re = [], [], [], []
        for gk, mem in members.items():
            need, deep_due, zero = False, False, False
            prio = 0
            for n, i in mem:
                e = pool_live["channels"][n][i]
                if e["s"] == 1:
                    if e["t"] == 0 or now - e["t"] > CONFIG["deep_normal"]:
                        need = True
                    if not e.get("d") or now - e["d"] > deep_ttl(e["u"]):
                        deep_due = True
                elif e["s"] == 0:
                    if now - e["t"] > 7 * 86400:
                        need = True
                else:
                    if fresh[n] < 2:
                        need = True
                if fresh[n] == 0:
                    zero = True
                if is_cctv(n) or is_sat(n):
                    prio = max(prio, 1)
                elif re.search(r"NewTV|咪咕|CGTN|体育|电影|剧场|轮播|新闻|高清|综艺|纪实|财经|影视|地方|国际|音乐|动画", n):
                    prio = max(prio, 2)
                else:
                    prio = max(prio, 3)
            if not need and not deep_due:
                continue
            if zero and need:
                (q_zp if prio == 1 else q_zm if prio == 2 else q_zr).append(gk)
            elif need or deep_due:
                q_re.append(gk)
        import random
        for q in (q_zp, q_zm, q_zr, q_re):
            random.shuffle(q)
        queue = q_zp + q_re + q_zm + q_zr

        checked, deep_done = 0, 0
        while left_budget() > 20 and queue:
            gk = queue.pop(0)
            if gk not in members:
                continue
            rep = await probe_head(session, sem, key_url[gk], 3)
            need_deep = False
            for dn, di in members[gk]:
                de = pool_live["channels"][dn][di]
                if de["s"] != 1 or not de.get("d") or now - de["d"] > deep_ttl(de["u"]):
                    need_deep = True
                    break
            deep_ran, incon = False, False
            if rep["ok"] and need_deep and left_budget() > 22:
                dv = await probe_deep(session, sem, rep, 4)
                if dv == 1:
                    deep_ran = True
                    deep_done += 1
                elif dv == 0:
                    rep["ok"] = 0
                else:
                    incon = True
            is_hls = rep["ok"] and ("#EXTM3U" in rep["body"] or "#EXTINF" in rep["body"])
            for n, i in members[gk]:
                e = pool_live["channels"][n][i]
                if incon:
                    if e["s"] == 1:
                        e["t"] = now
                else:
                    e["s"] = 1 if rep["ok"] else 0
                    e["t"] = now
                    if rep["ok"]:
                        e["ms"] = rep["ms"]
                        if deep_ran or not is_hls:
                            e["d"] = now
            checked += 1

        atomic_put(os.path.join(SEED, "pool_live.json"), pool_live, is_json=True)
        wlog("直播测速：检查 %d 分组，深测 %d" % (checked, deep_done))

        # ---------- 四、多仓测速（抽样 3 条） ----------
        todo = [e for e in cang_cands if e["s"] == -1 or (e["s"] == 1 and now - e["t"] > 7 * 86400) or (e["s"] == 0 and now - e["t"] > 10 * 86400)]
        import random as _r
        _r.shuffle(todo)
        todo = todo[:3]
        todo_u = set(e["u"] for e in todo)
        pool_cang["sites"] = []
        for e in cang_cands:
            if e["u"] in todo_u and left_budget() > 15:
                body = await http_get(session, e["u"], 3)
                j = None
                try:
                    j = json.loads(body) if body else None
                except Exception:
                    pass
                if isinstance(j, dict) and isinstance(j.get("sites"), list) and len(j["sites"]) >= 3:
                    e["s"] = 1; e["t"] = now; e["cnt"] = len(j["sites"])
                else:
                    e["s"] = 0; e["t"] = now
            pool_cang["sites"].append(e)
        atomic_put(os.path.join(SEED, "pool_cang.json"), pool_cang, is_json=True)
        wlog("多仓测速完成，候选 %d" % len(cang_cands))

        # ============================================================
        # 生成订阅输出
        # ============================================================
        # --- 直播 live.txt + live.json ---
        cctv, sat, oth = [], [], []
        live_json = []   # [{num,name,group,channels:[{url,ms}]}]
        tot_ok = tot_nodes = 0
        for n, es in pool_live["channels"].items():
            # 剔除带日期后缀的异常频道名（如 CCTV-20241 / 历年春晚误标）
            if re.search(r"CCTV-?(\d{3,})", n, re.I):
                continue
            valids = []
            for e in es:
                tot_nodes += 1
                if e["s"] == 1 and e["t"] > 0 and now - e["t"] <= CONFIG["fresh_days"] * 86400 \
                   and e.get("d") and now - e["d"] <= deep_ttl(e["u"]) \
                   and not re.search(r"[\x80-\xff\"'<>\\{}|^\`[]", e["u"]):
                    valids.append(e)
            if not valids:
                continue
            # 测速升序 + 同 ms 时 HTTPS 优先 + URL 更短优先
            valids.sort(key=lambda e: (e.get("ms") or 999999,
                                       0 if e["u"].startswith("https") else 1,
                                       len(e["u"])))
            pick = valids[:CONFIG["max_channels_per_chan"]]
            if is_cctv(n):
                cctv.append(n); tot_ok += 1
            elif is_sat(n):
                sat.append(n); tot_ok += 1
            else:
                oth.append(n); tot_ok += 1
            grp = "央视频道" if is_cctv(n) else "卫视频道" if is_sat(n) else "地方及特色"
            live_json.append({
                "name": n,
                "group": grp,
                "channels": [{"url": e["u"], "ms": e.get("ms", 0)} for e in pick],
            })

        # 央视组内部按数字排序
        cctv.sort(key=rank_cctv)
        sat.sort()
        oth.sort()
        live_json.sort(key=lambda x: (
            0 if x["group"] == "央视频道" else 1 if x["group"] == "卫视频道" else 2,
            rank_cctv(x["name"]) if x["group"] == "央视频道" else 0,
            x["name"],
        ))
        for i, c in enumerate(live_json):
            c["num"] = i + 1

        live_txt_lines = []
        for c in live_json:
            live_txt_lines.append("%s,#genre#" % c["group"])
            for ch in c["channels"]:
                live_txt_lines.append("%s,%s" % (c["name"], ch["url"]))
            live_txt_lines.append("")
        live_txt = "\n".join(live_txt_lines).strip() + "\n"

        # 保底：骤降时保留旧
        old_live = ""
        lp = os.path.join(OUT, "live.txt")
        if os.path.exists(lp):
            old_live = open(lp, encoding="utf-8").read()
        new_lines = sum(len(c["channels"]) for c in live_json)
        old_lines = sum(1 for l in old_live.splitlines() if ",http" in l)
        if new_lines > 0 and (src_ok == -1 or src_ok >= 2 or old_lines == 0 or new_lines >= old_lines * 0.4):
            atomic_put(lp, live_txt)
        else:
            wlog("直播结果异常(new=%d/old=%d)，保留旧缓存" % (new_lines, old_lines))

        atomic_put(os.path.join(OUT, "live.json"), live_json, is_json=True)
        wlog("直播输出：%d 频道（央视%d/卫视%d/其他%d），%d 条通道" %
             (tot_ok, len(cctv), len(sat), len(oth), new_lines))

        # --- 多仓 cang.json ---
        cang_urls, seen_u = [], set()
        for e in pool_cang["sites"]:
            if e["s"] != 1:
                continue
            ck = cang_key(e["u"])
            if ck in seen_u:
                continue
            seen_u.add(ck)
            nm = e["n"]
            if e.get("cnt", 0) > 0 and "保底" not in nm:
                nm = "%s〔%d源〕" % (nm, e["cnt"])
            cang_urls.append({"name": nm, "url": e["u"], "ms": 0})
            if len(cang_urls) >= 30:
                break
        cang_urls.sort(key=lambda x: x["name"].startswith("保底") or "保底" in x["name"])
        if cang_urls:
            atomic_put(os.path.join(OUT, "cang.json"), {"urls": cang_urls}, is_json=True)
        else:
            wlog("多仓本轮无可用，保留旧缓存")
        cang_ok = sum(1 for e in pool_cang["sites"] if e["s"] == 1)

        # --- state.json ---
        queue_left = 0
        for n, es in pool_live["channels"].items():
            for e in es:
                if e["s"] == -1 or (e["s"] == 1 and now - e["t"] > CONFIG["deep_normal"]):
                    queue_left += 1
        state = {
            "last_update": now,
            "live_channels_total": len(pool_live["channels"]),
            "live_channels_ok": tot_ok,
            "live_nodes_total": tot_nodes,
            "live_lines_out": new_lines,
            "cctv_lines": len(cctv), "sat_lines": len(sat), "oth_lines": len(oth),
            "cang_total": len(pool_cang["sites"]), "cang_ok": cang_ok, "cang_out": len(cang_urls),
            "sources_ok": max(0, pool_live.get("last_src_ok", 0)) if src_ok == -1 else max(0, src_ok),
            "sources_total": len(LIVE_SRC),
            "checked_this_round": checked, "deep_checked": deep_done,
        }
        atomic_put(os.path.join(OUT, "state.json"), state, is_json=True)
        wlog("======== 采集完成 ========")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as ex:
        import traceback
        wlog("采集异常：%s\n%s" % (ex, traceback.format_exc()))
        raise
