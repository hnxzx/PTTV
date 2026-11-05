# GitHub 建空仓库 + 推送 + 开 Pages 全程中文指南

> 配合老薛不熟悉 GitHub 界面，每一步给「英文原文 → 中文含义 → 该选什么」。
> 目标：建 `hnxzx/PTTV` → 推代码 → 开 Pages → 拿到公网链接。

---

## 第一步：建空仓库（3 个按钮，1 分钟）

1. 浏览器打开 **`https://github.com/new`**
2. 按下面表格填：

| 页面英文 | 中文含义 | 你填/选什么 |
|---|---|---|
| **Owner** | 仓库归属 | 保持 `hnxzx`（默认） |
| **Repository name** | 仓库名 | `PTTV` |
| **Description** | 描述 | `PT-IPTV 直播/点播聚合` |
| **Public** / **Private** | 公开/私有 | 选 **Public** |
| Add a README file | 加 README | ❌ 不勾 |
| Add .gitignore | 加忽略文件 | ❌ 不勾 |
| Initialize with a license | 加开源协议 | ❌ 不勾 |

3. 滑到最底，点蓝按钮 **Create repository**（创建仓库）

建好会跳到一个**空仓库页面**，页面上有一段 `git push` 命令提示——**忽略它**，代码我来推。

---

## 第二步：推代码（你来，1 条命令）

在你电脑终端（PowerShell 或 Git Bash）跑：

```bash
cd "I:/临时文件/workbuddy/2026-11-04-16-15-11/PT-IPTV"
git push -u origin main
```

> remote 我已配好指向 `git@github.com:hnxzx/PTTV.git`，SSH 已认证，直接推即可。
> 如果提示要确认 host，输 `yes` 回车。

推完回我"推好了"。

---

## 第三步：开 GitHub Pages（拿公网链接）

1. 进仓库页 **`https://github.com/hnxzx/PTTV`**
2. 顶部点 **Settings**（设置）
3. 左侧菜单往下滑找 **Pages**（站点）
4. 在 "Build and deployment" 区域：
   - **Source** 选 **Deploy from a branch**（从分支部署）
   - **Branch** 下拉选 `main`，旁边文件夹选 `/ (root)`
5. 点 **Save**（保存）

页面顶部会提示 "Your build was published"，链接就是：

```
https://hnxzx.github.io/PTTV/
```

几分钟后这个链接就能访问网页播放器了。

---

## 第四步：验订阅链接（写进播放器）

| 用途 | 公网链接 |
|---|---|
| 直播订阅 | `https://hnxzx.github.io/PTTV/web/live.txt` |
| 点播多仓 | `https://hnxzx.github.io/PTTV/web/cang.json` |
| 网页播放器 | `https://hnxzx.github.io/PTTV/` |

> 浏览器直接打开 `live.txt` 能看到 `台名,url` 文本 = 成功。
> 打开 `cang.json` 能看到 `{"urls":[...]}` = 成功。

---

## 备用：token 获取（仅当推代码失败需要）

1. 访问 **`https://github.com/settings/tokens`**
2. 点 **Generate new token (classic)**
3. Note 填 `pttv`，勾 **repo** 整组和 **workflow**，Expiration 选 7 天
4. 点 **Generate token**，复制生成的令牌（仅用于推送，用完即吊销）

> token=密码，用完建议到 `settings/tokens` 里 Revoke（吊销）。
