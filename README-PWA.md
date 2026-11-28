# 鱼虾游戏足球大战 · PWA 部署与商店包说明

独立、非官方 3D 足球游戏。支持 11v11、3v3、联机、排位、100 关挑战；可安装为 PWA，也可通过 Microsoft Store 分发 Windows 版。

## 目录

| 路径 | 说明 |
|---|---|
| `index.html` | 游戏主页 / 主菜单 / 全部玩法 |
| `ranking.html` | 公开排位总榜 |
| `manifest.webmanifest` | PWA 清单：名称、图标、截图、启动方式 |
| `sw.js` | Service Worker：离线缓存，当前 `zhuj-cup-2026-v0.8.0` |
| `js/three.min.js` | Three.js（MIT） |
| `js/online.js` | WebRTC 联机 + 最小 MQTT 信令 |
| `js/pwa.js` | 注册 Service Worker、安装按钮 |
| `icons/` | 无脸 PWA 图标 |
| `fb/` | 游戏内真人头像（开发者声明已授权；商店截图不展示） |
| `store-assets/` | 商店主视觉、实际截图、诊断图 |
| `privacy.html` | 隐私政策 |
| `terms.html` | 用户协议 |
| `code-of-conduct.html` | 行为准则（UGC / 联机 / 榜单） |
| `report.html` | 举报与申诉 |
| `support.html` | 帮助与支持 |
| `legal.css` | 法务页共用样式 |
| `AGE-RATING.md` | IARC 年龄分级问卷答案 |
| `STORE-SUBMISSION.md` | 商店提交手册（含包生成、商店文案、审核备注） |
| `THIRD-PARTY-NOTICES.md` | 第三方组件 / 素材声明 |

## 本地运行

项目根目录已生成 `site-deploy.zip`（41 个站点文件，不含测试包/工具/旧诊断图），可作为 GitHub 上传包。也可以直接启动静态 HTTP 服务（不要直接双击 `index.html`，Service Worker / fetch 需要 HTTP）：

```powershell
python -m http.server 4173
```

然后打开 <http://127.0.0.1:4173/index.html>。

## 部署到 GitHub Pages

1. 把站点文件推到仓库 `main` 分支（不要推 `store-package/`、`*.zip`、`tools/`）。
2. 仓库 Settings → Pages → Source 选择 `Deploy from a branch` → `main` / `/ (root)`。
3. 等 Actions/部署完成，访问：
   `https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/`
4. 用 PWABuilder 对该 URL 重新分析，确认 manifest / Service Worker / 图标。
5. 部署后全站搜索确认没有“世界杯 / World Cup / FIFA / 官方”旧文案。

## 生成 Microsoft Store 包

按 `STORE-SUBMISSION.md` 执行。核心步骤：

1. 从 Partner Center 复制 `Package ID`、`Publisher ID`、`Publisher Display Name`。
2. PWABuilder → Package for Stores → Windows → Generate Package。
3. 上传 `icons/icon-1024.png`（无脸 VS 图标）。
4. 下载 ZIP，取：
   - `鱼虾游戏足球大战.msixbundle`
   - `鱼虾游戏足球大战.classic.appxbundle`
5. 在 Partner Center → Packages 上传这两个包。
6. 填 Store listing、隐私/支持 URL、IARC 年龄分级、审核备注，提交认证。

**不要上传** `*.sideload.msix`、`install.ps1` 或旧测试包；旧包的 Publisher 是 `My Company Inc`，身份不匹配。

## 合规入口

游戏主菜单底部、联机面板、排位面板、排行榜页均有：

- 用户协议 `terms.html`
- 隐私政策 `privacy.html`
- 行为准则 `code-of-conduct.html`
- 举报与申诉 `report.html`
- 帮助 `support.html`

## 已知限制

- 联机/排行榜依赖第三方公共 MQTT / STUN；服务不可用时单机仍可玩，但联机/榜单可能失败。
- 游戏内真人头像仅用于游戏内展示；商店截图使用无脸菜单或远景比赛截图。
- 商店包必须使用 Partner Center 的真实身份值生成；本仓库不包含可直接上传的最终包。
