# Microsoft Store 提交包 · 鱼虾足球游戏大战

> 本文件是提交前的操作手册，不是给商店审核看的用户文档。所有 `<>` 占位项必须替换成 Partner Center 中的真实值；不要把占位包上传。
>
> 目标站点：`https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/`
>
> 仓库：`https://github.com/windows11stephen/3dfootballgamessmallfishfishVSsmallshrimpshrimp`

## 0. 一句话结论

当前站点在“内容与合规文件”层面已经具备提交条件；**Partner Center 身份值已取得，仍缺一次正式包生成**。不要上传旧的 `zhuj-cup-2026-vs-icon-test.zip`：它的 Publisher 是 `My Company Inc`、Package ID 是 `MyCompany.zhuj2026`、回退地址是临时 tunnelmole、图标还是带脸旧图。

## 1. 必须先拿到的 3 个值

登录 [Microsoft Partner Center](https://partner.microsoft.com/dashboard) → 你的应用 → **Product identity / 产品标识**，复制：

| 字段 | Partner Center 中的名称 | 当前值 | 用途 |
|---|---|---|---|
| Package ID | Package Identity Name / 包标识名称 | `474FFE41.35077CE1C329B` | PWABuilder 的 “Package ID” |
| Publisher ID | Publisher ID / 发布者 ID | `CN=07FF8F70-E697-4738-9E1B-5940F4E35BDC` | PWABuilder 的 “Publisher ID” |
| Publisher display name | Publisher Display Name / 发布者显示名称 | `一条小鱼~` | PWABuilder 的 “Publisher display name” |

这三个值已经填入本文件末尾的“提交记录”，生成包时直接使用。**Package ID 和 Publisher ID 必须与 Partner Center 完全一致，大小写、空格、`CN=` 都不能改。**

## 2. 站点部署（必须先做）

1. 把当前 `0.2` 目录除以下文件外全部推到 GitHub 仓库的 `main` 分支：
   - 要排除：`store-package/`、`tools/`、`*.zip`、`README-PWA.md` 中的本地说明（可保留但不要作为商店页面）。
   - 要包含：`index.html`、`manifest.webmanifest`、`sw.js`、`js/`、`icons/`、`fb/`、`store-assets/`、`ranking.html`、`privacy.html`、`terms.html`、`code-of-conduct.html`、`report.html`、`support.html`、`legal.css`、`THIRD-PARTY-NOTICES.md`、`AGE-RATING.md`、`STORE-SUBMISSION.md`。
2. 等 GitHub Pages 部署完成，打开：
   - 首页：`https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/`
   - 隐私：`.../privacy.html`
   - 协议：`.../terms.html`
   - 行为准则：`.../code-of-conduct.html`
   - 举报：`.../report.html`
   - 帮助：`.../support.html`
3. 确认线上首页不再出现“世界杯 / World Cup / FIFA / 官方 / 授权”等旧文案；标题应为 `鱼虾足球游戏大战 · zhuj杯 · 3D 足球游戏`。
4. 在浏览器开发者工具 → Application → Manifest 检查：名称、图标、`start_url`、`scope`、`display` 正确；Service Worker 显示 `zhuj-cup-2026-v0.8.0` 已激活。
5. 断网重载一次，确认菜单能打开；打开隐私/协议页，确认离线也能显示。

## 3. 生成正式商店包（PWABuilder）

1. 打开 <https://www.pwabuilder.com/>，输入 GitHub Pages 首页 URL，等待报告。
2. 点 **Package for Stores** → **Windows** → **Generate Package**。
3. 按下表填写（值来自第 1 节）：

| PWABuilder 字段 | 填什么 |
|---|---|
| Package ID | Partner Center 的 Package Identity Name |
| Publisher ID | Partner Center 的 Publisher ID（`CN=...`） |
| Publisher display name | Partner Center 的 Publisher Display Name |
| App name | `鱼虾足球游戏大战` |
| Version | `0.8.0.0`（Microsoft Store 四段版本号；后续递增） |
| Start URL | `https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/index.html` |
| Icon | 上传 `icons/icon-1024.png`（无脸 VS 图标） |
| Display mode | Standalone |
| Orientation | Landscape |
| Offline support | 勾选（使用现有 Service Worker） |
| Signing | 不勾选/使用 Store 签名（按 PWABuilder 当前选项） |

4. 下载生成的 ZIP。里面应有：
   - `鱼虾足球游戏大战.msixbundle`
   - `鱼虾足球游戏大战.classic.appxbundle`
   - `鱼虾足球游戏大战.sideload.msix`（仅测试，不上传）
   - `install.ps1`（仅侧载测试，不上传）
5. 解压/检查（可选）：包内 `AppxManifest.xml` 的 `Identity Name`、`Publisher`、`PublisherDisplayName` 必须与 Partner Center 完全一致；`--app-fallback-url` 和 `start-url` 必须是 GitHub Pages，不得出现 `tunnelmole.net`。
6. 把 `.msixbundle` 和 `.classic.appxbundle` 放到 `store-package/final/`，不要上传 `.sideload.msix` 或旧测试包。

> 如果 PWABuilder 报告 `Required 0`，那只代表 PWA 技术项通过，不代表商店认证通过；仍要完成下面的商店资料和年龄分级。

## 4. Partner Center 提交流程

### 4.1 Packages（包）

1. 进入应用 → **Packages** → 上传 `.msixbundle` 和 `.classic.appxbundle`。
2. 等待包验证通过；如提示身份不匹配，检查第 1 节三个值，不要重复上传旧包。
3. 确认包内版本号与 `What's new` 一致。

### 4.2 Store listing（商店列表）

| 字段 | 建议内容 |
|---|---|
| Product name | `鱼虾足球游戏大战` |
| Short description（短描述） | `独立非官方 3D 足球游戏：小鱼鱼 vs 小虾虾，11v11、3v3、联机、排位与 100 关挑战。` |
| Full description（完整描述） | 见下方“商店描述文案” |
| Keywords | `足球, 3D, 体育, 联机, 排位, 独立游戏`（最多 7 个，按商店当前限制） |
| Category | Games → Sports |
| Privacy policy URL | `https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/privacy.html` |
| Support URL | `https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/support.html` |
| Website | `https://github.com/windows11stephen/3dfootballgamessmallfishfishVSsmallshrimpshrimp` |
| Copyright | `© 2026 鱼虾足球游戏大战. Original content; third-party notices at THIRD-PARTY-NOTICES.md.` |
| Age rating | 以 IARC 问卷结果为准（保守目标 12+；见 `AGE-RATING.md`） |

### 4.3 商店描述文案

**短描述（建议 ≤ 200 字符）：**

```text
独立非官方 3D 足球游戏：小鱼鱼 vs 小虾虾，11v11、3v3、联机、排位与 100 关挑战。
```

**完整描述：**

```text
鱼虾足球游戏大战（zhuj杯）是一款独立、非官方的 3D 足球游戏。你可以在小鱼鱼队和小虾虾队之间选择，进行 11v11 标准比赛、3v3 小队比赛、联机对战、排位赛，以及 100 关挑战。

主要功能：
• 11v11 与 3v3 两种比赛模式
• 简单 / 困难 / 噩梦三档难度
• 通过 4 位房号加入 WebRTC 联机房间
• 排位赛与公开排行榜
• 100 关固定/随机挑战与皮肤解锁
• 键位自定义、触屏/键盘双模式
• 首次加载后可离线进入菜单和单机模式

本游戏免费，不含内购、订阅、广告或抽奖。联机和排行榜使用第三方公共 MQTT/STUN 服务，需要网络；公共中继不可用时可能无法联机或刷新榜单。

本游戏与 FIFA、UEFA、任何国家协会、联赛、俱乐部、真实球员、EA、Konami 或其他游戏公司均无关联，也未获得其赞助或授权。游戏中的队名、角色和图标为原创或已授权素材。
```

**不要使用：** “世界杯”“World Cup”“FIFA”“官方”“授权”“最强”“3A”“GTA”“EA”“Konami”等可能造成官方关联、比较或侵权的词。

### 4.4 Screenshots（截图）

按顺序上传 3 张 1920×1080：

1. `store-assets/store-menu-1920x1080.png` — 主菜单 / 模式选择
2. `store-assets/store-match-1920x1080.png` — 11v11 比赛画面
3. `store-assets/store-levels-1920x1080.png` — 100 关挑战

说明：
- 三张均为实际运行截图；主菜单截图在截图时隐藏了真人头像条，比赛截图是远景（球员脸部不可辨认）。
- 不要上传 `zhuj-2026-vs-players.png` 作为“游戏截图”；它是宣传主视觉，可放 Store 的 hero/宣传图（如果 Partner Center 有对应位置）。
- 不要上传带真人脸的 `game-3v3-closeup.png` 或旧测试包内的 `Images/Square150x150Logo.scale-200.png`。

### 4.5 商店图标 / 磁贴

- 应用图标源：`icons/icon-1024.png`（极简无脸 VS 图标：鱼/虾/VS/足球，无文字、无人脸）。
- 商店列表图标：`store-assets/store-logo-300.png`（300×300）或同一张 1024×1024 PNG；PWABuilder 会自动生成包内各尺寸磁贴。
- 如果 Partner Center 单独要求 300×300 / 150×150，用 `store-assets/store-logo-300.png` / `store-logo-150.png`；不要用带脸旧图。

### 4.6 Age rating（年龄分级）

1. Partner Center → **Age rating** → 填写 IARC 问卷。
2. 使用 `AGE-RATING.md` 的逐条答案。
3. 保守目标 12+ / Users Interact；如果 IARC 实际输出 7+ 或 16+，以系统结果为准；不要为了更低年龄而隐瞒联机、公开昵称或 UGC 功能。
4. 在“Review notes”中说明：无自由聊天、无图片上传、昵称最多 10 字符、有过滤和举报入口；未满 13 岁需监护人同意。

### 4.7 Pricing & availability

- 价格：**Free**。
- 市场：先选中国大陆 + 美国/英国/日本等主要市场；如果不想处理太多地区，先选中国大陆和美国。
- 可见性：Public。
- 设备：Windows 10 版本 19041 或更高；需要 Microsoft Edge WebView2（商店包声明 HostRuntimeDependency）。
- 不需要 Xbox 特定硬件。

### 4.8 Review notes（给审核员）

```text
This is an independent, non-official cartoon 3D football game. No account or test credentials are required.

Single-player and menu work offline after the first load. Online play is optional and uses WebRTC plus third-party public MQTT brokers; if the brokers are unavailable, online/leaderboard features may be temporarily unavailable, but the single-player game remains usable.

There is no free-form chat, no image/audio/video upload, and no in-app purchases or ads. Nicknames are limited to 10 characters, filtered for control characters/links/long numbers, and can be reported from the online panel, ranking page, and report.html. The Code of Conduct, Privacy Policy, Terms, Support, and Report pages are linked in the main menu.

The game is not affiliated with FIFA, UEFA, any league, club, player, EA, Konami, or any other game company. Team names, characters, and icons are original or licensed; the developer states that the in-game portrait images have written permission. No FIFA/World Cup/real-club marks are used as source identifiers.

Suggested rating: 12+ (Users Interact) because nicknames and leaderboard entries are public and players can join online rooms. If IARC generates a different rating, please use the generated result.
```

## 5. 审核风险清单（逐项核对）

| 项目 | 当前状态 | 说明 / 动作 |
|---|---|---|
| 正式包身份匹配 | **待生成** | Partner Center 身份值已取得（见第 1 节）；用真实 Package ID / Publisher ID / Display Name 生成，旧包不能再用 |
| 稳定域名 | 待部署 | 线上必须是 GitHub Pages，不得是 `tunnelmole.net`；部署后重新跑 PWABuilder |
| 隐私政策 | 已准备 | `privacy.html`，含 MQTT/WebRTC/排行榜/删除/儿童/跨境 |
| 用户协议 | 已准备 | `terms.html`，含许可、限制、UGC、免责声明、无官方关联 |
| 行为准则 / 举报 | 已准备 | `code-of-conduct.html`、`report.html`；游戏菜单、联机面板、排行榜页均有入口 |
| 年龄分级 | 待填 | 用 `AGE-RATING.md` 在 Partner Center 填 IARC |
| 商店截图 | 已准备 | 3 张 1920×1080 实际截图；无真人脸 |
| 应用图标 | 已准备 | `icons/icon-1024.png` 无脸；包内各尺寸由 PWABuilder 生成 |
| 素材授权 | 需留证 | 用户声明 `fb/` 真人头像有授权；请保留书面/聊天记录/授权邮件，审核可能追问 |
| 旧“世界杯”文案 | 待部署后确认 | 本地已去掉；线上旧版仍有，必须部署新版本后复查 |
| 公共 MQTT 可用性 | 风险中 | 多个 wss 中继 + 单机离线；审核备注说明第三方服务可能不可用 |
| 公共 MQTT 隐私 | 已披露 | 隐私政策说明公开主题、IP/ICE、无端到端加密 |
| 内购/广告 | 无 | 商店描述明确免费、无内购/广告 |
| 版权/专利 | 低风险但非零 | 见第 6 节；不要使用 FIFA/真实球队/真实球员/其他游戏素材 |
| 测试包/侧载 | 不使用 | 不上传 `.sideload.msix`、`install.ps1`；不走开发者模式 |

## 6. 侵权与专利风险说明（不是法律意见）

### 6.1 已做的规避

- 游戏名、队名、角色为原创：`鱼虾足球游戏大战`、`小鱼鱼`、`小虾虾`；不使用 FIFA、World Cup、UEFA、真实俱乐部/球员/球衣/队徽。
- 不使用其他游戏的素材、UI、角色、音乐、代码；Three.js 为 MIT 许可。
- 不使用真实赛事数据、真实转播画面、真实球员肖像（`fb/` 头像为用户声明已授权）。
- 不实现具体专利玩法：没有 Konami 的“屏幕边缘 guide mark”、没有 EA 的 volumetric capture/mimic 动画专利方案、没有 Ballinno 的现实 VAR/越位系统、没有 White Knuckle 的现实赛事数据同步。

### 6.2 公开检索到的具体专利

| 专利/案件 | 内容 | 与本游戏关系 |
|---|---|---|
| Konami EPO T 928/03 | 最近队友在屏幕外时，在屏幕边缘显示 guide mark | 未实现；代码只有 `bestPassTarget` 选传球目标，没有边缘 guide mark。该案 2006 年，按 20 年期限通常已过期，具体以专利族法律状态为准 |
| Konami US 6,280,323 | 点球大战踢球/守门员光标 | 未实现点球大战 |
| EA US 12,508,508 | 球员动画存储（volumetric capture / mimic model） | 未使用该方案；使用程序化低模角色 |
| Ballinno EP 1 944 067 | 真实比赛 VAR/越位检测 | 不是视频游戏；未实现 |
| White Knuckle US 8,529,350 | 现实赛事数据更新游戏 | 没有现实数据同步 |

### 6.3 残余风险

- **“足球游戏”整体玩法**通常属于抽象规则/表达，难以被单一家专利垄断；但具体 GUI、动画、AI 行为仍可能有专利。当前代码没有上述具体方案。
- **FIFA/World Cup** 是中太范围最强的商标风险；必须确保线上页面、商店文案、截图、图标、包内元数据都不出现“世界杯/World Cup/FIFA/官方”字样。部署后用全站搜索复查。
- **真人头像**：用户声明有授权；请保留授权证据。若授权只覆盖游戏内展示、不覆盖商店截图/宣传图，商店截图不要使用带脸图（当前已使用无脸/远景截图）。
- **公共 MQTT/STUN**：不是侵权问题，但是商店 10.3.2 可用性和隐私风险；已在隐私政策与审核备注披露，单机模式不依赖它。

## 7. 提交记录（生成包时填写）

```text
Partner Center 应用名：鱼虾足球游戏大战
Package ID（Package Identity Name）：474FFE41.35077CE1C329B
Publisher ID：CN=07FF8F70-E697-4738-9E1B-5940F4E35BDC
Publisher Display Name：一条小鱼~
版本：0.8.0.0
Start URL：https://windows11stephen.github.io/3dfootballgamessmallfishfishVSsmallshrimpshrimp/index.html
包生成时间：<PASTE>
包文件名：鱼虾足球游戏大战.msixbundle / 鱼虾足球游戏大战.classic.appxbundle
```

## 8. 提交前最后 10 项检查

- [ ] 线上首页标题不含“世界杯/World Cup/FIFA/官方”。
- [ ] 线上首页有“用户协议/隐私政策/行为准则/举报/帮助”链接。
- [ ] `privacy.html`、`terms.html`、`code-of-conduct.html`、`report.html`、`support.html` 可直接打开。
- [ ] 断网重载能进入菜单；法律页离线可开。
- [ ] Manifest 的 `start_url`、`scope`、图标、截图正确；Service Worker 为 v0.8.0。
- [ ] PWABuilder 使用 GitHub Pages URL，不再用 tunnelmole。
- [ ] 包内 Identity / Publisher / PublisherDisplayName 与 Partner Center 完全一致。
- [ ] 包内 fallback/start URL 是 GitHub Pages；无 `tunnelmole.net`。
- [ ] 商店截图是 3 张 1920×1080，无真人脸；图标为无脸版。
- [ ] IARC 问卷已提交；隐私 URL、支持 URL 可访问；审核备注已填。
