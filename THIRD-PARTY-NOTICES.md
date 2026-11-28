# 第三方组件与素材声明 · 鱼虾游戏足球大战

本游戏包含以下开源组件。各组件的版权归原作者所有，并按下列许可提供。

## Three.js

- 项目：Three.js
- 网站：https://threejs.org/
- 许可：MIT License
- 版权：Copyright © 2010-2025 three.js authors

```text
MIT License

Copyright © 2010-2025 three.js authors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## 游戏原创内容

- 游戏代码、界面、原创角色「小鱼鱼」「小虾虾」、队徽、图标、文字和美术素材：由开发者创作或委托创作，保留所有权利。
- 游戏内真人头像素材：开发者声明已取得授权，仅用于本游戏的球员头像展示；不得单独提取、传播或用于其他项目。
- 字体：使用系统字体（Segoe UI、Microsoft YaHei、PingFang SC 等），未嵌入第三方字体文件。
- 表情符号：由操作系统/浏览器渲染，不包含第三方表情图片文件。

## 名称与商标说明

本游戏是独立、非官方作品，与 FIFA、UEFA、任何国家协会、联赛、俱乐部、真实球员、EA、Konami 或其他游戏公司均无关联，也未获得其赞助或授权。游戏名称、队名、角色和界面不使用上述权利人的官方标志作为来源标识。

如你认为本游戏中的某项内容侵犯你的版权、商标权或肖像权，请通过仓库 Issues 或游戏内 `report.html` 提交权利人、作品/商标名称、登记号或授权链、具体位置和联系方式，我们会核实并处理。

## 开源组件清单

| 组件 | 用途 | 许可 | 版权 |
|---|---|---|---|
| Three.js | 3D 渲染 | MIT | three.js authors |
| WebRTC / 浏览器 API | 点对点联机 | 浏览器内置 | 各浏览器厂商 |

> 说明：本游戏使用公共 MQTT 中继（HiveMQ、EMQX、Eclipse Mosquitto 等）作为联机信令和排行榜传输通道；这些中继是第三方服务，不在本仓库内，也不由开发者运营。游戏内的 MQTT 客户端为项目自写的最小实现，未引入 MQTT.js。
