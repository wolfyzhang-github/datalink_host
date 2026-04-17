from __future__ import annotations


INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>datalink-host 实时联调台</title>
  <style>
    :root {
      --bg: #f4efe4;
      --bg-deep: #e5dccd;
      --panel: rgba(253, 249, 240, 0.86);
      --panel-strong: rgba(255, 251, 244, 0.94);
      --line: rgba(70, 59, 39, 0.15);
      --line-strong: rgba(70, 59, 39, 0.28);
      --ink: #1f241d;
      --muted: #5b6257;
      --accent: #0f7a65;
      --accent-2: #c55f2c;
      --accent-soft: rgba(15, 122, 101, 0.14);
      --danger: #b8422f;
      --ok: #1e7c52;
      --warn: #b57a12;
      --shadow: 0 22px 70px rgba(55, 43, 24, 0.12);
      --radius-xl: 28px;
      --radius-lg: 22px;
      --radius-md: 16px;
      --radius-sm: 12px;
      --mono: "SFMono-Regular", "Cascadia Code", "JetBrains Mono", "Menlo", monospace;
      --sans: "Avenir Next", "PingFang SC", "Noto Sans SC", sans-serif;
      --title: "Avenir Next Condensed", "PingFang SC", "Noto Sans SC", sans-serif;
    }

    * {
      box-sizing: border-box;
    }

    html, body {
      margin: 0;
      min-height: 100%;
      background:
        radial-gradient(circle at 15% 12%, rgba(197, 95, 44, 0.14), transparent 22%),
        radial-gradient(circle at 82% 16%, rgba(15, 122, 101, 0.16), transparent 26%),
        radial-gradient(circle at 50% 100%, rgba(13, 74, 88, 0.10), transparent 38%),
        linear-gradient(180deg, #fbf8f2 0%, var(--bg) 55%, var(--bg-deep) 100%);
      color: var(--ink);
      font-family: var(--sans);
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      background:
        linear-gradient(135deg, rgba(255,255,255,0.14), transparent 30%),
        repeating-linear-gradient(
          90deg,
          transparent 0,
          transparent 36px,
          rgba(39, 33, 21, 0.018) 36px,
          rgba(39, 33, 21, 0.018) 37px
        );
      pointer-events: none;
    }

    main {
      width: min(1440px, calc(100% - 32px));
      margin: 24px auto 40px;
      position: relative;
      z-index: 1;
    }

    .shell {
      display: grid;
      gap: 18px;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
      position: relative;
      overflow: hidden;
      animation: float-in 480ms ease both;
    }

    .panel::after {
      content: "";
      position: absolute;
      inset: 0;
      background: linear-gradient(120deg, rgba(255,255,255,0.16), transparent 25%, transparent 75%, rgba(255,255,255,0.10));
      pointer-events: none;
    }

    .hero {
      padding: 28px;
      display: grid;
      grid-template-columns: minmax(0, 1.6fr) minmax(300px, 0.9fr);
      gap: 20px;
      background:
        radial-gradient(circle at top right, rgba(15, 122, 101, 0.18), transparent 35%),
        linear-gradient(140deg, rgba(255,255,255,0.52), rgba(255,255,255,0.08));
    }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(31, 36, 29, 0.06);
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    h1 {
      margin: 14px 0 10px;
      font-family: var(--title);
      font-size: clamp(34px, 5vw, 54px);
      line-height: 0.98;
      letter-spacing: 0.01em;
    }

    .hero-copy {
      margin: 0;
      max-width: 760px;
      line-height: 1.65;
      color: var(--muted);
      font-size: 15px;
    }

    .hero-side {
      display: grid;
      gap: 14px;
      align-content: start;
    }

    .status-stack {
      display: grid;
      gap: 10px;
    }

    .status-strip {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }

    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.62);
      font-size: 13px;
      color: var(--muted);
    }

    .dot {
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: var(--warn);
      box-shadow: 0 0 0 5px rgba(181, 122, 18, 0.12);
    }

    .dot.ok {
      background: var(--ok);
      box-shadow: 0 0 0 5px rgba(30, 124, 82, 0.12);
    }

    .dot.off {
      background: #8f948b;
      box-shadow: 0 0 0 5px rgba(143, 148, 139, 0.12);
    }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }

    button,
    input,
    select {
      font: inherit;
    }

    button {
      appearance: none;
      border: none;
      border-radius: 999px;
      padding: 12px 18px;
      cursor: pointer;
      transition: transform 140ms ease, box-shadow 140ms ease, background 140ms ease;
    }

    button:hover {
      transform: translateY(-1px);
    }

    button:active {
      transform: translateY(0);
    }

    button.primary {
      background: linear-gradient(135deg, var(--accent), #1b5d79);
      color: white;
      box-shadow: 0 14px 30px rgba(15, 122, 101, 0.28);
    }

    button.secondary {
      background: rgba(31, 36, 29, 0.08);
      color: var(--ink);
      border: 1px solid rgba(31, 36, 29, 0.08);
    }

    button.warn {
      background: linear-gradient(135deg, #ca6631, #a94926);
      color: white;
      box-shadow: 0 14px 30px rgba(169, 73, 38, 0.24);
    }

    button[disabled] {
      opacity: 0.45;
      cursor: not-allowed;
      transform: none;
      box-shadow: none;
    }

    .message {
      min-height: 22px;
      font-size: 13px;
      color: var(--muted);
      transition: color 150ms ease;
    }

    .message.ok {
      color: var(--ok);
    }

    .message.error {
      color: var(--danger);
    }

    .grid {
      display: grid;
      grid-template-columns: minmax(360px, 1.05fr) minmax(280px, 0.95fr);
      gap: 18px;
    }

    .panel-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 18px;
    }

    .panel-head h2,
    .panel-head h3 {
      margin: 0;
      font-size: 18px;
      letter-spacing: 0.01em;
    }

    .subtle {
      color: var(--muted);
      font-size: 13px;
    }

    .config-panel,
    .forward-panel,
    .wave-panel,
    .packet-panel {
      padding: 22px;
    }

    .field-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px 12px;
    }

    .field {
      display: grid;
      gap: 7px;
    }

    .field.span-2 {
      grid-column: 1 / -1;
    }

    .field label {
      font-size: 12px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }

    .field input,
    .field select {
      width: 100%;
      padding: 12px 14px;
      border-radius: var(--radius-sm);
      border: 1px solid var(--line);
      background: rgba(255, 252, 246, 0.82);
      color: var(--ink);
      outline: none;
      transition: border-color 140ms ease, box-shadow 140ms ease, background 140ms ease;
    }

    .field input:focus,
    .field select:focus {
      border-color: rgba(15, 122, 101, 0.56);
      box-shadow: 0 0 0 4px rgba(15, 122, 101, 0.10);
      background: rgba(255, 252, 246, 0.96);
    }

    .field small {
      color: var(--muted);
      line-height: 1.5;
    }

    .toggle-line {
      display: flex;
      gap: 12px;
      align-items: center;
      padding: 12px 14px;
      border-radius: var(--radius-sm);
      border: 1px solid var(--line);
      background: rgba(255, 252, 246, 0.82);
    }

    .toggle-line input {
      width: 18px;
      height: 18px;
      accent-color: var(--accent);
    }

    .mode-hint {
      margin-top: 14px;
      padding: 12px 14px;
      border-radius: var(--radius-sm);
      background: rgba(15, 122, 101, 0.08);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }

    .wave-panel {
      display: grid;
      gap: 16px;
    }

    .wave-toolbar {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .canvas-wrap {
      position: relative;
      min-height: 360px;
      border-radius: 20px;
      border: 1px solid var(--line);
      background:
        radial-gradient(circle at top right, rgba(15, 122, 101, 0.10), transparent 34%),
        linear-gradient(180deg, rgba(22, 31, 28, 0.96), rgba(15, 25, 31, 0.98));
      overflow: hidden;
    }

    #wave-canvas {
      display: block;
      width: 100%;
      height: 100%;
      min-height: 360px;
    }

    .wave-meta {
      position: absolute;
      left: 14px;
      right: 14px;
      top: 12px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      pointer-events: none;
      color: rgba(240, 244, 241, 0.84);
      font-family: var(--mono);
      font-size: 12px;
    }

    .wave-empty {
      position: absolute;
      inset: 0;
      display: grid;
      place-items: center;
      text-align: center;
      color: rgba(240, 244, 241, 0.72);
      padding: 20px;
      font-size: 14px;
      line-height: 1.7;
      pointer-events: none;
    }

    .packet-panel {
      display: grid;
      gap: 16px;
    }

    .packet-toolbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
      gap: 12px;
      align-items: end;
    }

    .console {
      min-height: 620px;
      max-height: 68vh;
      overflow: auto;
      padding: 18px;
      border-radius: 20px;
      border: 1px solid rgba(206, 216, 209, 0.12);
      background:
        linear-gradient(180deg, rgba(13, 19, 22, 0.98), rgba(10, 14, 18, 0.98));
      color: #d7efe7;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.6;
      white-space: pre-wrap;
      word-break: break-word;
      position: relative;
    }

    .console::before {
      content: "";
      position: sticky;
      top: -18px;
      display: block;
      height: 18px;
      background: linear-gradient(180deg, rgba(10, 14, 18, 1), rgba(10, 14, 18, 0));
      pointer-events: none;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }

    .metric {
      border-radius: 18px;
      padding: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.56);
    }

    .metric strong {
      display: block;
      font-size: 22px;
      font-family: var(--title);
      margin-top: 6px;
    }

    .metric span {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .muted-box {
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(31, 36, 29, 0.05);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }

    @keyframes float-in {
      from {
        opacity: 0;
        transform: translateY(10px);
      }
      to {
        opacity: 1;
        transform: translateY(0);
      }
    }

    @media (max-width: 1080px) {
      .hero,
      .grid {
        grid-template-columns: 1fr;
      }

      .wave-toolbar,
      .metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 720px) {
      main {
        width: min(100% - 18px, 1440px);
        margin-top: 12px;
      }

      .hero,
      .config-panel,
      .forward-panel,
      .wave-panel,
      .packet-panel {
        padding: 18px;
      }

      .field-grid,
      .wave-toolbar,
      .metrics,
      .packet-toolbar {
        grid-template-columns: 1fr;
      }

      h1 {
        font-size: 34px;
      }
    }
  </style>
</head>
<body>
  <main>
    <div class="shell">
      <section class="panel hero">
        <div>
          <div class="eyebrow">DataLink Protocol Workbench</div>
          <h1>边调协议<br>边看原始数据和波形</h1>
          <p class="hero-copy">
            这个页面直接对接当前运行中的 datalink-host。你可以在这里修改接入方式和协议解析参数，
            实时查看收到的原始包十六进制内容，并把最新波形按原始值、相位展开或降采样结果绘制出来。
          </p>
        </div>
        <div class="hero-side">
          <div class="status-stack">
            <div class="status-strip">
              <div class="chip"><span id="dot-processing" class="dot off"></span><span id="label-processing">接收停止</span></div>
              <div class="chip"><span id="dot-data" class="dot off"></span><span id="label-data">数据未连接</span></div>
              <div class="chip"><span id="dot-datalink" class="dot off"></span><span id="label-datalink">DataLink 未连接</span></div>
            </div>
            <div class="metrics">
              <div class="metric"><span>已收包数</span><strong id="metric-packets">0</strong></div>
              <div class="metric"><span>已收字节</span><strong id="metric-bytes">0</strong></div>
              <div class="metric"><span>源采样率</span><strong id="metric-rate">-</strong></div>
              <div class="metric"><span>队列深度</span><strong id="metric-queue">0</strong></div>
            </div>
          </div>
          <div class="toolbar">
            <button id="save-config" class="primary">保存协议配置</button>
            <button id="start-processing" class="secondary">启动接收</button>
            <button id="stop-processing" class="warn">停止接收</button>
          </div>
          <div id="message" class="message"></div>
        </div>
      </section>

      <section class="grid">
        <div class="shell">
          <section class="panel config-panel">
            <div class="panel-head">
              <h2>接入与协议解析</h2>
              <div id="channels-badge" class="subtle">通道数 -</div>
            </div>
            <div class="field-grid">
              <div class="field">
                <label for="data-mode">接入模式</label>
                <select id="data-mode">
                  <option value="client">主动连接设备</option>
                  <option value="server">监听设备连接</option>
                </select>
              </div>
              <div class="field">
                <label for="frame-header">帧头值</label>
                <input id="frame-header" type="text" placeholder="例如 11 或 0x1234">
              </div>
              <div class="field" data-role="listen">
                <label for="listen-host">本地监听地址</label>
                <input id="listen-host" type="text" placeholder="0.0.0.0">
              </div>
              <div class="field" data-role="listen">
                <label for="listen-port">本地监听端口</label>
                <input id="listen-port" type="number" min="1" max="65535">
              </div>
              <div class="field" data-role="remote">
                <label for="remote-host">设备地址</label>
                <input id="remote-host" type="text" placeholder="169.254.56.252">
              </div>
              <div class="field" data-role="remote">
                <label for="remote-port">设备端口</label>
                <input id="remote-port" type="number" min="1" max="65535">
              </div>
              <div class="field">
                <label for="frame-header-size">帧头字节数</label>
                <select id="frame-header-size">
                  <option value="2">2</option>
                  <option value="4">4</option>
                  <option value="8">8</option>
                </select>
              </div>
              <div class="field">
                <label for="length-field-size">长度字段字节数</label>
                <select id="length-field-size">
                  <option value="4">4</option>
                  <option value="8">8</option>
                </select>
              </div>
              <div class="field">
                <label for="length-field-format">长度字段格式</label>
                <select id="length-field-format">
                  <option value="uint">无符号整数</option>
                  <option value="float64">浮点 float64</option>
                </select>
              </div>
              <div class="field">
                <label for="length-field-units">长度单位</label>
                <select id="length-field-units">
                  <option value="bytes">字节</option>
                  <option value="values">数值个数</option>
                </select>
              </div>
              <div class="field">
                <label for="byte-order">字节序</label>
                <select id="byte-order">
                  <option value="big">大端</option>
                  <option value="little">小端</option>
                </select>
              </div>
              <div class="field">
                <label for="channel-layout">通道排列</label>
                <select id="channel-layout">
                  <option value="interleaved">采样交织</option>
                  <option value="channel-major">按通道连续</option>
                </select>
              </div>
              <div class="field">
                <label for="data1-rate">降采样 1 频率</label>
                <input id="data1-rate" type="number" step="0.1" min="0.1">
              </div>
              <div class="field">
                <label for="data2-rate">降采样 2 频率</label>
                <input id="data2-rate" type="number" step="0.1" min="0.1">
              </div>
            </div>
            <div id="mode-hint" class="mode-hint"></div>
          </section>

          <section class="panel forward-panel">
            <div class="panel-head">
              <h3>DataLink 转发</h3>
              <div class="subtle">当前页面既能看接收，也能顺手改远传参数</div>
            </div>
            <div class="field-grid">
              <div class="field span-2">
                <label class="toggle-line" for="datalink-enabled">
                  <input id="datalink-enabled" type="checkbox">
                  <span>启用 DataLink 转发</span>
                </label>
              </div>
              <div class="field">
                <label for="datalink-host">DataLink 主机</label>
                <input id="datalink-host" type="text" placeholder="127.0.0.1">
              </div>
              <div class="field">
                <label for="datalink-port">DataLink 端口</label>
                <input id="datalink-port" type="number" min="1" max="65535">
              </div>
              <div class="field span-2">
                <label class="toggle-line" for="ack-required">
                  <input id="ack-required" type="checkbox">
                  <span>发送后等待 ACK</span>
                </label>
              </div>
              <div class="field span-2">
                <label class="toggle-line" for="send-data2">
                  <input id="send-data2" type="checkbox">
                  <span>同时发送降采样 2</span>
                </label>
              </div>
            </div>
            <div class="muted-box">
              原始打印区展示的是接收端解包前的字节包预览。为避免页面被超大帧拖垮，单包默认最多显示前 1024 字节，并标记截断长度。
            </div>
          </section>

          <section class="panel wave-panel">
            <div class="panel-head">
              <h2>实时波形</h2>
              <div class="subtle" id="wave-summary">等待数据...</div>
            </div>
            <div class="wave-toolbar">
              <div class="field">
                <label for="wave-mode">波形来源</label>
                <select id="wave-mode">
                  <option value="raw">原始值</option>
                  <option value="unwrapped">相位展开</option>
                  <option value="data1">降采样 1</option>
                  <option value="data2">降采样 2</option>
                </select>
              </div>
              <div class="field">
                <label for="wave-channel">显示通道</label>
                <select id="wave-channel"></select>
              </div>
              <div class="field">
                <label for="max-points">最大点数</label>
                <select id="max-points">
                  <option value="512">512</option>
                  <option value="1024" selected>1024</option>
                  <option value="2048">2048</option>
                  <option value="4096">4096</option>
                </select>
              </div>
              <div class="field">
                <label for="refresh-ms">刷新周期</label>
                <select id="refresh-ms">
                  <option value="250">250 ms</option>
                  <option value="500" selected>500 ms</option>
                  <option value="1000">1 s</option>
                </select>
              </div>
            </div>
            <div class="canvas-wrap">
              <canvas id="wave-canvas"></canvas>
              <div class="wave-meta">
                <span id="wave-left-meta">channel -</span>
                <span id="wave-right-meta">sampleRate -</span>
              </div>
              <div id="wave-empty" class="wave-empty">还没有可绘制的数据。<br>保存协议并启动接收后，最新帧会出现在这里。</div>
            </div>
          </section>
        </div>

        <section class="panel packet-panel">
          <div class="panel-head">
            <h2>原始包打印</h2>
            <div class="subtle" id="packet-summary">最近 0 包</div>
          </div>
          <div class="packet-toolbar">
            <div class="field">
              <label for="max-packets">打印包数</label>
              <select id="max-packets">
                <option value="10">10</option>
                <option value="20" selected>20</option>
                <option value="30">30</option>
                <option value="40">40</option>
              </select>
            </div>
            <div class="field">
              <label class="toggle-line" for="auto-scroll">
                <input id="auto-scroll" type="checkbox" checked>
                <span>自动滚动到底部</span>
              </label>
            </div>
            <button id="clear-console" class="secondary">清空视图</button>
          </div>
          <pre id="packet-console" class="console">等待数据...</pre>
        </section>
      </section>
    </div>
  </main>

  <script>
    const state = {
      waveform: null,
      channelCodes: [],
      configLoaded: false,
      pollTimer: null,
      pollInFlight: false,
      maxPoints: 1024,
      maxPackets: 20,
      refreshMs: 500,
      clearConsoleRequested: false,
    };

    const elements = {
      saveConfig: document.getElementById("save-config"),
      startProcessing: document.getElementById("start-processing"),
      stopProcessing: document.getElementById("stop-processing"),
      message: document.getElementById("message"),
      dataMode: document.getElementById("data-mode"),
      frameHeader: document.getElementById("frame-header"),
      listenHost: document.getElementById("listen-host"),
      listenPort: document.getElementById("listen-port"),
      remoteHost: document.getElementById("remote-host"),
      remotePort: document.getElementById("remote-port"),
      frameHeaderSize: document.getElementById("frame-header-size"),
      lengthFieldSize: document.getElementById("length-field-size"),
      lengthFieldFormat: document.getElementById("length-field-format"),
      lengthFieldUnits: document.getElementById("length-field-units"),
      byteOrder: document.getElementById("byte-order"),
      channelLayout: document.getElementById("channel-layout"),
      data1Rate: document.getElementById("data1-rate"),
      data2Rate: document.getElementById("data2-rate"),
      datalinkEnabled: document.getElementById("datalink-enabled"),
      datalinkHost: document.getElementById("datalink-host"),
      datalinkPort: document.getElementById("datalink-port"),
      ackRequired: document.getElementById("ack-required"),
      sendData2: document.getElementById("send-data2"),
      channelsBadge: document.getElementById("channels-badge"),
      modeHint: document.getElementById("mode-hint"),
      dotProcessing: document.getElementById("dot-processing"),
      dotData: document.getElementById("dot-data"),
      dotDatalink: document.getElementById("dot-datalink"),
      labelProcessing: document.getElementById("label-processing"),
      labelData: document.getElementById("label-data"),
      labelDatalink: document.getElementById("label-datalink"),
      metricPackets: document.getElementById("metric-packets"),
      metricBytes: document.getElementById("metric-bytes"),
      metricRate: document.getElementById("metric-rate"),
      metricQueue: document.getElementById("metric-queue"),
      waveMode: document.getElementById("wave-mode"),
      waveChannel: document.getElementById("wave-channel"),
      maxPoints: document.getElementById("max-points"),
      refreshMs: document.getElementById("refresh-ms"),
      maxPackets: document.getElementById("max-packets"),
      autoScroll: document.getElementById("auto-scroll"),
      clearConsole: document.getElementById("clear-console"),
      packetConsole: document.getElementById("packet-console"),
      packetSummary: document.getElementById("packet-summary"),
      waveSummary: document.getElementById("wave-summary"),
      waveCanvas: document.getElementById("wave-canvas"),
      waveEmpty: document.getElementById("wave-empty"),
      waveLeftMeta: document.getElementById("wave-left-meta"),
      waveRightMeta: document.getElementById("wave-right-meta"),
    };

    async function fetchJson(url, options) {
      const response = await fetch(url, {
        headers: {
          "Content-Type": "application/json",
        },
        ...options,
      });
      const data = await response.json();
      if (!response.ok || data.status !== "ok") {
        const message = data && data.detail ? data.detail : "请求失败";
        throw new Error(message);
      }
      return data.payload;
    }

    function setMessage(text, tone = "") {
      elements.message.textContent = text;
      elements.message.className = "message" + (tone ? " " + tone : "");
    }

    function formatNumber(value) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) {
        return "-";
      }
      return new Intl.NumberFormat("zh-CN").format(Number(value));
    }

    function formatRate(value) {
      if (!value) {
        return "-";
      }
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {
        return "-";
      }
      return numeric >= 100 ? numeric.toFixed(0) + " Hz" : numeric.toFixed(2) + " Hz";
    }

    function formatTimestamp(value) {
      if (!value) {
        return "-";
      }
      return new Date(value * 1000).toLocaleString("zh-CN", {
        hour12: false,
      });
    }

    function setStatusChip(dotEl, textEl, activeText, inactiveText, active) {
      dotEl.className = "dot " + (active ? "ok" : "off");
      textEl.textContent = active ? activeText : inactiveText;
    }

    function updateModeHint() {
      const mode = elements.dataMode.value;
      if (mode === "client") {
        elements.modeHint.textContent = "当前为主动连接设备模式：系统会主动连到“设备地址/设备端口”，本地监听地址仅用于保留配置。";
      } else {
        elements.modeHint.textContent = "当前为监听设备连接模式：系统会在“本地监听地址/本地监听端口”等待设备接入。";
      }
      document.querySelectorAll("[data-role='listen']").forEach((item) => {
        item.style.opacity = mode === "server" ? "1" : "0.62";
      });
      document.querySelectorAll("[data-role='remote']").forEach((item) => {
        item.style.opacity = mode === "client" ? "1" : "0.62";
      });
    }

    function applyConfig(config) {
      const dataServer = config.data_server || {};
      const protocol = config.protocol || {};
      const processing = config.processing || {};
      const datalink = config.datalink || {};

      elements.dataMode.value = dataServer.mode || "client";
      elements.frameHeader.value = protocol.frame_header ?? "";
      elements.listenHost.value = dataServer.host || "";
      elements.listenPort.value = dataServer.port ?? "";
      elements.remoteHost.value = dataServer.remote_host || "";
      elements.remotePort.value = dataServer.remote_port ?? "";
      elements.frameHeaderSize.value = String(protocol.frame_header_size ?? 2);
      elements.lengthFieldSize.value = String(protocol.length_field_size ?? 8);
      elements.lengthFieldFormat.value = protocol.length_field_format || "float64";
      elements.lengthFieldUnits.value = protocol.length_field_units || "values";
      elements.byteOrder.value = protocol.byte_order || "big";
      elements.channelLayout.value = protocol.channel_layout || "interleaved";
      elements.data1Rate.value = processing.data1_rate ?? 100;
      elements.data2Rate.value = processing.data2_rate ?? 10;
      elements.datalinkEnabled.checked = Boolean(datalink.enabled);
      elements.datalinkHost.value = datalink.host || "";
      elements.datalinkPort.value = datalink.port ?? "";
      elements.ackRequired.checked = Boolean(datalink.ack_required);
      elements.sendData2.checked = Boolean(datalink.send_data2);
      elements.channelsBadge.textContent = "通道数 " + (protocol.channels ?? "-");
      state.configLoaded = true;
      updateModeHint();
    }

    function readNumber(input, fieldName) {
      const value = input.value.trim();
      if (!value) {
        throw new Error(fieldName + "不能为空");
      }
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) {
        throw new Error(fieldName + "格式不正确");
      }
      return parsed;
    }

    function collectConfigPayload() {
      return {
        data_server: {
          mode: elements.dataMode.value,
          host: elements.listenHost.value.trim(),
          port: readNumber(elements.listenPort, "本地监听端口"),
          remote_host: elements.remoteHost.value.trim(),
          remote_port: readNumber(elements.remotePort, "设备端口"),
        },
        protocol: {
          frame_header: elements.frameHeader.value.trim(),
          frame_header_size: readNumber(elements.frameHeaderSize, "帧头字节数"),
          length_field_size: readNumber(elements.lengthFieldSize, "长度字段字节数"),
          length_field_format: elements.lengthFieldFormat.value,
          length_field_units: elements.lengthFieldUnits.value,
          byte_order: elements.byteOrder.value,
          channel_layout: elements.channelLayout.value,
        },
        processing: {
          data1_rate: readNumber(elements.data1Rate, "降采样 1 频率"),
          data2_rate: readNumber(elements.data2Rate, "降采样 2 频率"),
        },
        datalink: {
          enabled: elements.datalinkEnabled.checked,
          host: elements.datalinkHost.value.trim(),
          port: readNumber(elements.datalinkPort, "DataLink 端口"),
          ack_required: elements.ackRequired.checked,
          send_data2: elements.sendData2.checked,
        },
      };
    }

    function syncChannelSelect(channelCodes, channelCount) {
      const labels = [];
      for (let index = 0; index < channelCount; index += 1) {
        labels.push(channelCodes[index] || ("CH" + String(index + 1).padStart(2, "0")));
      }
      const currentValue = Number(elements.waveChannel.value || 0);
      if (elements.waveChannel.options.length !== labels.length) {
        elements.waveChannel.innerHTML = labels
          .map((label, index) => '<option value="' + index + '">' + label + "</option>")
          .join("");
      } else {
        Array.from(elements.waveChannel.options).forEach((option, index) => {
          option.textContent = labels[index];
        });
      }
      elements.waveChannel.value = String(Math.min(currentValue, Math.max(labels.length - 1, 0)));
    }

    function renderStatus(status, processingActive) {
      setStatusChip(elements.dotProcessing, elements.labelProcessing, "接收运行中", "接收停止", processingActive);
      setStatusChip(elements.dotData, elements.labelData, "数据已连接", "数据未连接", Boolean(status.data_connected));
      setStatusChip(
        elements.dotDatalink,
        elements.labelDatalink,
        "DataLink 已连接",
        status.datalink_enabled ? "DataLink 连接中" : "DataLink 未启用",
        Boolean(status.datalink_connected)
      );

      if (!status.datalink_enabled && !status.datalink_connected) {
        elements.dotDatalink.className = "dot off";
      }

      elements.metricPackets.textContent = formatNumber(status.packets_received);
      elements.metricBytes.textContent = formatNumber(status.bytes_received);
      elements.metricRate.textContent = formatRate(status.source_sample_rate);
      elements.metricQueue.textContent = formatNumber(status.queue_depth);

      elements.startProcessing.disabled = processingActive;
      elements.stopProcessing.disabled = !processingActive;

      if (status.last_error) {
        setMessage("最近错误: " + status.last_error, "error");
      }
    }

    function resizeCanvas(canvas) {
      const ratio = window.devicePixelRatio || 1;
      const width = Math.max(Math.floor(canvas.clientWidth * ratio), 1);
      const height = Math.max(Math.floor(canvas.clientHeight * ratio), 1);
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      return { width, height, ratio };
    }

    function drawWaveform(waveform) {
      state.waveform = waveform;
      const canvas = elements.waveCanvas;
      const ctx = canvas.getContext("2d");
      const { width, height, ratio } = resizeCanvas(canvas);
      ctx.save();
      ctx.scale(ratio, ratio);
      const cssWidth = width / ratio;
      const cssHeight = height / ratio;

      ctx.clearRect(0, 0, cssWidth, cssHeight);

      const series = waveform && Array.isArray(waveform.series) ? waveform.series : [];
      const channelIndex = Math.min(Number(elements.waveChannel.value || 0), Math.max(series.length - 1, 0));
      const channelData = series[channelIndex] || [];
      const hasData = channelData.length > 1;
      elements.waveEmpty.style.display = hasData ? "none" : "grid";

      const gridColor = "rgba(212, 234, 224, 0.12)";
      ctx.strokeStyle = gridColor;
      ctx.lineWidth = 1;
      for (let i = 1; i < 6; i += 1) {
        const y = (cssHeight / 6) * i;
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(cssWidth, y);
        ctx.stroke();
      }
      for (let i = 1; i < 8; i += 1) {
        const x = (cssWidth / 8) * i;
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, cssHeight);
        ctx.stroke();
      }

      if (!hasData) {
        ctx.restore();
        return;
      }

      let min = Math.min(...channelData);
      let max = Math.max(...channelData);
      if (Math.abs(max - min) < 1e-9) {
        min -= 1;
        max += 1;
      }
      const padding = (max - min) * 0.1;
      min -= padding;
      max += padding;

      const zeroY = cssHeight - ((0 - min) / (max - min)) * cssHeight;
      ctx.strokeStyle = "rgba(241, 196, 92, 0.28)";
      ctx.beginPath();
      ctx.moveTo(0, zeroY);
      ctx.lineTo(cssWidth, zeroY);
      ctx.stroke();

      const gradient = ctx.createLinearGradient(0, 0, cssWidth, cssHeight);
      gradient.addColorStop(0, "#53f2b5");
      gradient.addColorStop(0.55, "#7af0ff");
      gradient.addColorStop(1, "#f7a258");

      ctx.lineWidth = 2;
      ctx.strokeStyle = gradient;
      ctx.shadowColor = "rgba(83, 242, 181, 0.32)";
      ctx.shadowBlur = 14;
      ctx.beginPath();
      channelData.forEach((value, index) => {
        const x = (index / Math.max(channelData.length - 1, 1)) * cssWidth;
        const y = cssHeight - ((value - min) / (max - min)) * cssHeight;
        if (index === 0) {
          ctx.moveTo(x, y);
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.stroke();
      ctx.shadowBlur = 0;

      elements.waveSummary.textContent =
        "更新时间 " + formatTimestamp(waveform.updated_at) +
        " · " + waveform.points + " 点 · " + formatRate(waveform.sample_rate);
      elements.waveLeftMeta.textContent =
        "channel " + (elements.waveChannel.options[channelIndex]?.textContent || ("CH" + (channelIndex + 1)));
      elements.waveRightMeta.textContent =
        "min " + min.toFixed(3) + "  max " + max.toFixed(3);
      ctx.restore();
    }

    function renderPackets(packets) {
      const items = Array.isArray(packets) ? packets : [];
      elements.packetSummary.textContent = "最近 " + items.length + " 包";
      if (state.clearConsoleRequested) {
        elements.packetConsole.textContent = "";
        state.clearConsoleRequested = false;
      }
      if (!items.length) {
        elements.packetConsole.textContent = "等待数据...";
        return;
      }
      const text = items.map((packet, index) => {
        const header =
          "[" + String(index + 1).padStart(2, "0") + "] " +
          formatTimestamp(packet.received_at) +
          "  sr=" + formatRate(packet.sample_rate) +
          "  payload=" + formatNumber(packet.payload_bytes) + "B" +
          "  raw=" + formatNumber(packet.raw_bytes) + "B";
        const truncated = packet.truncated_bytes > 0
          ? "\\n... 已截断 " + formatNumber(packet.truncated_bytes) + " 字节"
          : "";
        return header + "\\n" + (packet.hex_dump || "<empty>") + truncated;
      }).join("\\n\\n");

      elements.packetConsole.textContent = text;
      if (elements.autoScroll.checked) {
        elements.packetConsole.scrollTop = elements.packetConsole.scrollHeight;
      }
    }

    async function refreshMonitor() {
      if (state.pollInFlight) {
        return;
      }
      state.pollInFlight = true;
      try {
        const payload = await fetchJson(
          "/api/monitor?mode=" + encodeURIComponent(elements.waveMode.value) +
          "&max_points=" + encodeURIComponent(String(state.maxPoints)) +
          "&max_packets=" + encodeURIComponent(String(state.maxPackets))
        );
        state.channelCodes = payload.channel_codes || [];
        syncChannelSelect(state.channelCodes, payload.waveform?.channels || state.channelCodes.length || 1);
        renderStatus(payload.status || {}, Boolean(payload.processing_active));
        drawWaveform(payload.waveform || { series: [] });
        renderPackets(payload.recent_packets || []);
      } catch (error) {
        setMessage(error.message || "刷新监视数据失败", "error");
      } finally {
        state.pollInFlight = false;
      }
    }

    async function loadConfig() {
      try {
        const config = await fetchJson("/api/config");
        applyConfig(config);
      } catch (error) {
        setMessage(error.message || "加载配置失败", "error");
      }
    }

    async function saveConfig() {
      try {
        setMessage("正在保存配置...");
        const payload = collectConfigPayload();
        const config = await fetchJson("/api/config", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        applyConfig(config);
        setMessage("协议配置已保存，运行中的接收端会按新参数自动重连。", "ok");
        await refreshMonitor();
      } catch (error) {
        setMessage(error.message || "保存配置失败", "error");
      }
    }

    async function toggleProcessing(action) {
      try {
        setMessage(action === "start" ? "正在启动接收..." : "正在停止接收...");
        await fetchJson("/api/processing/" + action, {
          method: "POST",
        });
        setMessage(action === "start" ? "数据接收已启动。" : "数据接收已停止。", "ok");
        await refreshMonitor();
      } catch (error) {
        setMessage(error.message || "处理接收状态失败", "error");
      }
    }

    function schedulePolling() {
      if (state.pollTimer) {
        clearInterval(state.pollTimer);
      }
      state.pollTimer = window.setInterval(refreshMonitor, state.refreshMs);
    }

    function bindEvents() {
      elements.saveConfig.addEventListener("click", saveConfig);
      elements.startProcessing.addEventListener("click", () => toggleProcessing("start"));
      elements.stopProcessing.addEventListener("click", () => toggleProcessing("stop"));
      elements.dataMode.addEventListener("change", updateModeHint);
      elements.waveMode.addEventListener("change", refreshMonitor);
      elements.waveChannel.addEventListener("change", () => drawWaveform(state.waveform || { series: [] }));
      elements.maxPoints.addEventListener("change", () => {
        state.maxPoints = Number(elements.maxPoints.value);
        refreshMonitor();
      });
      elements.maxPackets.addEventListener("change", () => {
        state.maxPackets = Number(elements.maxPackets.value);
        refreshMonitor();
      });
      elements.refreshMs.addEventListener("change", () => {
        state.refreshMs = Number(elements.refreshMs.value);
        schedulePolling();
      });
      elements.clearConsole.addEventListener("click", () => {
        state.clearConsoleRequested = true;
        renderPackets([]);
      });

      if (window.ResizeObserver) {
        const observer = new ResizeObserver(() => drawWaveform(state.waveform || { series: [] }));
        observer.observe(elements.waveCanvas);
      } else {
        window.addEventListener("resize", () => drawWaveform(state.waveform || { series: [] }));
      }
    }

    async function boot() {
      bindEvents();
      updateModeHint();
      await loadConfig();
      await refreshMonitor();
      schedulePolling();
    }

    boot();
  </script>
</body>
</html>
"""
