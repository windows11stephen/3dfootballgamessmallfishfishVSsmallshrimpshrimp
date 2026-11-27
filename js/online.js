/* ============================================================
   联机对战模块
   - 房间发现 / 握手 / SDP 信令走公共 broker（不承载比赛数据）
   - 比赛输入和快照只走 WebRTC 直连（P2P），不挂 TURN、不跑 MQTT 数据中继
   - 主机权威：主机跑完整比赛，客户端只发操作、收快照渲染
   - 单机模式完全不受影响（NET.role 为空时所有钩子直接透传）
   ============================================================ */
(function(){
  // ---------------- 最小 MQTT 3.1.1 over WebSocket ----------------
  var ENC = new TextEncoder(), DEC = new TextDecoder();
  function mkPacket(first, parts){
    var bodyLen = 0;
    for(var i=0;i<parts.length;i++) bodyLen += parts[i].length;
    var varint = [], n = bodyLen;
    do { var d = n % 128; n = Math.floor(n/128); if(n>0) d |= 0x80; varint.push(d); } while(n>0);
    var out = new Uint8Array(1 + varint.length + bodyLen);
    out[0] = first;
    out.set(varint, 1);
    var off = 1 + varint.length;
    for(var j=0;j<parts.length;j++){ out.set(parts[j], off); off += parts[j].length; }
    return out;
  }
  function strBytes(s){
    var b = ENC.encode(s);
    var r = new Uint8Array(2 + b.length);
    r[0] = (b.length >> 8) & 255; r[1] = b.length & 255; r.set(b, 2);
    return r;
  }
  function MiniMQTT(url, clientId){
    this.url = url; this.clientId = clientId; this.ws = null;
    this.buf = new Uint8Array(0); this.onmsg = null; this.onclose = null;
    this.connected = false; this.pid = 1; this.pingTimer = null;
  }
  MiniMQTT.prototype.connect = function(timeoutMs){
    var self = this;
    return new Promise(function(resolve, reject){
      var settled = false, ws;
      try { ws = new WebSocket(self.url, ['mqtt']); } catch(e){ return reject(e); }
      self.ws = ws; ws.binaryType = 'arraybuffer';
      var timer = setTimeout(function(){ if(!settled){ settled = true; try{ws.close();}catch(_){}; reject(new Error('连接超时')); } }, timeoutMs || 10000);
      ws.onopen = function(){
        ws.send(mkPacket(0x10, [new Uint8Array([0,4]), ENC.encode('MQTT'), new Uint8Array([4,2,0,0x3C]), strBytes(self.clientId)]));
      };
      ws.onerror = function(){};
      ws.onclose = function(){
        clearTimeout(timer);
        var wasConn = self.connected;
        self.connected = false;
        if(self.pingTimer){ clearInterval(self.pingTimer); self.pingTimer = null; }
        if(!settled){ settled = true; reject(new Error('WebSocket 已关闭')); }
        else if(wasConn && self.onclose) self.onclose();
      };
      ws.onmessage = function(ev){
        var data = ev.data;
        if(!(data instanceof ArrayBuffer)) return;
        self._feed(new Uint8Array(data), function(){
          if(!settled){
            settled = true; clearTimeout(timer); self.connected = true;
            self.pingTimer = setInterval(function(){ try{ self.ws.send(mkPacket(0xC0, [])); }catch(_){} }, 30000);
            resolve(self);
          }
        });
      };
    });
  };
  MiniMQTT.prototype._feed = function(bytes, onConnack){
    var merged = new Uint8Array(this.buf.length + bytes.length);
    merged.set(this.buf, 0); merged.set(bytes, this.buf.length);
    var buf = merged;
    while(buf.length >= 2){
      var len = 0, mul = 1, i = 1, ok = false;
      while(i < buf.length){
        var d = buf[i++];
        len += (d & 0x7f) * mul; mul *= 128;
        if((d & 0x80) === 0){ ok = true; break; }
        if(mul > 1e7) break;
      }
      if(!ok) break;
      var headerEnd = i, total = headerEnd + len;
      if(buf.length < total) break;
      var first = buf[0], body = buf.subarray(headerEnd, total);
      buf = buf.subarray(total);
      if(first === 0x20){ if(onConnack) onConnack(); continue; }
      if(first === 0x90 || first === 0xD0) continue;
      if((first & 0xF0) === 0x30){
        var off2 = ((first & 0x06) !== 0) ? 2 : 0;
        var tlen = (body[off2] << 8) | body[off2+1];
        var topic = DEC.decode(body.subarray(off2+2, off2+2+tlen));
        var payload = DEC.decode(body.subarray(off2+2+tlen));
        if(this.onmsg) this.onmsg(topic, payload);
      }
    }
    this.buf = buf;
  };
  MiniMQTT.prototype.subscribe = function(topic){
    var pid = (this.pid++ & 0xFFFF) || 1;
    try { this.ws.send(mkPacket(0x82, [new Uint8Array([(pid>>8)&255, pid&255]), strBytes(topic), new Uint8Array([0])])); } catch(_){}
  };
  MiniMQTT.prototype.publish = function(topic, payload, retain){
    if(!this.connected) return;
    try { this.ws.send(mkPacket(retain ? 0x31 : 0x30, [strBytes(topic), ENC.encode(payload)])); } catch(_){}
  };
  MiniMQTT.prototype.close = function(){ try{ this.ws.close(); }catch(_){} };

  // ---------------- 联机状态 ----------------
  // 公网信令候选（只用于房间发现 / 加入握手 / SDP 交换，不承载输入和快照）。
  // 实测公共 broker 的游戏往返都 >100ms，达不到低延迟要求，
  // 所以联机数据只走 WebRTC 直连；这里保留几个最快的只做信令。
  var BROKERS = [
    'ws://broker-cn.emqx.io:8083/mqtt',
    'ws://test.mosquitto.org:8080/mqtt',
    'wss://broker-cn.emqx.io:8084/mqtt',
    'wss://test.mosquitto.org:8081/mqtt',
    'ws://broker.emqx.io:8083/mqtt'
  ];
  var NS = 'zhujcup2026/';
  var NET = window.NET = {
    role: null,            // null | 'host' | 'client' | 'client-wait'
    code: '', nick: '', oppNick: '',
    broker: '', mq: null, mqs: [], roomMq: null, roomSeen: false,
    rooms: {}, p2p: null, p2pOk: false, iceOk: false, pc: null, p2pAt: 0,
    p2pTries: 0, p2pTimer: null, p2pClosing: false, p2pReqCount: 0,
    clientP2PTimer: null, lastRtcReqAt: 0, lastHostRtcReq: 0, rtcOfferSdp: '', directFail: false, iceQ: [],
    myTeam: 0, remoteTeam: 1, remoteIdx: 9, matchSize: 11,
    remote: null,
    inp: { x: 0, y: 0, sp: 0 },
    act: [],
    lastSend: 0, lastInpAt: 0, lastInpRecv: 0, lastRecv: 0,
    inSeq: 0, lastInSeq: 0,
    snaps: [], interpDelay: 0.16, clockOff: null, snapGap: 0, snap: null, snapAt: 0,
    lastSnapTs: 0, replayBuf: [], replayOn: false, replayIdx: 0, localKickAt: 0, localHoldAt: 0,
    lastMqSnap: 0, clientSess: '', session: '',
    rtt: 0, pingAt: 0,
    bnKey: '', endShown: false, matchStarted: false,
    keepT: null, joinT: null, badgeT: 0
  };
  function Rp(code, ch){ return NS + code + '/' + (ch||''); }
  function setRtt(ms){
    // 后台标签页的 setTimeout/rAF 会被浏览器节流，偶尔测出 2~5 秒的假延迟；
    // 这种值不能直接显示成网络延迟。只接受 1.5 秒内的样本，并做平滑。
    if(!(ms > 0) || ms > 1500) return;
    NET.rtt = Math.round(NET.rtt ? NET.rtt * 0.7 + ms * 0.3 : ms);
  }
  function genCode(){
    var chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789', s = '';
    for(var i=0;i<4;i++) s += chars.charAt((Math.random()*chars.length)|0);
    return s;
  }
  function el(id){ return document.getElementById(id); }
  function setStatus(text, ok){
    var e = el('net-status');
    if(!e) return;
    e.textContent = text || '';
    e.style.color = (ok === false) ? '#ff9d9d' : '#9fe6ff';
  }
  function sleep(ms){ return new Promise(function(r){ setTimeout(r, ms); }); }

  // ---------------- 连接信令服务器（并行多路，能连上的全部保留） ----------------
  function connectRelays(timeoutMs){
    return new Promise(function(resolve, reject){
      var ok = [], done = false, firstAt = 0, t0 = Date.now(), pending = [];
      var finish = function(){
        if(done) return; done = true;
        clearInterval(tick);
        pending.forEach(function(m){ if(ok.indexOf(m) < 0){ try{ m.close(); }catch(_){} } });
        if(ok.length){
          NET.mqs = ok;
          NET.broker = ok.map(function(m){ return m.url; }).join(' | ');
          resolve(ok);
        } else {
          reject(new Error('房间服务器连不上（换个网络或稍后重试）'));
        }
      };
      var tick = setInterval(function(){
        if(done) return;
        if(ok.length && Date.now() - firstAt > 2500) finish();          // 有一个通了就再等 2.5s 收其它路
        if(Date.now() - t0 > (timeoutMs || 9000) + 600) finish();        // 总超时
      }, 150);
      BROKERS.forEach(function(url){
        var mq = new MiniMQTT(url, 'zhuj_' + Math.random().toString(36).slice(2, 10));
        pending.push(mq);
        mq.connect(timeoutMs || 9000).then(function(){
          if(done){ try{ mq.close(); }catch(_){}; return; }
          ok.push(mq);
          if(!firstAt) firstAt = Date.now();
        }).catch(function(){
          try{ mq.close(); }catch(_){}
        });
      });
    });
  }
  function netPub(topic, payload, retain){
    var arr = (NET.mqs && NET.mqs.length) ? NET.mqs : (NET.mq ? [NET.mq] : []);
    for(var i=0;i<arr.length;i++){ try{ arr[i].publish(topic, payload, retain); }catch(_){} }
  }

  /* ============================================================
     点对点直连（WebRTC DataChannel）
     - 信令借用房间服务器（只在开局交换 SDP/ICE，量极小）
     - 直连通道只跑输入流和快照流；不挂 TURN、不跑 MQTT 数据中继
     - 直连建立失败会多轮重试；仍然失败就明确提示，不偷偷回落到高延迟中继
     ============================================================ */
  var ICE = {
    iceServers: [
      { urls: 'stun:stun.chat.bilibili.com:3478' },   // 国内实测能拿到 srflx
      { urls: 'stun:stun.hitv.com:3478' },            // 国内实测能拿到 srflx
      { urls: 'stun:stun.miwifi.com:3478' },           // 国内实测能拿到 srflx
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun.cloudflare.com:3478' },
      { urls: 'stun:global.stun.twilio.com:3478' },
      { urls: 'stun:stun.nextcloud.com:443' }
    ],
    iceCandidatePoolSize: 4,
    bundlePolicy: 'max-bundle',
    rtcpMuxPolicy: 'require',
    iceTransportPolicy: 'all'          // 明确不用 relay/TURN
  };
  function rtcOK(){ return typeof RTCPeerConnection === 'function'; }
  function p2pSend(data){
    if(NET.p2p && NET.p2p.readyState === 'open'){
      try{ NET.p2p.send(data); return true; }catch(_){}
    }
    return false;
  }
  function p2pKill(keepIce){
    NET.p2pClosing = true;
    NET.p2pOk = false;
    NET.iceOk = false;
    clearTimeout(NET.p2pTimer);
    clearTimeout(NET.clientP2PTimer);
    if(NET.p2p){ try{ NET.p2p.close(); }catch(_){} NET.p2p = null; }
    if(NET.pc){ try{ NET.pc.close(); }catch(_){} NET.pc = null; }
    if(!keepIce) NET.iceQ = [];
    NET.p2pClosing = false;
  }
  function p2pBind(ch){
    NET.p2p = ch;
    ch.binaryType = 'arraybuffer';
    ch.onopen = function(){
      if(NET.p2p !== ch) return;
      NET.p2pOk = true;
      NET.p2pAt = performance.now();
      NET.p2pTries = 0;
      NET.directFail = false;
      NET.p2pReqCount = 0;
      clearTimeout(NET.clientP2PTimer);
      if(typeof sayComment === 'function') sayComment('⚡ 直连已建立，低延迟模式');
    };
    ch.onclose = function(){
      if(NET.p2pClosing || !NET.p2p || NET.p2p !== ch) return;
      NET.p2pOk = false;
      if(NET.role === 'host' && NET.pc){
        if(typeof sayComment === 'function') sayComment('⚠ 直连断开，正在重连…');
        retryRtc(NET.pc);
      } else if(NET.role === 'client'){
        if(typeof sayComment === 'function') sayComment('⚠ 直连断开，正在请求重连…');
        requestDirectRestart();
      }
    };
    ch.onmessage = function(ev){
      if(NET.p2p !== ch) return;
      if(typeof ev.data === 'string') p2pMsg(ev.data);
    };
  }
  function p2pMsg(s){
    if(!s) return;
    var c0 = s.charAt(0);
    if(c0 === 's'){
      if(NET.role === 'client'){ var o = null; try{ o = JSON.parse(s.slice(1)); }catch(_){ return; } clientOnSnapshot(o); }
      return;
    }
    if(c0 === 'i'){ if(NET.role === 'host') hostOnInput(s.slice(1)); return; }
    if(c0 === 'j'){
      if(NET.role === 'host'){
        var m2=null; try{ m2=JSON.parse(s.slice(1)); }catch(_){ return; }
        if(m2 && m2.t === 'skip'){ if(G.state === 'replay') G.stateT = 0.001; return; }
        hostOnCtl(m2);
      }
      return;
    }
    if(c0 === 'p'){ if(s.length > 1) p2pSend('o' + s.slice(1)); return; }
    if(c0 === 'o'){ var n = +s.slice(1); if(n) setRtt(performance.now() - n); return; }
  }
  function rtcJSON(c){
    if(!c) return null;
    try{ if(c.toJSON) return c.toJSON(); }catch(_){}
    return { candidate: c.candidate, sdpMid: c.sdpMid, sdpMLineIndex: c.sdpMLineIndex };
  }
  function signalUp(obj){
    if(!NET.code || !obj) return;
    var s = JSON.stringify(obj);
    var arr = (NET.mqs && NET.mqs.length) ? NET.mqs : (NET.mq ? [NET.mq] : []);
    for(var i=0;i<arr.length;i++){ try{ arr[i].publish(Rp(NET.code,'c2h'), s); }catch(_){} }
  }
  function signalDown(obj){
    if(!NET.code || !obj) return;
    netPub(Rp(NET.code,'h2c'), JSON.stringify(obj));
  }
  function rtcAddIce(pc, c){
    if(!c) return;
    try{
      if(!pc || !pc.remoteDescription){
        NET.iceQ.push(c);
      } else {
        pc.addIceCandidate(new RTCIceCandidate(c)).catch(function(){});
      }
    }catch(_){}
  }
  function rtcFlushIce(pc){
    var q = NET.iceQ || [];
    NET.iceQ = [];
    for(var i=0;i<q.length;i++){
      try{ pc.addIceCandidate(new RTCIceCandidate(q[i])).catch(function(){}); }catch(_){}
    }
  }
  function rtcWireIce(pc, up){
    pc.onicecandidate = function(e){
      if(!e.candidate || NET.pc !== pc || !NET.code) return;
      var c = rtcJSON(e.candidate);
      if(!c) return;
      var m = { t:'rtcIce', c:c };
      if(up) signalUp(m); else signalDown(m);
    };
  }
  function rtcHostIce(c){
    if(NET.role !== 'host') return;
    rtcAddIce(NET.pc, c);
  }
  function rtcClientIce(c){
    if(NET.role !== 'client' && NET.role !== 'client-wait') return;
    rtcAddIce(NET.pc, c);
  }
  function retryRtc(pc){
    if(NET.role !== 'host' || NET.pc !== pc) return;
    if(NET.p2pOk) return;
    if((NET.p2pTries || 0) >= 3){
      if(!NET.directFail){
        NET.directFail = true;
        setStatus('⚠ 直连失败：双方换网络或开热点后重试', false);
        signalDown({ t:'directFail' });
        if(typeof sayComment === 'function') sayComment('⚠ 直连失败，请双方换网络/开手机热点后重试');
      }
      return;
    }
    NET.p2pTries = (NET.p2pTries || 0) + 1;
    clearTimeout(NET.p2pTimer);
    setTimeout(function(){ if(NET.role === 'host' && NET.pc === pc) rtcHostStart(); }, 1400);
  }
  function requestDirectRestart(){
    if(NET.role !== 'client' || !NET.code) return;
    var now = performance.now();
    if(now - (NET.lastRtcReqAt || 0) < 3500) return;
    NET.lastRtcReqAt = now;
    signalUp({ t:'rtcReq' });
  }
  function rtcHostStart(){
    if(!rtcOK() || !NET.code) return;
    p2pKill();
    var pc;
    try{ pc = new RTCPeerConnection(ICE); }catch(_){ return; }
    NET.pc = pc;
    p2pBind(pc.createDataChannel('g', { ordered: false, maxRetransmits: 0 }));
    rtcWireIce(pc, false);
    pc.oniceconnectionstatechange = function(){
      var st = pc.iceConnectionState;
      if(st === 'connected' || st === 'completed'){ NET.iceOk = true; NET.p2pTries = 0; }
      else if(st === 'failed'){ NET.iceOk = false; NET.p2pOk = false; retryRtc(pc); }
    };
    pc.onconnectionstatechange = function(){
      if(pc.connectionState === 'failed'){ NET.iceOk = false; NET.p2pOk = false; retryRtc(pc); }
    };
    pc.createOffer()
      .then(function(o){ return pc.setLocalDescription(o); })
      .then(function(){
        if(!NET.code || NET.pc !== pc || !pc.localDescription) return;
        // 立即发 offer，候选用 trickle 边收边发，不再等 2.5 秒 gather
        signalDown({ t:'rtc', sdp:{ type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
        clearTimeout(NET.p2pTimer);
        NET.p2pTimer = setTimeout(function(){
          if(NET.p2pOk || NET.pc !== pc) return;
          retryRtc(pc);
        }, 6500);
      }).catch(function(){});
  }
  function rtcClientStart(desc){
    if(!rtcOK() || !desc || !desc.sdp) return;
    // 多路信令会送来同一份 offer，不能每份都重建 PC，否则自己取消自己。
    if(NET.pc && NET.rtcOfferSdp === desc.sdp) return;
    NET.rtcOfferSdp = desc.sdp;
    p2pKill(true);   // 保留 offer 之前先到的 ICE，setRemote 后再统一灌入
    var pc;
    try{ pc = new RTCPeerConnection(ICE); }catch(_){ return; }
    NET.pc = pc;
    pc.ondatachannel = function(ev){ p2pBind(ev.channel); };
    rtcWireIce(pc, true);
    pc.oniceconnectionstatechange = function(){
      var st = pc.iceConnectionState;
      if(st === 'connected' || st === 'completed'){ NET.iceOk = true; }
      else if(st === 'failed'){ NET.iceOk = false; NET.p2pOk = false; requestDirectRestart(); }
    };
    pc.onconnectionstatechange = function(){
      if(pc.connectionState === 'failed'){ NET.iceOk = false; NET.p2pOk = false; requestDirectRestart(); }
    };
    pc.setRemoteDescription(new RTCSessionDescription(desc))
      .then(function(){ rtcFlushIce(pc); return pc.createAnswer(); })
      .then(function(a){ return pc.setLocalDescription(a); })
      .then(function(){
        if(!NET.code || NET.pc !== pc || !pc.localDescription) return;
        signalUp({ t:'rtcAns', sdp:{ type: pc.localDescription.type, sdp: pc.localDescription.sdp } });
        clearTimeout(NET.clientP2PTimer);
        var clientRetry = function(){
          if(NET.p2pOk || NET.role !== 'client') return;
          if((NET.p2pReqCount || 0) >= 2) return;
          NET.p2pReqCount = (NET.p2pReqCount || 0) + 1;
          requestDirectRestart();
          NET.clientP2PTimer = setTimeout(clientRetry, 4200);
        };
        NET.clientP2PTimer = setTimeout(clientRetry, 5000);
      }).catch(function(){});
  }
  function rtcHostAnswer(desc){
    if(!NET.pc || !desc || !desc.sdp) return;
    if(NET.pc.remoteDescription) return;   // 多路信令的重复 answer
    try{
      NET.pc.setRemoteDescription(new RTCSessionDescription(desc))
        .then(function(){ rtcFlushIce(NET.pc); })
        .catch(function(){});
    }catch(_){}
  }
  // 联机数据只走直连；中继只做房间和信令，绝不承载输入/快照。
  function sendUp(str){ p2pSend('i' + str); }
  function sendUpJSON(obj){
    if(!obj) return;
    // 重连请求必须走信令（此时直连已经断了）
    if(obj.t === 'rtcReq'){ signalUp(obj); return; }
    p2pSend('j' + JSON.stringify(obj));
  }
  function sendSnap(payload){ p2pSend('s' + payload); }

  // ---------------- 面板操作 ----------------
  window.setOnlineMode = function(mode){
    if(NET.role){ setStatus('已在房间中，退出后才能切换人数', false); return; }
    NET.matchSize = mode === 3 ? 3 : 11;
    G.matchSize = NET.matchSize;
    G.matchMode = NET.matchSize === 3 ? '3v3' : '11v11';
    var p = el('online-panel');
    var ttl = p && p.querySelector('h2');
    if(ttl) ttl.textContent = NET.matchSize === 3 ? '🌐 联机对战 · 3v3 小队' : '🌐 联机对战 · 11v11';
    var b11 = el('mode-11'), b3 = el('mode-3');
    if(b11){ b11.classList.toggle('primary', NET.matchSize !== 3); }
    if(b3){ b3.classList.toggle('primary', NET.matchSize === 3); }
    setStatus(NET.matchSize === 3 ? '3v3：门将 + 后卫 + 前锋，双方各操控一队。' : '房号即房间钥匙，把它发给朋友即可联机。');
  };
  window.openOnline = function(mode){
    var p = el('online-panel');
    if(!p) return;
    p.classList.remove('hidden');
    var nk = el('net-nick');
    if(nk && !nk.value) nk.value = '球员' + (Math.floor(Math.random()*90) + 10);
    window.setOnlineMode(mode === 3 ? 3 : 11);
    window.netRefreshRooms();
  };
  window.closeOnline = function(){
    var p = el('online-panel');
    if(p) p.classList.add('hidden');
  };

  // ---------------- 公开房间浏览 ----------------
  function renderRoomList(){
    var list = el('room-list');
    if(!list) return;
    var nowMs = Date.now();
    var codes = Object.keys(NET.rooms || {}).filter(function(c){
      var r = NET.rooms[c]; return r && r.t && (nowMs - r.t) < 90000;   // 90 秒内有心跳才算活跃
    });
    if(!codes.length){
      list.innerHTML = '<div class="rb-empty">暂无公开房间 · 你可以创建一个</div>';
      return;
    }
    codes.sort(function(a,b){ return (NET.rooms[b].t||0) - (NET.rooms[a].t||0); });
    list.innerHTML = '';
    codes.slice(0, 12).forEach(function(code){
      var r = NET.rooms[code];
      var row = document.createElement('div');
      row.className = 'rb-row';
      var rc = document.createElement('span'); rc.textContent = code; rc.className = 'rcode';
      var rm = document.createElement('span');
      rm.className = 'rmeta';
      rm.textContent = (r.n || '玩家') + ' · 直连房间';
      row.appendChild(rc); row.appendChild(rm);
      row.onclick = function(){
        var inp = el('join-code');
        if(inp) inp.value = code;
        setStatus('已选房间 ' + code + '，点“加入房间”');
      };
      list.appendChild(row);
    });
  }
  window.netRefreshRooms = function(){
    if(NET.role) return;
    var list = el('room-list');
    if(list) list.innerHTML = '<div class="rb-empty">搜索公开房间中…</div>';
    NET.rooms = {};
    var have = (NET.mqs && NET.mqs.length) ? Promise.resolve(NET.mqs) : connectRelays();
    have.then(function(mqs){
      NET.mqs = mqs;
      mqs.forEach(function(mq){
        mq.onmsg = function(t, p){ onRelayMsg(t, p, mq); };
        try{ mq.subscribe('zhujcup2026/#'); }catch(_){}
      });
      return sleep(1800);
    }).then(function(){
      renderRoomList();
    }).catch(function(){
      if(list) list.innerHTML = '<div class="rb-empty">房间服务器连不上（检查网络后重试）</div>';
    });
  };
  window.netCreateRoom = function(){
    if(NET.role){ setStatus('已在房间中，请先退出'); return; }
    var nick = ((el('net-nick') && el('net-nick').value) || '').trim().slice(0,10) || '球员';
    var btn = el('btn-create-room');
    if(btn) btn.disabled = true;
    setStatus('正在连接房间服务器…');
    var haveC = (NET.mqs && NET.mqs.length) ? Promise.resolve(NET.mqs) : connectRelays();
    haveC.then(function(mqs){
      NET.mq = mqs[0]; NET.role = 'host'; NET.nick = nick;
      NET.myTeam = G.userTeam; NET.code = genCode();
      NET.session = 'h' + Math.random().toString(36).slice(2, 10);
      mqs.forEach(function(mq){
        mq.onmsg = function(t, p){ onRelayMsg(t, p, mq); };
        mq.subscribe(Rp(NET.code, 'c2h'));
        mq.publish(Rp(NET.code, ''), JSON.stringify({ n: nick, t: Date.now() }), true);
      });
      var rs = el('room-show'); if(rs) rs.classList.remove('hidden');
      var rc = el('room-code'); if(rc) rc.textContent = NET.code;
      setStatus('房间已创建 · 等待对手加入 · 直连建立后开赛');
      if(btn) btn.disabled = false;
      NET.keepT = setInterval(function(){
        if(NET.role === 'host' && NET.code) netPub(Rp(NET.code,''), JSON.stringify({ n: nick, t: Date.now() }), true);
      }, 20000);
    }).catch(function(e){
      setStatus('❌ ' + (e.message || '连接失败') + '（检查网络后重试）', false);
      if(btn) btn.disabled = false;
    });
  };
  window.netJoinRoom = function(){
    if(NET.role){ setStatus('已在房间中，请先退出'); return; }
    var code = ((el('join-code') && el('join-code').value) || '').trim().toUpperCase();
    if(!/^[A-Z0-9]{4}$/.test(code)){ setStatus('请输入 4 位房号', false); return; }
    var nick = ((el('net-nick') && el('net-nick').value) || '').trim().slice(0,10) || '球员';
    var btn = el('btn-join-room');
    if(btn) btn.disabled = true;
    setStatus('正在连接房间服务器…');
    NET.roomSeen = false; NET.roomMq = null;
    var haveJ = (NET.mqs && NET.mqs.length) ? Promise.resolve(NET.mqs) : connectRelays();
    haveJ.then(function(mqs){
      NET.nick = nick; NET.code = code;
      NET.session = 'c' + Math.random().toString(36).slice(2, 10);
      mqs.forEach(function(mq){
        mq.onmsg = function(t, p){ onRelayMsg(t, p, mq); };
        mq.subscribe(Rp(code, 'h2c'));
        mq.subscribe(Rp(code, ''));
      });
      return sleep(1600);
    }).then(function(){
      if(!NET.roomSeen || !NET.roomMq){
        setStatus('❌ 房间不存在或已关闭（检查房号）', false);
        (NET.mqs || []).forEach(function(m){ try{ m.close(); }catch(_){} });
        NET.mqs = []; NET.mq = null; NET.code = '';
        if(btn) btn.disabled = false;
        return;
      }
      // 保留多路信令：只用于 SDP/ICE 交换，某条 broker 抽风不影响直连数据。
      NET.mq = NET.roomMq;
      // mqs 只是上一个 .then 的局部变量，这里不能引用；NET.mqs 已由 connectRelays 保留。
      NET.role = 'client-wait';
      setStatus('已找到房间 · 正在握手…');
      var tries = 0;
      var sendJoin = function(){
        if(NET.role === 'client'){ clearInterval(NET.joinT); NET.joinT = null; return; }
        if(++tries > 24){
          clearInterval(NET.joinT); NET.joinT = null;
          setStatus('❌ 房主无响应，请重试', false);
          (NET.mqs || []).forEach(function(m){ try{ m.close(); }catch(_){} });
          NET.mq = null; NET.mqs = []; NET.role = null;
          if(btn) btn.disabled = false;
          return;
        }
        var arr = (NET.mqs && NET.mqs.length) ? NET.mqs : (NET.mq ? [NET.mq] : []);
        for(var ai=0; ai<arr.length; ai++){ try{ arr[ai].publish(Rp(NET.code, 'c2h'), JSON.stringify({ t: 'j', n: NET.nick, sess: NET.session })); }catch(_){} }
      };
      sendJoin();
      NET.joinT = setInterval(sendJoin, 500);
    }).catch(function(e){
      setStatus('❌ ' + (e.message || '连接失败'), false);
      if(btn) btn.disabled = false;
    });
  };
  window.copyRoomCode = function(){
    var c = NET.code || (el('room-code') && el('room-code').textContent) || '';
    if(!c) return;
    try{
      if(navigator.clipboard) navigator.clipboard.writeText(c);
      else{
        var t = document.createElement('textarea'); t.value = c; document.body.appendChild(t);
        t.select(); document.execCommand('copy'); document.body.removeChild(t);
      }
      setStatus('房间号已复制：' + c);
    }catch(_){ setStatus('复制失败，请手动记下：' + c, false); }
  };
  window.netExitRoom = function(){
    p2pKill();
    if(NET.keepT){ clearInterval(NET.keepT); NET.keepT = null; }
    if(NET.joinT){ clearInterval(NET.joinT); NET.joinT = null; }
    if(NET.mqs && NET.mqs.length){
      NET.mqs.forEach(function(m){
        try{
          if(NET.role === 'host' && NET.code) m.publish(Rp(NET.code,''), '', true);
          m.close();
        }catch(_){}
      });
    } else if(NET.mq){
      try{
        if(NET.role === 'host' && NET.code) NET.mq.publish(Rp(NET.code,''), '', true);
        NET.mq.close();
      }catch(_){}
    }
    if(NET.remote){ NET.remote.remoteCtl = false; }
    NET.role = null; NET.mq = null; NET.mqs = []; NET.roomMq = null; NET.remote = null; NET.code = '';
    NET.snaps.length = 0; NET.matchStarted = false;
    var rs = el('room-show'); if(rs) rs.classList.add('hidden');
    var nb = el('net-badge'); if(nb) nb.style.display = 'none';
    setStatus('已退出联机');
    // 回到菜单
    var menu = el('menu'); if(menu) menu.classList.remove('hidden');
    var end = el('end'); if(end) end.classList.add('hidden');
    if(typeof hideBanner === 'function') hideBanner();
  };

  // ---------------- 消息分发 ----------------
  function onRelayMsg(topic, payload, src){
    if(!payload) return;
    if(topic.slice(-3) === 'h2c'){
      if(NET.role === 'host') return;   // 主机绝不处理下行：多路中继 + 通配符订阅会把自己发的 acc/快照收回来
      if(payload.charAt(0) === '{'){
        var o = null;
        try{ o = JSON.parse(payload); }catch(_){ return; }
        if(o.t === 'acc') clientOnAccept(o);
        else if(o.t === 'rst') clientOnReset();
        else if(o.t === 'rtc'){ if(NET.role === 'client' || NET.role === 'client-wait') rtcClientStart(o.sdp); }
        else if(o.t === 'rtcIce'){ rtcClientIce(o.c); }
        else if(o.t === 'directFail'){
          NET.directFail = true;
          setStatus('⚠ 直连失败：双方换网络或开热点后重试', false);
          if(typeof sayComment === 'function') sayComment('⚠ 直连失败，请双方换网络/开手机热点后重试');
        }
        // 注意：h2c 里不再处理 o.t === 's'，快照只允许走直连
      }
      return;
    }
    if(topic.slice(-3) === 'c2h'){
      if(NET.role !== 'host') return;
      if(payload.charAt(0) !== '{') return;   // 输入/快照不再走中继
      try{
        var m = JSON.parse(payload);
        if(m.t === 'j') hostOnJoin(m);
        else if(m.t === 'sel'){ if(NET.role === 'host') hostOnCtl(m); }
        else if(m.t === 'skip'){ if(NET.role === 'host' && G.state === 'replay') G.stateT = 0.001; }
        else if(m.t === 'rtcAns'){ if(NET.role === 'host') rtcHostAnswer(m.sdp); }
        else if(m.t === 'rtcIce'){ rtcHostIce(m.c); }
        else if(m.t === 'rtcReq'){
          if(NET.role === 'host' && !NET.p2pOk && NET.remote && !NET.directFail){
            var now = performance.now();
            if(now - (NET.lastHostRtcReq || 0) > 3000){
              NET.lastHostRtcReq = now;
              rtcHostStart();
            }
          }
        }
      }catch(_){}
      return;
    }
    if(NET.role !== 'host'){
      NET.roomSeen = true;   // 房间存在（retained）
      if(src) NET.roomMq = src;
      // 浏览模式：登记公开房间
      var mm = topic.match(/^zhujcup2026\/([A-Z0-9]{4})\/$/);
      if(mm && NET.role === null && payload && payload.charAt(0) === '{'){
        var code = mm[1], nick = '玩家', ts = 0;
        try{ var mo = JSON.parse(payload); if(mo.n) nick = mo.n; ts = mo.t || 0; }catch(_){}
        var cur = NET.rooms[code] || { n: nick, t: 0, hops: 0 };
        cur.hops = (cur.hops || 0) + 1;
        cur.n = nick;
        if(ts > cur.t) cur.t = ts;
        NET.rooms[code] = cur;
        renderRoomList();
      }
    }
  }

  // ---------------- 主机 ----------------
  function hostOnJoin(m){
    var sess = (m && m.sess) ? String(m.sess) : '';
    // 已在比赛中还收到握手（多半是应答丢了）：原样重发应答，保证客户端一定能连上。
    // 如果是新页面的客机（session 变了），重置输入序号，否则新客机的 seq 从 1 开始，
    // 会被主机的旧序号挡住，表现为“能进场但完全动不了”。
    if(NET.matchStarted){
      if(sess && NET.clientSess && sess !== NET.clientSess){
        NET.clientSess = sess;
        NET.lastInSeq = 0;
        NET.lastMqSnap = 0;
      }
      netPub(Rp(NET.code,'h2c'), JSON.stringify({ t:'acc', team: NET.remoteTeam, sess: sess || NET.clientSess || '', sz: G.matchSize }));
      if(!NET.p2pOk) rtcHostStart();   // 直连没建起来就再喊一次
      return;
    }
    NET.clientSess = sess;
    NET.oppNick = (m.n || '对手').toString().slice(0, 10);
    NET.matchStarted = true;
    NET.remoteTeam = 1 - NET.myTeam;
    G.userTeam = NET.myTeam;
    // 房主创建房间时决定 11v11 / 3v3；客户端不能自己改，避免两边人数不一致
    G.matchSize = NET.matchSize === 3 ? 3 : 11;
    G.matchMode = G.matchSize === 3 ? '3v3' : '11v11';
    G.levelMode = false;
    startGame();
    if(NET.remote) NET.remote.remoteCtl = false;
    NET.remoteIdx = G.matchSize === 3 ? 2 : 9;
    NET.remote = G.teams[NET.remoteTeam][NET.remoteIdx];
    if(NET.remote){
      NET.remote.remoteCtl = true;
      NET.remote.facing = NET.remote.team === 0 ? 0 : Math.PI;
      // 客机操控的球员脚下亮光环
      if(NET.remote.mesh && NET.remote.mesh.userData){
        NET.remote.mesh.userData.ring.visible = true;
        if(NET.remote.mesh.userData.ringGlow) NET.remote.mesh.userData.ringGlow.visible = true;
      }
    }
    NET.lastInpRecv = performance.now();
    NET.lastSend = 0; NET.snaps.length = 0;
    NET.inSeq = 0; NET.lastInSeq = 0; NET.lastMqSnap = 0;
    NET.p2pTries = 0; clearTimeout(NET.p2pTimer);
    NET.directFail = false; NET.p2pReqCount = 0; NET.rtcOfferSdp = ''; NET.iceQ = [];
    netPub(Rp(NET.code,'h2c'), JSON.stringify({ t:'acc', team: NET.remoteTeam, sess: sess, sz: G.matchSize }));
    closeOnline();
    showBadge();
    rtcHostStart();   // 只尝试点对点直连；失败会重试，不回落中继
    if(typeof sayComment === 'function') sayComment('🌐 对手「' + NET.oppNick + '」已加入，正在建立直连…');
  }
  function hostOnInput(str){
    if(str.charAt(0) === 'i') str = str.slice(1);   // 兼容带前缀/已剥前缀
    var parts = str.split('|');
    // 新页面客机的 session 不同：旧客户端的迟到输入不能继续操控新客机。
    // 必须先做 session 过滤，再写 lastInSeq；否则旧 session 的高序号会污染序号，
    // 新客机的 seq=1 会被误判成“旧包”而永远被丢掉。
    if(parts.length > 6 && parts[6] && NET.clientSess && parts[6] !== NET.clientSess) return;
    // 直连包乱序到达时按序号去重，旧包不能把球员往回拉。
    if(parts.length > 5){
      var sq = parts[5] | 0;
      var last = NET.lastInSeq || 0;
      if(sq){
        if(sq === last) return;
        if(last && sq < last && (last - sq) < 0x40000000) return;
        NET.lastInSeq = sq;
      }
    }
    NET.inp.x = +parts[0] || 0;
    NET.inp.y = +parts[1] || 0;
    NET.inp.sp = +parts[2] || 0;
    var acts = parts[3] || '';
    for(var i=0;i<acts.length;i++) NET.act.push(acts.charAt(i));
    // 操控编号也每帧带一份：切人消息丢了也不会出现“客机在控 A、主机还在控 B”
    if(parts.length > 4){
      var si = parts[4] | 0;
      var sz = (G.teams[NET.remoteTeam] || []).length;
      if(si >= 0 && si < sz && (!NET.remote || NET.remote.idx !== si)) hostOnCtl({ t:'sel', i:si });
    }
    NET.lastInpRecv = performance.now();
  }
  // 客机控制消息（切换球员等）
  function hostOnCtl(m){
    if(!m) return;
    if(m.t === 'sel'){
      var arr = G.teams[NET.remoteTeam];
      var idx = (m.i|0);
      if(arr && idx >= 0 && idx < arr.length && arr[idx].role !== 'GK'){
        // 旧操控对象光环熄灭，同时解除“客机操控”标记，
        // 否则切人后旧球员不再受 AI 控制，会永远钉在原地
        if(NET.remote){
          if(NET.remote.mesh && NET.remote.mesh.userData){
            NET.remote.mesh.userData.ring.visible = false;
            if(NET.remote.mesh.userData.ringGlow) NET.remote.mesh.userData.ringGlow.visible = false;
          }
          NET.remote.remoteCtl = false;
        }
        NET.remote = arr[idx];
        NET.remote.remoteCtl = true;
        // 和单机切人一致：默认面朝对方球门，避免切人后第一次传球变成回传
        NET.remote.facing = NET.remote.team === 0 ? 0 : Math.PI;
        // 新操控对象脚下亮光环（和单机一样）
        if(NET.remote.mesh && NET.remote.mesh.userData){
          NET.remote.mesh.userData.ring.visible = true;
          if(NET.remote.mesh.userData.ringGlow) NET.remote.mesh.userData.ringGlow.visible = true;
        }
      }
    }
  }
  function hostPreTick(dt){
    var p = NET.remote;
    if(!p) return;
    // 只有比赛进行中才认客机输入；开球倒计时/进球庆祝/回放/中场都冻结，
    // 否则主机会在客机已经停下时继续把人往前推，两边画面必然对不上
    if(G.state !== 'play' || G.paused){
      NET.inp.x = 0; NET.inp.y = 0; NET.inp.sp = 0;
      NET.act.length = 0;   // 倒计时/庆祝/回放时按的键不算，避免开球瞬间突然一脚
      p.velx = 0; p.velz = 0;
      return;
    }
    // 输入断流（客机切后台/丢包）超过 140ms 就松开所有方向键，
    // 不能让最后一条输入在主机上一直生效，把人推出场外
    if(NET.lastInpRecv && performance.now() - NET.lastInpRecv > 140){
      NET.inp.x = 0; NET.inp.y = 0; NET.inp.sp = 0;
    }
    // 移动（与玩家操控同一套速度/朝向规则）
    var spd = NET.inp.sp ? (SPRINT_SPD + USER_SPRINT_BOOST) : WALK_SPD;
    var len = Math.hypot(NET.inp.x, NET.inp.y);
    if(len > 0.01){
      p.pos.x += NET.inp.x / len * spd * dt;
      p.pos.z += NET.inp.y / len * spd * dt;
      p.facing = Math.atan2(NET.inp.y, NET.inp.x);
      p.velx = NET.inp.x / len * spd; p.velz = NET.inp.y / len * spd;
    } else if(ball.passTarget === p || (!ball.owner && dist2D(p.pos.x, p.pos.z, ball.pos.x, ball.pos.z) < 10 && ball.speed < 20)){
      // 和单机 userMove 的“不操作也自动迎球”一致：传球朝你飞来/球滚到脚边时自动跑过去
      moveAI(p, {x:ball.pos.x, z:ball.pos.z}, dt, true);
    } else {
      p.velx = 0; p.velz = 0;
    }
    clampPlayer(p);
    // 拾球（与玩家同一规则：自己控制的球员只在脚下 1.55m 内接球，不能隔空吸球）
    if(!ball.owner && ball.speed < 22 &&
       !(typeof selfPassLock === 'function' && selfPassLock(p)) &&
       dist2D(p.pos.x, p.pos.z, ball.pos.x, ball.pos.z) < PICKUP_R){
      ball.owner = p; p.owns = true; ball.passTarget = null; G.oppPasses = 0; p.decideT = 0.1;
      ball.vel.x = 0; ball.vel.y = 0; ball.vel.z = 0;
    }
    // 动作（与 userShoot / userPass 完全同一套参数）
    // 客机的按键会晚 RTT/2 才到，如果这半拍里主机判定“没控球”，客机会白按一次；
    // 所以球就在脚边时补一个宽容拾球，保证联机和单机一样按得动
    if(NET.act.length && G.state === 'play'){
      if(!p.owns && !ball.owner && ball.speed < 22 &&
         dist2D(p.pos.x, p.pos.z, ball.pos.x, ball.pos.z) < PICKUP_R){
        ball.owner = p; p.owns = true; ball.passTarget = null; ball.vel.x = 0; ball.vel.y = 0; ball.vel.z = 0;
      }
      if(!p.owns){
        // 客机刚切人/球还没到脚边时按的射门/传球，不要立刻清掉：
        // 给 350ms 等球权到手；如果一直没球再丢弃，避免下一脚误触。
        if(!NET.actHoldAt) NET.actHoldAt = performance.now();
        if(performance.now() - NET.actHoldAt > 350){
          NET.act.length = 0; NET.actHoldAt = 0;
        }
        return;
      }
      NET.actHoldAt = 0;
      var acts = NET.act.splice(0);
      for(var i=0;i<acts.length;i++){
        if(acts[i] === 's'){
          var gx = oppGoalX(p.team);
          // 客机把射门参数（落点 z / 力量 / 弧线）一起发过来，主机照做，
          // 两边看到的是同一条射门轨迹；旧端没带参数就随机
          var spec = '';
          while(i + 1 < acts.length && acts[i + 1] !== 'p' && acts[i + 1] !== 's'){ spec += acts[i + 1]; i++; }
          var vals = spec.split(',');
          var tz = (vals.length >= 3 && isFinite(+vals[0])) ? +vals[0] : (Math.random() < 0.5 ? -1 : 1) * (2.2 + Math.random() * 1.6);
          var pw = (vals.length >= 3 && isFinite(+vals[1])) ? +vals[1] : (28 + Math.random() * 4);
          var lf = (vals.length >= 3 && isFinite(+vals[2])) ? +vals[2] : (1.5 + Math.random() * 2);
          shootToward(p, gx, tz, pw, lf);
        } else if(acts[i] === 'p'){
          // 客机可能把传球目标编号一起发过来（p + 编号），这样两边传向同一个人。
          // 没有编号就退回主机自己选最佳目标（兼容旧端）。
          var num = '';
          while(i + 1 < acts.length && acts[i + 1] >= '0' && acts[i + 1] <= '9'){ num += acts[i + 1]; i++; }
          var t = (num !== '') ? (G.teams[p.team] && G.teams[p.team][+num]) : bestPassTarget(p, G.teams[p.team]);
          if(t && t !== p) passTo(p, t, 20);
        }
      }
    } else if(NET.act.length > 4){
      NET.act.length = 0;
    }
  }
  function r1(v){ return Math.round(v * 10) / 10; }
  function hostPostTick(){
    var now = performance.now();
    if(!NET.p2pOk){
      var nb0 = el('net-badge');
      if(nb0){
        nb0.textContent = (document.hidden ? '⚠ 窗口在后台会变慢 · ' : '') +
          (NET.directFail ? '⚠ 直连失败 · ' : '📡 正在建立直连 · ') + NET.code;
      }
      return;
    }
    // 只走直连 50Hz；中继不再承载任何比赛数据
    var gap = 20;
    if(now - NET.lastSend > gap){
      NET.lastSend = now;
      var ps = new Array((G.teams[0]?G.teams[0].length:11) + (G.teams[1]?G.teams[1].length:11)), n = 0;
      for(var ti=0; ti<2; ti++){
        var arr = G.teams[ti];
        for(var i=0;i<arr.length;i++){
          var p = arr[i];
          ps[n++] = [r1(p.pos.x), r1(p.pos.z), r1(p.facing), r1(p.velx || 0), r1(p.velz || 0)];
        }
      }
      var bn = (G.bannerT > 0 && el('banner-big')) ? [el('banner-big').textContent, (el('banner-sub')||{}).textContent || ''] : null;
      var sg = (G.state === 'goal' && G.scorerPlayer) ? [G.scorerPlayer.team, G.scorerPlayer.idx] : null;
      var bo = ball.owner ? [ball.owner.team, ball.owner.idx] : null;
      var pt = ball.passTarget ? [ball.passTarget.team, ball.passTarget.idx] : null;
      // 动画状态（滑铲/扑救、庆祝）和统计也一起发，客机才不会“人滑着走、门将不扑”
      var an = [];
      for(var ti2=0; ti2<2; ti2++){
        var ar2 = G.teams[ti2];
        for(var j2=0; j2<ar2.length; j2++){
          var q2 = ar2[j2];
          if((q2.celebrateT || 0) > 0.01 || (q2.diveT || 0) > 0.01) an.push([ti2, j2, r1(q2.celebrateT || 0), r1(q2.diveT || 0)]);
        }
      }
      var st2 = G.stats ? [r1(G.stats.possT[0]), r1(G.stats.possT[1]), G.stats.shots[0], G.stats.shots[1], G.stats.passes[0], G.stats.passes[1]] : null;
      sendSnap(JSON.stringify({
        t:'s', ts:Math.round(now), st:G.state, sc:G.score, ck:r1(G.clock), hf:G.half, stt:r1(Math.max(0, G.stateT)), tm:r1(G.time),
        lk:G.lastKickoff,
        bl:[r1(ball.pos.x), r1(ball.pos.y), r1(ball.pos.z), r1(ball.vel.x), r1(ball.vel.y), r1(ball.vel.z)],
        bo:bo, sg:sg, bn:bn, ps:ps, an:an, ss:st2, pt:pt
      }));
    }
    var nb = el('net-badge');
    if(nb){
      var off = NET.lastInpRecv && now - NET.lastInpRecv > 4000;
      nb.textContent = (document.hidden ? '⚠ 窗口在后台会变慢 · ' : '') + '⚡直连 ' +
        (off ? (NET.code + ' · ⚠ 对手可能离线') : (NET.code + (NET.rtt ? ' · ' + NET.rtt + 'ms' : '')));
    }
  }

  // ---------------- 客户端 ----------------
  function clientOnAccept(o){
    if(NET.role === 'client') return;
    if(NET.joinT){ clearInterval(NET.joinT); NET.joinT = null; }
    NET.role = 'client';
    NET.myTeam = (o.team | 0) || 0;
    NET.remoteTeam = 1 - NET.myTeam;
    G.userTeam = NET.myTeam;
    // 跟随房主的房间人数（3v3 / 11v11），否则客机会用 11 人阵容接 6 人快照
    G.matchSize = (o.sz === 3) ? 3 : 11;
    G.matchMode = G.matchSize === 3 ? '3v3' : '11v11';
    G.levelMode = false;
    NET.matchSize = G.matchSize;
    NET.remoteIdx = G.matchSize === 3 ? 2 : 9;
    NET.snaps.length = 0; NET.endShown = false; NET.bnKey = ''; NET.camInit = false;
    NET.directFail = false; NET.p2pReqCount = 0; NET.rtcOfferSdp = ''; NET.iceQ = [];
    NET.session = o.sess || NET.session || '';
    NET.lastInpAt = 0; NET.lastMqSnap = 0;
    NET.snap = null; NET.snapAt = 0; NET.lastSc = null; NET.lastClockTxt = '';
    NET.clockOff = null; NET.snapGap = 0; NET.interpDelay = 0.16;
    NET.lastSnapTs = 0; NET.replayBuf = []; NET.replayOn = false; NET.replayIdx = 0; NET.animPrev = null; NET.replaySeq = 0; NET.localKickAt = 0; NET.localHoldAt = 0;
    NET.inSeq = 0; NET.lastInSeq = 0;
    G.replayFrames = null;
    startGame();
    closeOnline();
    showBadge();
    if(typeof sayComment === 'function') sayComment('📡 已连上房主，正在建立直连…');
  }
  function clientOnReset(){
    NET.snaps.length = 0; NET.endShown = false; NET.bnKey = '';
    NET.snap = null; NET.snapAt = 0; NET.lastSc = null; NET.lastClockTxt = '';
    NET.clockOff = null; NET.snapGap = 0; NET.interpDelay = 0.16;
    NET.lastSnapTs = 0; NET.replayBuf = []; NET.replayOn = false; NET.replayIdx = 0; NET.animPrev = null; NET.replaySeq = 0; NET.localKickAt = 0; NET.localHoldAt = 0;
    NET.inSeq = 0; NET.lastInSeq = 0;
    G.replayFrames = null;
    startGame();
  }
  function lerpAngle(a, b, k){
    var d = b - a;
    while(d > Math.PI) d -= Math.PI * 2;
    while(d < -Math.PI) d += Math.PI * 2;
    return a + d * k;
  }
  // 客机：只保留“最新一张快照 + 到达时刻”。渲染时按现在外推，
  // 不再用“落后 interpDelay 的延迟插值”（那是画面慢半拍、对手像不动的根源）
  function clientOnSnapshot(o){
    if(!o || !o.ps) return;
    // 直连重复包可能各送一份；同一时间戳的旧快照直接丢掉，避免画面被拉回去抖一下
    if(o.ts && NET.lastSnapTs && o.ts <= NET.lastSnapTs) return;
    if(o.ts) NET.lastSnapTs = o.ts;
    var now = performance.now();
    if(NET.snapAt){
      var g = now - NET.snapAt;
      NET.snapGap = NET.snapGap ? NET.snapGap * 0.8 + g * 0.2 : g;
    }
    NET.lastRecv = now;
    NET.snap = o;
    NET.snapAt = now;
    clientRecordReplay(o);
    applySnapshotState(o);
    applySnapshotAnim(o);
  }
  // 客机自己录一份“进球前的比赛帧”，进球后用同一套回放镜头播，
  // 不然主机会播 2.2 秒回放，客机只能干看实时画面，两边完全不同步
  function clientRecordReplay(o){
    if(!o.ps || !o.bl) return;
    if(o.st !== 'play') return;
    NET.replaySeq = (NET.replaySeq || 0) + 1;
    if(NET.replaySeq % 2) return;      // 30Hz 快照隔一张录一帧，回放约 15fps，够用且省内存
    var players = new Array((G.teams[0]?G.teams[0].length:11) + (G.teams[1]?G.teams[1].length:11)), n = 0;
    for(var ti=0; ti<2; ti++){
      var len = G.teams[ti] ? G.teams[ti].length : 11;
      for(var i=0;i<len;i++){
        var pa = o.ps[n++] || [0,0,0];
        players[n-1] = { t:ti, i:i, x:pa[0], z:pa[1], f:pa[2] || 0 };
      }
    }
    NET.replayBuf.push({ players:players, ball:{ x:o.bl[0], y:o.bl[1], z:o.bl[2] } });
    while(NET.replayBuf.length > 110) NET.replayBuf.shift();
  }
  // 动画状态（扑救/庆祝）跟着快照走；上一张有、这一张没有的就清零
  function applySnapshotAnim(o){
    var prev = NET.animPrev;
    if(prev){
      for(var k=0;k<prev.length;k++){
        var pp = G.teams[prev[k][0]] && G.teams[prev[k][0]][prev[k][1]];
        if(pp){ pp.celebrateT = 0; pp.diveT = 0; }
      }
    }
    NET.animPrev = [];
    if(o.an){
      for(var j=0;j<o.an.length;j++){
        var rec = o.an[j], arr = G.teams[rec[0]];
        var p2 = arr && arr[rec[1]];
        if(p2){
          p2.celebrateT = rec[2] || 0;
          p2.diveT = rec[3] || 0;
          NET.animPrev.push([rec[0], rec[1]]);
        }
      }
    }
    if(o.ss && G.stats && G.stats.possT){
      G.stats.possT[0] = o.ss[0]; G.stats.possT[1] = o.ss[1];
      G.stats.shots[0] = o.ss[2]; G.stats.shots[1] = o.ss[3];
      G.stats.passes[0] = o.ss[4]; G.stats.passes[1] = o.ss[5];
      if(typeof updateStatsPanel === 'function') updateStatsPanel();
    }
  }
  // 比分/时钟/横幅这些低频状态：只在变化时写 DOM
  function applySnapshotState(sb){
    var newGoal = (!NET.lastSc || NET.lastSc[0] !== sb.sc[0] || NET.lastSc[1] !== sb.sc[1]);
    if(newGoal){
      NET.lastSc = [sb.sc[0], sb.sc[1]];
      G.score[0] = sb.sc[0]; G.score[1] = sb.sc[1];
      if(typeof updateScoreboard === 'function') updateScoreboard();
      // 客机也要看到进球庆祝：给进球者挂 celebrateT，applyWorld 刷 mesh 时就会跳起来
      if(sb.st === 'goal' && sb.sg){
        var stm = G.teams[sb.sg[0]], sp = stm ? stm[sb.sg[1]] : null;
        if(sp){
          sp.celebrateT = 3.0;
          if(typeof spawnConfetti === 'function') spawnConfetti(sp.pos.x, sp.pos.z, sb.sg[0]);
        }
      }
    }
    G.clock = sb.ck; G.half = sb.hf;
    if(typeof sb.lk === 'number') G.lastKickoff = sb.lk;
    if(sb.tm) G.time = sb.tm;   // 镜头摆动/回放环绕相位也跟主机一致
    G.state = sb.st; G.stateT = sb.stt;
    // 客机回放：用自己录的快照帧，配合主机的同一套回放镜头
    if(G.state === 'replay'){
      if(!NET.replayOn){
        NET.replayOn = true;
        NET.replayIdx = 0;
        G.replayFrames = (NET.replayBuf && NET.replayBuf.length > 8) ? NET.replayBuf.slice() : null;
        G.replayIdx = 0;
        if(typeof hideBanner === 'function') hideBanner();
        // 客机刚进来还没录到足够帧：直接请求主机跳过回放，两边一起回到开球
        if(!G.replayFrames){ try{ sendUpJSON({ t:'skip' }); }catch(_){} }
      }
    } else if(NET.replayOn){
      NET.replayOn = false;
      NET.replayIdx = 0;
      G.replayFrames = null;
    }
    G.scorerPlayer = (sb.sg && G.teams[sb.sg[0]]) ? G.teams[sb.sg[0]][sb.sg[1]] : null;
    // 客机也要知道“这脚球是传给谁的”，本地预测才能和单机一样自动迎球
    if(sb.pt){
      var ptr = G.teams[sb.pt[0]] && G.teams[sb.pt[0]][sb.pt[1]];
      if(ptr) ball.passTarget = ptr;
    } else {
      ball.passTarget = null;
    }
    var cm = Math.floor(Math.max(0, G.clock) / 60), cs = Math.floor(Math.max(0, G.clock) % 60);
    var ctxt = String(cm).padStart(2,'0') + ':' + String(cs).padStart(2,'0');
    if(NET.lastClockTxt !== ctxt){
      NET.lastClockTxt = ctxt;
      var ce = el('clock'); if(ce) ce.textContent = ctxt;
    }
    var he = el('half-label'); if(he) he.textContent = G.half === 0 ? '上半场' : '下半场';
    if(sb.bn){
      var key = sb.bn[0] + '|' + sb.bn[1];
      if(NET.bnKey !== key){
        NET.bnKey = key;
        var bb = el('banner-big'), bs = el('banner-sub');
        if(bb){ bb.textContent = sb.bn[0]; bb.style.opacity = 1; }
        if(bs){ bs.textContent = sb.bn[1] || ''; bs.style.opacity = 1; }
      }
    } else if(NET.bnKey){
      NET.bnKey = '';
      if(typeof hideBanner === 'function') hideBanner();
    }
    if(G.state === 'end' && !NET.endShown){
      NET.endShown = true;
      showEndPanel();
    }
  }
  // 世界同步：位置 = 最新快照 + 速度外推（按“现在”），再用误差平滑收束。
  // 这样对手是连续移动的，球也贴着持球人脚下走，而不是每 55ms 跳一格。
  function applyWorld(dt){
    var s = NET.snap; if(!s || !s.ps) return;
    // 快照是“主机发车时”的状态，到客机已经晚了一个单程延迟。
    // 按 RTT/2 往前多外推一点，客机看到的对手/球才接近主机当前，而不是永远慢半拍。
    var lag = NET.rtt ? Math.min(0.08, NET.rtt / 2000) : 0;
    var age = Math.min(0.32, Math.max(0, (performance.now() - NET.snapAt) / 1000 + lag));
    var myIdx = G.activeIdx, n = 0;
    for(var ti=0; ti<2; ti++){
      var arr = G.teams[ti];
      for(var i=0;i<arr.length;i++){
        var pa = s.ps[n++]; if(!pa) continue;
        var p = arr[i];
        var tx = pa[0] + (pa[3] || 0) * age;
        var tz = pa[1] + (pa[4] || 0) * age;
        var dx = tx - p.pos.x, dz = tz - p.pos.z, e = Math.hypot(dx, dz);
        if(ti === NET.myTeam && i === myIdx){
          // 自己在本地已经预测走过，只有偏差超过死区才拉回来（避免被延迟一直拖住）
          if(e > 5){ p.pos.x = tx; p.pos.z = tz; }
          else if(e > 0.5){ p.pos.x += dx * 0.2; p.pos.z += dz * 0.2; }
        } else {
          if(e > 9){ p.pos.x = tx; p.pos.z = tz; }
          else { var kk = Math.min(0.85, 0.45 + dt * 12); p.pos.x += dx * kk; p.pos.z += dz * kk; }
          p.facing = lerpAngle(p.facing, pa[2], Math.min(1, 0.25 + dt * 8));
          // 快照里的速度必须写回，否则客机的 AI 球员 vel=0，跑动动画完全不播，
          // 看上去就是“人在地面上滑”，这也是两边画面不一样的主要来源之一
          p.velx = pa[3] || 0;
          p.velz = pa[4] || 0;
        }
      }
    }
    // 球：自己刚踢出去的头 220ms 内不要被“主机还没收到动作”的旧快照拉回脚下，
    // 否则客机看到球飞出去又瞬移回脚边，像丢球/接不到
    var lf = !!(NET.localKickAt && (performance.now() - NET.localKickAt) < 220 &&
                !ball.owner && (Math.hypot(ball.vel.x || 0, ball.vel.z || 0) > 4 || ball.pos.y > 0.6));
    var lh = !!(NET.localHoldAt && (performance.now() - NET.localHoldAt) < 260 &&
                ball.owner && !s.bo);
    if(lh){
      // 本地刚接到球，主机还没确认：先把球钉在自己脚下，等主机球权回来再交还权威
      var hp = ball.owner;
      ball.pos.x = hp.pos.x + Math.cos(hp.facing) * 1.0;
      ball.pos.z = hp.pos.z + Math.sin(hp.facing) * 1.0;
      ball.pos.y = 0.34;
      ball.vel.x = 0; ball.vel.y = 0; ball.vel.z = 0;
      ball.speed = 0;
    } else if(lf){
      ball.owner = null;
      ball.pos.x += ball.vel.x * dt;
      ball.pos.y += ball.vel.y * dt;
      ball.pos.z += ball.vel.z * dt;
      ball.vel.y -= 22 * dt;
      if(ball.pos.y < 0.34){ ball.pos.y = 0.34; if(Math.abs(ball.vel.y) < 2.2) ball.vel.y = 0; else ball.vel.y = -ball.vel.y * 0.55; }
      ball.speed = Math.hypot(ball.vel.x, ball.vel.z);
    } else if(s.bo){
      var o = G.teams[s.bo[0]][s.bo[1]];
      if(o){
        ball.owner = o;
        var bx = o.pos.x + Math.cos(o.facing) * 1.0;
        var bz = o.pos.z + Math.sin(o.facing) * 1.0;
        ball.pos.x += (bx - ball.pos.x) * 0.6;
        ball.pos.z += (bz - ball.pos.z) * 0.6;
        ball.pos.y += (0.34 - ball.pos.y) * 0.6;
      }
    } else {
      ball.owner = null;
      var bl = s.bl || [0, 0.34, 0];
      var btx = bl[0] + (bl[3] || 0) * age;
      // 高球要按重力下落，不能直线外推（否则客机看到的弧线比主机平，球像飘过去）
      var bty = bl[1] + (bl[4] || 0) * age - 0.5 * 22 * age * age;
      if(bty < 0.34) bty = 0.34;
      var btz = bl[2] + (bl[5] || 0) * age;
      var be = Math.hypot(btx - ball.pos.x, bty - ball.pos.y, btz - ball.pos.z);
      if(be > 12){ ball.pos.x = btx; ball.pos.y = bty; ball.pos.z = btz; }
      else {
        ball.pos.x += (btx - ball.pos.x) * 0.78;
        ball.pos.y += (bty - ball.pos.y) * 0.78;
        ball.pos.z += (btz - ball.pos.z) * 0.78;
      }
      if(ball.pos.y < 0.34) ball.pos.y = 0.34;
      ball.speed = Math.hypot(bl[3] || 0, bl[5] || 0);
    }
    if(!NET.camInit){ NET.camInit = true; camCur.x = ball.pos.x; camCur.y = 1.2; camCur.z = ball.pos.z; }
    for(var t2=0; t2<2; t2++){
      var a2 = G.teams[t2];
      for(var j=0;j<a2.length;j++) a2[j].updateMesh();
    }
    ball.mesh.position.set(ball.pos.x, ball.pos.y, ball.pos.z);
  }
  function showEndPanel(){
    var a = G.score[0], b = G.score[1];
    var t = el('end-title'), s = el('end-score'), m = el('end-msg'), lg = el('end-log');
    if(t) t.textContent = (a === b) ? ('平局 · ' + TOURNAMENT) : ('🏆 ' + (a > b ? '小鱼鱼' : '小虾虾') + ' 夺冠 · ' + TOURNAMENT);
    if(s) s.textContent = a + ' : ' + b;
    if(m) m.textContent = (a === b) ? '势均力敌，再来一局！' : (a > b ? '小鱼鱼获胜！' : '小虾虾获胜！');
    if(lg) lg.textContent = '联机对战 · 由房主结算';
    var e = el('end'); if(e) e.classList.remove('hidden');
  }
  function clientSendInput(){
    var now = performance.now();
    // 输入 50Hz；没有直连就不发，绝不退回中继制造高延迟
    if(now - NET.lastInpAt < 20) return;
    var canP2P = NET.p2p && NET.p2p.readyState === 'open';
    if(!canP2P) return;
    NET.lastInpAt = now;
    var dx = ((keys['KeyD'] || keys['ArrowRight'] || keys['Numpad6']) ? 1 : 0) - ((keys['KeyA'] || keys['ArrowLeft'] || keys['Numpad4']) ? 1 : 0);
    var dz = ((keys['KeyS'] || keys['ArrowDown'] || keys['Numpad2']) ? 1 : 0) - ((keys['KeyW'] || keys['ArrowUp'] || keys['Numpad8']) ? 1 : 0);
    var sp = (keys['ShiftLeft'] || keys['ShiftRight']) ? 1 : 0;
    var act = '';
    while(NET.act.length) act += NET.act.shift();
    // 动作本地预演：按键当场出脚，别等主机往返回来才看到球动
    var p = G.teams[NET.myTeam] ? G.teams[NET.myTeam][G.activeIdx] : null;
    if(act && p && G.state === 'play' && ball.owner === p){
      if(act.indexOf('s') >= 0){
        var gx = oppGoalX(p.team);
        var tz = (Math.random() < 0.5 ? -1 : 1) * (2.2 + Math.random() * 1.6);
        var pw = 28 + Math.random() * 4;
        var lf = 1.5 + Math.random() * 2;
        shootToward(p, gx, tz, pw, lf);
        NET.localKickAt = performance.now();
        NET.localHoldAt = 0;
        var si = act.indexOf('s');
        act = act.slice(0, si) + 's' + tz.toFixed(1) + ',' + pw.toFixed(1) + ',' + lf.toFixed(1) + act.slice(si + 1);
      } else if(act.indexOf('p') >= 0){
        var t = bestPassTarget(p, G.teams[p.team]);
        if(t){
          passTo(p, t, 20);
          NET.localKickAt = performance.now();
          NET.localHoldAt = 0;
          // 把目标编号带给主机，避免客机看到传给 A、主机却传给 B
          var pi = act.indexOf('p');
          act = act.slice(0, pi) + 'p' + t.idx + act.slice(pi + 1);
        }
      }
    }
    NET.inSeq = (NET.inSeq + 1) >>> 0;
    sendUp(dx + '|' + dz + '|' + sp + '|' + act + '|' + G.activeIdx + '|' + NET.inSeq + '|' + (NET.session || ''));
    if(now - NET.pingAt > 2000 && !document.hidden){
      NET.pingAt = now;
      p2pSend('p' + Math.round(now));
    }
  }
  // 客机本地预测：自己的球员先按同一套速度规则在本地走，再向主机快照平滑收敛。
  // 主机仍是唯一权威，这里只影响客机自己的画面，不会改变任何游戏逻辑。
  function clientPredict(dt){
    if(!G || G.state !== 'play' || G.paused) return;
    var arr = G.teams[NET.myTeam];
    if(!arr) return;
    var p = arr[G.activeIdx];
    if(!p) return;
    var dx = ((keys['KeyD'] || keys['ArrowRight'] || keys['Numpad6']) ? 1 : 0) - ((keys['KeyA'] || keys['ArrowLeft'] || keys['Numpad4']) ? 1 : 0);
    var dz = ((keys['KeyS'] || keys['ArrowDown'] || keys['Numpad2']) ? 1 : 0) - ((keys['KeyW'] || keys['ArrowUp'] || keys['Numpad8']) ? 1 : 0);
    var len = Math.hypot(dx, dz);
    if(len > 0.01){
      var spd = (keys['ShiftLeft'] || keys['ShiftRight']) ? (SPRINT_SPD + USER_SPRINT_BOOST) : WALK_SPD;
      p.pos.x += dx/len * spd * dt;
      p.pos.z += dz/len * spd * dt;
      p.facing = Math.atan2(dz, dx);
      p.velx = dx/len * spd; p.velz = dz/len * spd;
    } else if(ball.passTarget === p || (!ball.owner && dist2D(p.pos.x,p.pos.z,ball.pos.x,ball.pos.z) < 10 && ball.speed < 20)){
      // 与单机 userMove 相同：传球朝你飞来 / 球滚到脚边时自动迎球
      if(typeof moveAI === 'function') moveAI(p, {x:ball.pos.x, z:ball.pos.z}, dt, true);
    } else {
      p.velx = 0; p.velz = 0;
    }
    if(typeof clampPlayer === 'function') clampPlayer(p);
    // 本地先完成“接球”：客机的球比主机快一个单程延迟，如果等主机确认再拿球，
    // 会出现“人已经站在球上、球却从脚边滑过去”。这里只做一个短窗口的本地持球，
    // 主机快照一旦给出正式球权就以主机为准。
    var hostBallOwner = NET.snap && NET.snap.bo;
    var hostOtherOwns = !!(hostBallOwner && !(hostBallOwner[0] === NET.myTeam && hostBallOwner[1] === p.idx));
    if(!hostOtherOwns && !ball.owner && G.state === 'play' && !selfPassLock(p) &&
       ball.speed < 22 && dist2D(p.pos.x, p.pos.z, ball.pos.x, ball.pos.z) < PICKUP_R){
      ball.owner = p; p.owns = true; ball.passTarget = null;
      ball.vel.x = 0; ball.vel.y = 0; ball.vel.z = 0;
      NET.localHoldAt = performance.now();
    }
    // 自己控球时把球钉在脚下（主机快照里的球权 + 本地预测的位置），球不会拖在身后
    if(ball.owner === p){
      ball.pos.x = p.pos.x + Math.cos(p.facing) * 1.0;
      ball.pos.z = p.pos.z + Math.sin(p.facing) * 1.0;
      ball.pos.y = 0.34;
    }
    // 不再单独 updateMesh：applyWorld 最后会统一刷一次，
    // 这里再刷会让自己的跑动动画 phase 每帧加两次，腿摆得比主机快一倍
  }
  function clientFrame(dt){
    G.time += dt;
    if(G.state !== 'play') NET.act.length = 0;   // 非比赛状态不攒动作键
    // 客机也要推进庆祝/扑救计时和彩带，不然进球者会一直定格在空中
    for(var ti=0; ti<2; ti++){
      var arr0 = G.teams[ti];
      if(!arr0) continue;
      for(var i0=0; i0<arr0.length; i0++){
        var p0 = arr0[i0];
        if(p0.celebrateT > 0) p0.celebrateT -= dt;
        if(p0.diveT > 0) p0.diveT -= dt;
      }
    }
    if(typeof updateConfetti === 'function') updateConfetti(dt);
    var replaying = !!(G.state === 'replay' && G.replayFrames && G.replayFrames.length);
    if(replaying){
      // 客机用自己录的快照帧播回放；和主机共用“按 2.2 秒进度连续插值”的算法，
      // 不再 0.6 帧硬跳、也不会播完循环回第一帧闪一下。
      G.stateT = Math.max(0, (G.stateT || 0) - dt);
      var total = 2.2;
      var prog = Math.max(0, Math.min(1, 1 - G.stateT / total));
      var flen = G.replayFrames.length;
      var start = Math.max(0, flen - 76);
      var raw = start + prog * (flen - 1 - start);
      for(var tc=0; tc<2; tc++){
        var ac = G.teams[tc];
        if(!ac) continue;
        for(var ic=0; ic<ac.length; ic++){ ac[ic].celebrateT = 0; ac[ic].diveT = 0; }
      }
      if(typeof applyReplayAt === 'function') applyReplayAt(G.replayFrames, raw);
      ball.owner = null; ball.speed = 0;
      for(var t3=0; t3<2; t3++){
        var a3 = G.teams[t3];
        if(!a3) continue;
        for(var j3=0; j3<a3.length; j3++) a3[j3].updateMesh();
      }
      ball.mesh.position.set(ball.pos.x, ball.pos.y, ball.pos.z);
    } else {
      clientSendInput();
      clientPredict(dt);     // 本地先走，手感立刻响应
      // 和主机一样做球员身体碰撞：客机不跑物理，但至少自己的球员不能穿过对手
      if(typeof resolveCollisions === 'function') resolveCollisions();
      applyWorld(dt);        // 用最新快照外推 + 平滑收束（主机仍是权威）
    }
    // 客机没有本地回放帧，但回放阶段要能看到“跳过回放”按钮（点了会让主机结束回放）
    var sk = el('btn-skip-replay');
    if(sk) sk.style.display = (G.state === 'replay') ? 'block' : 'none';
    if(typeof updateCamera === 'function') updateCamera(dt);
    if(typeof drawMinimap === 'function') drawMinimap();
    renderer.render(scene, camera);
    var nb = el('net-badge');
    if(nb){
      var lost = NET.p2pOk && NET.lastRecv && performance.now() - NET.lastRecv > 4000;
      nb.textContent = (NET.p2pOk ? '⚡直连 ' : (NET.directFail ? '⚠ 直连失败 · ' : '📡 正在建立直连 · ')) +
        (lost ? (NET.code + ' · ⚠ 连接中断') : (NET.code + (NET.rtt ? ' · ' + NET.rtt + 'ms' : '')));
    }
    if(!NET.p2pOk){
      if(NET.directFail) setStatus('⚠ 直连失败：双方换网络或开热点后重试', false);
      else setStatus('正在建立直连（P2P）…');
    } else if(NET.lastRecv && performance.now() - NET.lastRecv > 4000){
      setStatus('连接中断', false);
    }
  }

  function showBadge(){
    var nb = el('net-badge');
    if(nb){ nb.style.display = 'block'; nb.textContent = '📡 房号 ' + NET.code; }
  }

  // ---------------- 钩子（单机模式全部透传） ----------------
  var _runFrame = runFrame;
  var _userShoot = userShoot;
  var _userPass = userPass;
  var _switchPlayer = switchPlayer;
  var _restartMatch = restartMatch;
  var _togglePause = togglePause;
  var _oppAI = updateOpponentAI;
  var _mateAI = updateTeammateAI;

  runFrame = function(dt){
    if(NET.role === 'client'){
      if(G.paused) return;
      return clientFrame(dt);
    }
    if(NET.role === 'host' && NET.remote){
      hostPreTick(dt);
      _runFrame(dt);
      hostPostTick();
      return;
    }
    return _runFrame(dt);
  };
  userShoot = function(){
    if(NET.role === 'client'){
      if(G.state === 'play'){ NET.act.push('s'); return true; }
      return false;
    }
    return _userShoot.apply(this, arguments);
  };
  userPass = function(){
    if(NET.role === 'client'){
      if(G.state === 'play'){ NET.act.push('p'); return true; }
      return false;
    }
    return _userPass.apply(this, arguments);
  };
  switchPlayer = function(){
    if(NET.role === 'client'){
      // 联机：和单机同一套切换逻辑（切到离球最近的本方非门将），再把所选编号告诉主机
      var before = G.activeIdx;
      _switchPlayer.apply(this, arguments);
      if(G.activeIdx !== before){
        sendUpJSON({ t:'sel', i: G.activeIdx });
        if(typeof sayComment === 'function') sayComment('已切换操控本队 #' + (G.activeIdx+1));
      }
      return;
    }
    return _switchPlayer.apply(this, arguments);
  };
  togglePause = function(){
    if(NET.role === 'client' || NET.role === 'host'){
      if(typeof sayComment === 'function') sayComment('🌐 联机模式不能暂停');
      return;
    }
    return _togglePause.apply(this, arguments);
  };
  restartMatch = function(){
    if(NET.role === 'client'){
      if(typeof sayComment === 'function') sayComment('🌐 联机模式：等待房主重开');
      return;
    }
    if(NET.role === 'host'){
      _restartMatch();
      NET.lastSend = 0; NET.snaps.length = 0;
      if(NET.code) netPub(Rp(NET.code,'h2c'), JSON.stringify({ t:'rst' }));
      return;
    }
    return _restartMatch.apply(this, arguments);
  };
  updateOpponentAI = function(team, dt){
    if(NET.role === 'host' && NET.remote) return _oppAI(team.filter(function(p){ return p !== NET.remote; }), dt);
    return _oppAI(team, dt);
  };
  updateTeammateAI = function(team, dt){
    if(NET.role === 'host' && NET.remote) return _mateAI(team.filter(function(p){ return p !== NET.remote; }), dt);
    return _mateAI(team, dt);
  };
  var _skipReplay = window.skipReplay;
  window.skipReplay = function(){
    if(NET.role === 'client'){ sendUpJSON({ t:'skip' }); return; }
    return _skipReplay.apply(this, arguments);
  };

  // 断线自动回到菜单（客户端）
  window.netSelfTest = function(){
    return {
      role: NET.role, code: NET.code, broker: NET.broker,
      signals: (NET.mqs || []).length, signalsTotal: BROKERS.length,
      direct: NET.p2pOk, ice: NET.iceOk, pc: !!NET.pc,
      directFail: NET.directFail, tries: NET.p2pTries,
      snaps: NET.snaps.length, rtt: NET.rtt,
      remote: NET.remote ? [NET.remote.team, NET.remote.idx, r1(NET.remote.pos.x), r1(NET.remote.pos.z)] : null
    };
  };
  // ---------------- 排位赛榜单（纯前端 · retained 公共主题） ----------------
  // 每人一条 retained 主题 zhujcup2026/rank/<赛季>/<玩家ID>，榜单页用 + 通配一次性收齐。
  // 不依赖自建后端；公共中继不可达时前端只看本机成绩。
  var RANK_NS = NS + 'rank/';
  // 榜单多路备份：任意一路连得上就能看榜、上榜，避免被单一中继限流卡死
  var RANK_BROKERS = [
    'wss://broker.hivemq.com:8884/mqtt',
    'ws://broker.hivemq.com:8000/mqtt',
    'wss://broker.emqx.io:8084/mqtt',
    'wss://broker-cn.emqx.io:8084/mqtt',
    'wss://mqtt.eclipseprojects.io/mqtt',
    'wss://test.mosquitto.org:8081/mqtt',
    'ws://test.mosquitto.org:8080/mqtt',
    'wss://broker.mqttdashboard.com:8000/mqtt'
  ];

  // 年度榜：不再按周清零，避免周一零点榜单突然变空
  function seasonKey(){
    return String(new Date().getFullYear());
  }

  function rankParse(topic, payload){
    try {
      var o = JSON.parse(payload);
      if(!o || !o.id || typeof o.p !== 'number' || !isFinite(o.p)) return null;
      o.p = Math.max(0, Math.min(9999, Math.round(o.p)));
      o.g = Math.max(0, Math.min(99, +o.g || 0));
      o.c = Math.max(0, Math.min(99, +o.c || 0));
      o.n = String(o.n || '匿名').slice(0, 10);
      o.t = +o.t || 0;
      o.s = String(o.s || seasonKey());
      return o;
    } catch(_){ return null; }
  }

  window.RANK = {
    season: seasonKey,
    submit: function(entry){
      return new Promise(function(resolve){
        var season = entry.s || seasonKey();
        var topic = RANK_NS + season + '/' + entry.id;
        var done = false;
        var finish = function(ok){ if(done) return; done = true; resolve(ok); };
        RANK_BROKERS.forEach(function(url){
          var mq = new MiniMQTT(url, 'zhujr_' + Math.random().toString(36).slice(2, 8));
          mq.connect(5000).then(function(){
            var decided = false, seen = null;
            var decide = function(){
              if(decided) return; decided = true;
              // 同一玩家已有更高分 → 不拿这次的低分去覆盖，最高分留在榜上
              var keep = seen && (seen.p > entry.p || (seen.p === entry.p && seen.g >= (+entry.g||0)));
              if(!keep){
                try{ mq.publish(topic, JSON.stringify(entry), true); }catch(_){}
                // retained 主题在部分公共中继上偶发丢包，稍后再补一次，题目/内容相同不会产生重复成绩
                setTimeout(function(){ try{ mq.publish(topic, JSON.stringify(entry), true); }catch(_){} }, 300);
              }
              setTimeout(function(){ try{ mq.close(); }catch(_){} }, 900);
              finish(true);
            };
            mq.onmsg = function(t, payload){
              if(t !== topic) return;
              var o = rankParse(t, payload);
              if(o && o.id === entry.id && (!seen || o.p > seen.p)) seen = o;
              decide();
            };
            try{ mq.subscribe(topic); }catch(_){}
            setTimeout(decide, 1200);
          }).catch(function(){ try{ mq.close(); }catch(_){} });
        });
        setTimeout(function(){ finish(false); }, 7500);
      });
    },
    fetch: function(season, cb){
      season = season || seasonKey();
      var got = {}, mqs = [], started = false, closed = false;
      var finish = function(none){
        if(closed) return; closed = true;
        mqs.forEach(function(m){ try{ m.close(); }catch(_){} });
        cb(none ? null : Object.keys(got).map(function(k){ return got[k]; }));
      };
      var startWindow = function(){
        if(started) return; started = true;
        setTimeout(function(){ finish(false); }, 3200);
      };
      RANK_BROKERS.forEach(function(url){
        var mq = new MiniMQTT(url, 'zhujr_' + Math.random().toString(36).slice(2, 8));
        mq.connect(5000).then(function(){
          if(closed){ try{ mq.close(); }catch(_){} return; }
          mqs.push(mq);
          mq.onmsg = function(topic, payload){
            var o = rankParse(topic, payload);
            if(o && o.s === season && (!got[o.id] || o.p > got[o.id].p)) got[o.id] = o;
          };
          mq.subscribe(RANK_NS + season + '/+');
          startWindow();
        }).catch(function(){ try{ mq.close(); }catch(_){} });
      });
      setTimeout(function(){ if(!started) finish(true); }, 7500);
    }
  };
})();

