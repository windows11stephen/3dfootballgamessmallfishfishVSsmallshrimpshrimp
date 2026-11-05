/* ============================================================
   联机对战模块
   - 公网 MQTT 中继（HiveMQ 公共 broker），无需自建服务器
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
      try { ws = new WebSocket(self.url); } catch(e){ return reject(e); }
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
  // 公网中继候选（全部免费、无需账号）。开房时并行连接，能连上的全部同时挂房间，
  // 客户端只要其中一个能用就能找到房间 —— 单个中继抽风不影响联机。
  var BROKERS = [
    'wss://broker.hivemq.com:8884/mqtt',
    'ws://broker.hivemq.com:8000/mqtt',
    'wss://broker.emqx.io:8084/mqtt',
    'ws://broker.emqx.io:8083/mqtt',
    'wss://broker-cn.emqx.io:8084/mqtt',
    'wss://mqtt.eclipseprojects.io/mqtt',
    'wss://test.mosquitto.org:8081/mqtt'
  ];
  var NS = 'zhujcup2026/';
  var NET = window.NET = {
    role: null,            // null | 'host' | 'client' | 'client-wait'
    code: '', nick: '', oppNick: '',
    broker: '', mq: null, mqs: [], roomMq: null, roomSeen: false,
    rooms: {}, p2p: null, p2pOk: false, pc: null, p2pAt: 0,
    myTeam: 0, remoteTeam: 1, remoteIdx: 9,
    remote: null,
    inp: { x: 0, y: 0, sp: 0 },
    act: [],
    lastSend: 0, lastInpAt: 0, lastInpRecv: 0, lastRecv: 0,
    snaps: [], interpDelay: 0.16,
    rtt: 0, pingAt: 0,
    bnKey: '', endShown: false, matchStarted: false,
    keepT: null, joinT: null, badgeT: 0
  };
  function Rp(code, ch){ return NS + code + '/' + (ch||''); }
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

  // ---------------- 连接中继（并行多路，能连上的全部保留） ----------------
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
          reject(new Error('所有公网中继都连不上（换个网络或稍后重试）'));
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
     - 信令借用已连上的 MQTT 中继（只在开局交换一次 SDP，量极小）
     - 直连通道只跑输入流和快照流（丢帧无所谓，下一帧就是全量状态）
     - 直连建立失败/中途断开：自动继续用中继，玩家无感
     ============================================================ */
  var ICE = { iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ] };
  function rtcOK(){ return typeof RTCPeerConnection === 'function'; }
  function p2pSend(data){
    if(NET.p2p && NET.p2p.readyState === 'open'){
      try{ NET.p2p.send(data); return true; }catch(_){}
    }
    return false;
  }
  function p2pKill(){
    NET.p2pOk = false;
    if(NET.p2p){ try{ NET.p2p.close(); }catch(_){} NET.p2p = null; }
    if(NET.pc){ try{ NET.pc.close(); }catch(_){} NET.pc = null; }
  }
  function p2pBind(ch){
    NET.p2p = ch;
    ch.binaryType = 'arraybuffer';
    ch.onopen = function(){
      NET.p2pOk = true;
      NET.p2pAt = performance.now();
      if(typeof sayComment === 'function') sayComment('⚡ 已建立直连（P2P），延迟更低');
    };
    ch.onclose = function(){
      if(NET.p2pOk && typeof sayComment === 'function') sayComment('直连断开，已自动切回中继');
      NET.p2pOk = false;
    };
    ch.onmessage = function(ev){
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
    if(c0 === 'p'){ if(s.length > 1) p2pSend('o' + s.slice(1)); return; }
    if(c0 === 'o'){ var n = +s.slice(1); if(n){ NET.rtt = Math.round(performance.now() - n); } return; }
  }
  function rtcGather(pc, ms){
    return new Promise(function(res){
      if(pc.iceGatheringState === 'complete') return res();
      var done = false;
      var t = setTimeout(function(){ if(!done){ done = true; res(); } }, ms || 2500);
      pc.onicegatheringstatechange = function(){
        if(pc.iceGatheringState === 'complete' && !done){ done = true; clearTimeout(t); res(); }
      };
    });
  }
  function rtcHostStart(){
    if(!rtcOK() || !NET.code) return;
    p2pKill();
    var pc;
    try{ pc = new RTCPeerConnection(ICE); }catch(_){ return; }
    NET.pc = pc;
    p2pBind(pc.createDataChannel('g', { ordered: false, maxRetransmits: 0 }));
    pc.oniceconnectionstatechange = function(){
      if(pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'disconnected'){ NET.p2pOk = false; }
    };
    pc.createOffer()
      .then(function(o){ return pc.setLocalDescription(o); })
      .then(function(){ return rtcGather(pc, 2500); })
      .then(function(){
        if(!NET.code || !pc.localDescription) return;
        netPub(Rp(NET.code,'h2c'), JSON.stringify({ t:'rtc', sdp:{ type: pc.localDescription.type, sdp: pc.localDescription.sdp } }));
      }).catch(function(){});
  }
  function rtcClientStart(desc){
    if(!rtcOK() || !desc) return;
    p2pKill();
    var pc;
    try{ pc = new RTCPeerConnection(ICE); }catch(_){ return; }
    NET.pc = pc;
    pc.ondatachannel = function(ev){ p2pBind(ev.channel); };
    pc.oniceconnectionstatechange = function(){
      if(pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'disconnected'){ NET.p2pOk = false; }
    };
    pc.setRemoteDescription(new RTCSessionDescription(desc))
      .then(function(){ return pc.createAnswer(); })
      .then(function(a){ return pc.setLocalDescription(a); })
      .then(function(){ return rtcGather(pc, 2500); })
      .then(function(){
        if(!NET.code || !NET.mq || !pc.localDescription) return;
        NET.mq.publish(Rp(NET.code,'c2h'), JSON.stringify({ t:'rtcAns', sdp:{ type: pc.localDescription.type, sdp: pc.localDescription.sdp } }));
      }).catch(function(){});
  }
  function rtcHostAnswer(desc){
    if(!NET.pc || !desc) return;
    try{ NET.pc.setRemoteDescription(new RTCSessionDescription(desc)).catch(function(){}); }catch(_){}
  }
  // 直连可用就用直连，否则回中继 —— 所有上行/下行都走这里
  function sendUp(str){
    if(p2pSend('i' + str)) return;
    if(NET.mq) NET.mq.publish(Rp(NET.code,'c2h'), str);
  }
  function sendSnap(payload){
    if(p2pSend('s' + payload)) return;
    netPub(Rp(NET.code,'h2c'), payload);
  }

  // ---------------- 面板操作 ----------------
  window.openOnline = function(){
    var p = el('online-panel');
    if(!p) return;
    p.classList.remove('hidden');
    var nk = el('net-nick');
    if(nk && !nk.value) nk.value = '球员' + (Math.floor(Math.random()*90) + 10);
    setStatus('房号即房间钥匙，把它发给朋友即可联机。');
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
      rm.textContent = (r.n || '玩家') + ' · ' + (r.hops || 1) + ' 路中继';
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
      if(list) list.innerHTML = '<div class="rb-empty">公网中继连不上（房号直连仍可尝试）</div>';
    });
  };
  window.netCreateRoom = function(){
    if(NET.role){ setStatus('已在房间中，请先退出'); return; }
    var nick = ((el('net-nick') && el('net-nick').value) || '').trim().slice(0,10) || '球员';
    var btn = el('btn-create-room');
    if(btn) btn.disabled = true;
    setStatus('正在连接公网中继（多路并行）…');
    var haveC = (NET.mqs && NET.mqs.length) ? Promise.resolve(NET.mqs) : connectRelays();
    haveC.then(function(mqs){
      NET.mq = mqs[0]; NET.role = 'host'; NET.nick = nick;
      NET.myTeam = G.userTeam; NET.code = genCode();
      mqs.forEach(function(mq){
        mq.onmsg = function(t, p){ onRelayMsg(t, p, mq); };
        mq.subscribe(Rp(NET.code, 'c2h'));
        mq.publish(Rp(NET.code, ''), JSON.stringify({ n: nick, t: Date.now() }), true);
      });
      var rs = el('room-show'); if(rs) rs.classList.remove('hidden');
      var rc = el('room-code'); if(rc) rc.textContent = NET.code;
      setStatus('房间已创建 · 已挂 ' + mqs.length + ' 个中继 · 等待对手加入…');
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
    setStatus('正在连接公网中继（多路并行）…');
    NET.roomSeen = false; NET.roomMq = null;
    var haveJ = (NET.mqs && NET.mqs.length) ? Promise.resolve(NET.mqs) : connectRelays();
    haveJ.then(function(mqs){
      NET.nick = nick; NET.code = code;
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
      // 只保留找到房间的那一条中继，避免同一份快照被重复接收
      NET.mq = NET.roomMq;
      (NET.mqs || []).forEach(function(m){ if(m !== NET.mq){ try{ m.close(); }catch(_){} } });
      NET.mqs = [NET.mq];
      NET.role = 'client-wait';
      setStatus('已找到房间 · 正在握手…');
      var tries = 0;
      var sendJoin = function(){
        if(NET.role === 'client'){ clearInterval(NET.joinT); NET.joinT = null; return; }
        if(++tries > 24){
          clearInterval(NET.joinT); NET.joinT = null;
          setStatus('❌ 房主无响应，请重试', false);
          try{ NET.mq.close(); }catch(_){}
          NET.mq = null; NET.mqs = []; NET.role = null;
          if(btn) btn.disabled = false;
          return;
        }
        NET.mq.publish(Rp(NET.code, 'c2h'), JSON.stringify({ t: 'j', n: NET.nick }));
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
      if(payload.charAt(0) === '{'){
        var o = null;
        try{ o = JSON.parse(payload); }catch(_){ return; }
        if(o.t === 's'){ if(NET.role === 'client') clientOnSnapshot(o); }
        else if(o.t === 'acc') clientOnAccept(o);
        else if(o.t === 'rst') clientOnReset();
        else if(o.t === 'rtc'){ if(NET.role === 'client' || NET.role === 'client-wait') rtcClientStart(o.sdp); }
      } else if(payload.charAt(0) === 'o'){
        NET.rtt = Math.round(performance.now() - (+payload.slice(1)));
      }
      return;
    }
    if(topic.slice(-3) === 'c2h'){
      if(NET.role !== 'host') return;
      var c0 = payload.charAt(0);
      if(c0 === 'i'){ hostOnInput(payload); return; }
      if(c0 === 'p'){ (src || NET.mq).publish(Rp(NET.code,'h2c'), 'o' + payload.slice(1)); return; }
      try{
        var m = JSON.parse(payload);
        if(m.t === 'j') hostOnJoin(m);
        else if(m.t === 'rtcAns'){ if(NET.role === 'host') rtcHostAnswer(m.sdp); }
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
    // 已在比赛中还收到握手（多半是应答丢了）：原样重发应答，保证客户端一定能连上
    if(NET.matchStarted){
      netPub(Rp(NET.code,'h2c'), JSON.stringify({ t:'acc', team: NET.remoteTeam }));
      if(!NET.p2pOk) rtcHostStart();   // 直连没建起来就再喊一次
      return;
    }
    NET.oppNick = (m.n || '对手').toString().slice(0, 10);
    NET.matchStarted = true;
    NET.remoteTeam = 1 - NET.myTeam;
    G.userTeam = NET.myTeam;
    startGame();
    NET.remote = G.teams[NET.remoteTeam][9];
    if(NET.remote) NET.remote.remoteCtl = true;
    NET.lastInpRecv = performance.now();
    NET.lastSend = 0; NET.snaps.length = 0;
    netPub(Rp(NET.code,'h2c'), JSON.stringify({ t:'acc', team: NET.remoteTeam }));
    closeOnline();
    showBadge();
    rtcHostStart();   // 尝试建立点对点直连（失败就继续走中继）
    if(typeof sayComment === 'function') sayComment('🌐 对手「' + NET.oppNick + '」已加入，比赛开始！');
  }
  function hostOnInput(str){
    var parts = str.split('|');
    NET.inp.x = +parts[0] || 0;
    NET.inp.y = +parts[1] || 0;
    NET.inp.sp = +parts[2] || 0;
    var acts = parts[3] || '';
    for(var i=0;i<acts.length;i++) NET.act.push(acts.charAt(i));
    NET.lastInpRecv = performance.now();
  }
  function hostPreTick(dt){
    var p = NET.remote;
    if(!p) return;
    // 移动（与玩家操控同一套速度/朝向规则）
    var spd = NET.inp.sp ? (SPRINT_SPD + USER_SPRINT_BOOST) : WALK_SPD;
    var len = Math.hypot(NET.inp.x, NET.inp.y);
    if(len > 0.01){
      p.pos.x += NET.inp.x / len * spd * dt;
      p.pos.z += NET.inp.y / len * spd * dt;
      p.facing = Math.atan2(NET.inp.y, NET.inp.x);
      p.velx = NET.inp.x / len * spd; p.velz = NET.inp.y / len * spd;
    } else { p.velx = 0; p.velz = 0; }
    clampPlayer(p);
    // 拾球（与玩家同一规则）
    if(!ball.owner && ball.speed < 22 &&
       dist2D(p.pos.x, p.pos.z, ball.pos.x, ball.pos.z) < PICKUP_R + 2.0){
      ball.owner = p; p.owns = true; ball.passTarget = null; G.oppPasses = 0; p.decideT = 0.1;
      ball.vel.x = 0; ball.vel.y = 0; ball.vel.z = 0;
    }
    // 动作（与 userShoot / userPass 完全同一套参数）
    if(NET.act.length && p.owns && G.state === 'play'){
      var acts = NET.act.splice(0);
      for(var i=0;i<acts.length;i++){
        if(acts[i] === 's'){
          var gx = oppGoalX(p.team);
          var tz = (Math.random() < 0.5 ? -1 : 1) * (2.2 + Math.random() * 1.6);
          shootToward(p, gx, tz, 28 + Math.random() * 4, 1.5 + Math.random() * 2);
        } else if(acts[i] === 'p'){
          var t = bestPassTarget(p, G.teams[p.team]);
          if(t) passTo(p, t, 20);
        }
      }
    } else if(NET.act.length > 4){
      NET.act.length = 0;
    }
  }
  function r1(v){ return Math.round(v * 10) / 10; }
  function hostPostTick(){
    var now = performance.now();
    if(now - NET.lastSend > 82){
      NET.lastSend = now;
      var ps = new Array(22), n = 0;
      for(var ti=0; ti<2; ti++){
        var arr = G.teams[ti];
        for(var i=0;i<arr.length;i++){
          var p = arr[i];
          ps[n++] = [r1(p.pos.x), r1(p.pos.z), r1(p.facing)];
        }
      }
      var bn = (G.bannerT > 0 && el('banner-big')) ? [el('banner-big').textContent, (el('banner-sub')||{}).textContent || ''] : null;
      var sg = (G.state === 'goal' && G.scorerPlayer) ? [G.scorerPlayer.team, G.scorerPlayer.idx] : null;
      var bo = ball.owner ? [ball.owner.team, ball.owner.idx] : null;
      sendSnap(JSON.stringify({
        t:'s', st:G.state, sc:G.score, ck:r1(G.clock), hf:G.half, stt:r1(Math.max(0, G.stateT)),
        bl:[r1(ball.pos.x), r1(ball.pos.y), r1(ball.pos.z)], bo:bo, sg:sg, bn:bn, ps:ps
      }));
    }
    var nb = el('net-badge');
    if(nb){
      var off = now - NET.lastInpRecv > 4000;
      nb.textContent = (document.hidden ? '⚠ 窗口在后台会变慢 · ' : '') + (NET.p2pOk ? '⚡直连 ' : '🌐中继 ') +
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
    NET.snaps.length = 0; NET.endShown = false; NET.bnKey = ''; NET.camInit = false;
    startGame();
    closeOnline();
    showBadge();
    if(typeof sayComment === 'function') sayComment('🌐 已连上房主，开始比赛！');
  }
  function clientOnReset(){
    NET.snaps.length = 0; NET.endShown = false; NET.bnKey = '';
    startGame();
  }
  function clientOnSnapshot(o){
    NET.lastRecv = performance.now();
    NET.snaps.push({ s: o, t: NET.lastRecv / 1000 });
    if(NET.snaps.length > 10) NET.snaps.shift();
  }
  function lerpAngle(a, b, k){
    var d = b - a;
    while(d > Math.PI) d -= Math.PI * 2;
    while(d < -Math.PI) d += Math.PI * 2;
    return a + d * k;
  }
  function applyInterp(){
    var snaps = NET.snaps;
    if(!snaps.length) return;
    var a = snaps[0], b = snaps[0];
    if(snaps.length > 1){
      var target = performance.now() / 1000 - NET.interpDelay;
      for(var i = snaps.length - 1; i > 0; i--){
        if(snaps[i-1].t <= target){ a = snaps[i-1]; b = snaps[i]; break; }
      }
      if(a === b){ a = snaps[Math.max(0, snaps.length-2)]; b = snaps[snaps.length-1]; }
    }
    var k = 1;
    if(a && b && b.t > a.t) k = Math.max(0, Math.min(1, (performance.now()/1000 - NET.interpDelay - a.t) / (b.t - a.t)));
    var sa = a.s, sb = b.s;
    var n = 0;
    for(var ti=0; ti<2; ti++){
      var arr = G.teams[ti];
      for(var i=0;i<arr.length;i++){
        var pa = sa.ps[n], pb = sb.ps[n]; n++;
        var p = arr[i];
        p.pos.x = pa[0] + (pb[0] - pa[0]) * k;
        p.pos.z = pa[1] + (pb[1] - pa[1]) * k;
        p.facing = lerpAngle(pa[2], pb[2], k);
      }
    }
    ball.pos.x = sa.bl[0] + (sb.bl[0] - sa.bl[0]) * k;
    ball.pos.y = sa.bl[1] + (sb.bl[1] - sa.bl[1]) * k;
    ball.pos.z = sa.bl[2] + (sb.bl[2] - sa.bl[2]) * k;
    ball.owner = sb.bo ? G.teams[sb.bo[0]][sb.bo[1]] : null;
    // 第一张快照：相机立刻对准球，避免客户端开局镜头停在菜单位置
    if(!NET.camInit){ NET.camInit = true; camCur.x = ball.pos.x; camCur.y = 1.2; camCur.z = ball.pos.z; }
    G.score[0] = sb.sc[0]; G.score[1] = sb.sc[1];
    G.clock = sb.ck; G.half = sb.hf;
    G.state = sb.st; G.stateT = sb.stt;
    G.scorerPlayer = sb.sg ? G.teams[sb.sg[0]][sb.sg[1]] : null;
    for(var t2=0; t2<2; t2++){
      var a2 = G.teams[t2];
      for(var j=0;j<a2.length;j++) a2[j].updateMesh();
    }
    ball.mesh.position.set(ball.pos.x, ball.pos.y, ball.pos.z);
    if(typeof updateScoreboard === 'function') updateScoreboard();
    var cm = Math.floor(Math.max(0, G.clock) / 60), cs = Math.floor(Math.max(0, G.clock) % 60);
    var ce = el('clock'); if(ce) ce.textContent = String(cm).padStart(2,'0') + ':' + String(cs).padStart(2,'0');
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
    if(now - NET.lastInpAt < 55) return;
    if(!NET.mq || !NET.mq.connected) return;
    NET.lastInpAt = now;
    var dx = ((keys['KeyD'] || keys['ArrowRight']) ? 1 : 0) - ((keys['KeyA'] || keys['ArrowLeft']) ? 1 : 0);
    var dz = ((keys['KeyS'] || keys['ArrowDown']) ? 1 : 0) - ((keys['KeyW'] || keys['ArrowUp']) ? 1 : 0);
    var sp = (keys['ShiftLeft'] || keys['ShiftRight']) ? 1 : 0;
    var act = '';
    while(NET.act.length) act += NET.act.shift();
    sendUp(dx + '|' + dz + '|' + sp + '|' + act);
    if(now - NET.pingAt > 2000){
      NET.pingAt = now;
      if(!p2pSend('p' + Math.round(now))) NET.mq.publish(Rp(NET.code,'c2h'), 'p' + Math.round(now));
    }
  }
  function clientFrame(dt){
    G.time += dt;
    clientSendInput();
    applyInterp();
    if(typeof updateCamera === 'function') updateCamera(dt);
    if(typeof drawMinimap === 'function') drawMinimap();
    renderer.render(scene, camera);
    var nb = el('net-badge');
    if(nb){
      var lost = performance.now() - NET.lastRecv > 4000;
      nb.textContent = (NET.p2pOk ? '⚡直连 ' : '🌐中继 ') +
        (lost ? (NET.code + ' · ⚠ 连接中断') : (NET.code + (NET.rtt ? ' · ' + NET.rtt + 'ms' : '')));
    }
    if(performance.now() - NET.lastRecv > 4000) setStatus('连接中断', false);
  }

  function showBadge(){
    var nb = el('net-badge');
    if(nb){ nb.style.display = 'block'; nb.textContent = '🌐 房号 ' + NET.code; }
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
    if(NET.role === 'client'){ NET.act.push('s'); return; }
    return _userShoot.apply(this, arguments);
  };
  userPass = function(){
    if(NET.role === 'client'){ NET.act.push('p'); return; }
    return _userPass.apply(this, arguments);
  };
  switchPlayer = function(){
    if(NET.role === 'client'){
      if(typeof sayComment === 'function') sayComment('🌐 联机模式：固定操控你的前锋');
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

  // 断线自动回到菜单（客户端）
  window.netSelfTest = function(){
    return {
      role: NET.role, code: NET.code, broker: NET.broker,
      relays: (NET.mqs || []).length, brokers: BROKERS.length,
      direct: NET.p2pOk, pc: !!NET.pc,
      snaps: NET.snaps.length, rtt: NET.rtt,
      remote: NET.remote ? [NET.remote.team, NET.remote.idx, r1(NET.remote.pos.x), r1(NET.remote.pos.z)] : null
    };
  };
})();
