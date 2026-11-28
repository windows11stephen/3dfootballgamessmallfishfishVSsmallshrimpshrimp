/* 鱼虾游戏足球大战 PWA 安装入口与服务注册 */
(function () {
  var installPrompt = null;
  var installBtn = null;
  var offlineTip = null;

  function updateOfflineTip() {
    if (!offlineTip) return;
    offlineTip.style.display = navigator.onLine ? 'none' : 'block';
  }

  function ensureOfflineTip() {
    if (offlineTip) return;
    offlineTip = document.getElementById('offline-tip');
    if (!offlineTip) {
      offlineTip = document.createElement('div');
      offlineTip.id = 'offline-tip';
      offlineTip.setAttribute('role', 'status');
      offlineTip.setAttribute('aria-live', 'polite');
      offlineTip.textContent = '离线模式 · 单机可玩，联机暂不可用';
      offlineTip.style.cssText = [
        'position:fixed', 'left:50%', 'bottom:calc(18px + env(safe-area-inset-bottom, 0px))',
        'transform:translateX(-50%)', 'z-index:95', 'display:none',
        'padding:8px 14px', 'border-radius:999px', 'pointer-events:none',
        'background:rgba(227,6,19,.92)', 'color:#fff', 'font-size:12px',
        'font-weight:700', 'white-space:nowrap', 'box-shadow:0 6px 20px rgba(0,0,0,.35)'
      ].join(';');
      document.body.appendChild(offlineTip);
    }
    updateOfflineTip();
  }

  function isStandalone() {
    return (window.matchMedia && window.matchMedia('(display-mode: standalone)').matches) ||
      window.navigator.standalone === true;
  }

  function showInstallButton() {
    if (!installBtn) return;
    if (isStandalone() || !installPrompt) return;
    installBtn.style.display = 'inline-block';
  }

  function hideInstallButton() {
    if (installBtn) installBtn.style.display = 'none';
  }

  function initialize() {
    ensureOfflineTip();
    window.addEventListener('online', updateOfflineTip);
    window.addEventListener('offline', updateOfflineTip);

    installBtn = document.getElementById('btn-install');
    if (installBtn) {
      installBtn.addEventListener('click', async function () {
        if (!installPrompt) {
          return;
        }
        try {
          installPrompt.prompt();
          var choice = await installPrompt.userChoice;
          if (choice && choice.outcome === 'accepted') {
            hideInstallButton();
          }
          installPrompt = null;
        } catch (e) {
          /* 浏览器可能取消了安装；保持按钮可再次尝试 */
        }
      });
    }

    window.addEventListener('beforeinstallprompt', function (event) {
      event.preventDefault();
      installPrompt = event;
      showInstallButton();
    });

    window.addEventListener('appinstalled', function () {
      installPrompt = null;
      hideInstallButton();
    });

    if (isStandalone()) hideInstallButton();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initialize);
  } else {
    initialize();
  }

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', function () {
      navigator.serviceWorker.register('./sw.js', { scope: './' }).catch(function (err) {
        console.warn('[PWA] Service worker 注册失败', err);
      });
    });
  }
})();
