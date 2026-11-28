/* zhuj杯 2026 PWA 安装入口与服务注册 */
(function () {
  var installPrompt = null;
  var installBtn = null;

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
