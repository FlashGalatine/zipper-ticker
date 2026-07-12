// panel-core — the Streamer.bot transport the Zipper overlay + control page load.
//
// Serve this file at /zipper-shared/panel-core.js. It speaks Streamer.bot's
// WebSocket protocol with a raw browser WebSocket (no @streamerbot/client / CDN,
// so it works offline in the OBS CEF browser source). It subscribes to
// General.Custom and unwraps every broadcast into window CustomEvents keyed by
// the payload's `type`:
//
//   ticker:update  — full ticker payload (detail = the payload object)
//   ticker:status  — sidecar status       (detail = the payload object)
//   ticker:error   — action-side failure  (detail = { message })
//
// Streamer.bot has no state-on-connect replay. We recreate it: after the
// Subscribe is acknowledged we fire a DoAction naming the push action, which
// re-broadcasts the persisted payload — so a source added mid-tournament paints
// immediately instead of staying blank.
//
// Config (set a window global before this loads, or — only when served over
// http:// — pass a URL query; file:// OBS sources cannot take query params):
//   window.__SB_WS_URL      — SB WebSocket Server URL (default ws://127.0.0.1:8080/)
//   ?sbport=<n>             — shorthand for ws://127.0.0.1:<n>/  (http:// sources only)
//   window.__SB_SYNC_ACTION — action to DoAction on connect (default 'Ticker Push'; '' to skip)
//   window.__SB_DEBUG / ?sbdebug=1 — log the connection + message flow

(() => {
  'use strict';

  const q = (() => { try { return new URLSearchParams(location.search); } catch { return new URLSearchParams(''); } })();
  const WS_URL = window.__SB_WS_URL
    || (q.get('sbport') ? `ws://127.0.0.1:${q.get('sbport')}/` : 'ws://127.0.0.1:8080/');
  const SYNC_ACTION = (typeof window.__SB_SYNC_ACTION === 'string')
    ? window.__SB_SYNC_ACTION
    : 'Ticker Push';
  const DEBUG = /[?&]sbdebug=1/.test(location.search) || !!window.__SB_DEBUG;
  const RECONNECT_BASE_MS = 2000;
  const RECONNECT_MAX_MS = 15000;
  const SYNC_FALLBACK_MS = 400; // fire the sync DoAction even if the ack shape isn't recognized

  let ws = null;
  let reconnectDelay = RECONNECT_BASE_MS;
  let msgId = 0;

  const log = (...a) => { if (DEBUG) console.log('[panel-core-sb]', ...a); };

  // Offline indicator — the overlay styles body.offline.
  const setOffline = (off) => { if (document.body) document.body.classList.toggle('offline', off); };

  // Every ticker:* payload becomes a same-named window CustomEvent with the
  // payload as detail — the overlay and control page pick what they need.
  function dispatch(d) {
    if (!d || typeof d.type !== 'string' || !d.type.startsWith('ticker:')) return;
    window.dispatchEvent(new CustomEvent(d.type, { detail: d }));
  }

  function connect() {
    log('connecting to', WS_URL);
    ws = new WebSocket(WS_URL);

    let subId = null;
    let syncFired = false;
    let syncTimer = null;

    function fireSync() {
      if (syncFired) return;
      syncFired = true;
      if (syncTimer) { clearTimeout(syncTimer); syncTimer = null; }
      if (!SYNC_ACTION) return;
      log('requesting state via DoAction', JSON.stringify(SYNC_ACTION));
      ws.send(JSON.stringify({
        request: 'DoAction',
        id: String(++msgId),
        action: { name: SYNC_ACTION },
        args: { reason: 'overlay-connect' },
      }));
    }

    ws.onopen = () => {
      reconnectDelay = RECONNECT_BASE_MS;
      setOffline(false);

      // The event-SOURCE key is LOWERCASE ('general') in the Subscribe request even
      // though delivered events carry a capitalized source ('General'). This
      // asymmetry is per Streamer.bot's docs; capital 'General' here silently
      // receives nothing.
      subId = String(++msgId);
      ws.send(JSON.stringify({ request: 'Subscribe', id: subId, events: { general: ['Custom'] } }));
      log('sent Subscribe (id', subId + '); waiting for ack before sync');

      syncTimer = setTimeout(fireSync, SYNC_FALLBACK_MS);
    };

    ws.onmessage = (evt) => {
      let m;
      try { m = JSON.parse(evt.data); } catch { return; }

      // Subscribe acknowledgement (response carrying our id, not an event) → now
      // safe to ask for the state re-broadcast.
      if (m && m.id && m.id === subId && !m.event) {
        log('Subscribe ack:', m.status || '(no status field)');
        fireSync();
        return;
      }

      // The payload we care about: General.Custom broadcasts.
      if (m && m.event && m.event.source === 'General' && m.event.type === 'Custom') {
        let d = m.data;
        if (typeof d === 'string') { try { d = JSON.parse(d); } catch { /* leave as string */ } }
        if (!d || typeof d !== 'object' || !d.type) { log('General.Custom with no data.type — ignored'); return; }
        log('General.Custom →', d.type);
        dispatch(d);
        return;
      }
    };

    ws.onclose = () => { setOffline(true); log('closed; reconnecting'); scheduleReconnect(); };
    ws.onerror = () => { log('WebSocket error — is SB\'s WebSocket Server enabled on', WS_URL, 'with authentication OFF?'); };
  }

  function scheduleReconnect() {
    setTimeout(() => {
      reconnectDelay = Math.min(reconnectDelay * 1.5, RECONNECT_MAX_MS);
      connect();
    }, reconnectDelay);
  }

  // Exposed for the control page: fire a named action with arguments.
  window.__ZIPPER_DO_ACTION = (name, args) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    ws.send(JSON.stringify({
      request: 'DoAction',
      id: String(++msgId),
      action: { name: String(name) },
      args: args || {},
    }));
    return true;
  };
  // Shorthand for the common case: a Ticker Command.
  window.__ZIPPER_SEND_COMMAND = (command, value) => window.__ZIPPER_DO_ACTION('Ticker Command', {
    command: String(command),
    value: value == null ? '' : String(value),
  });

  function init() { setOffline(true); connect(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
  else init();
})();
