// panel-core — the Streamer.bot transport the Zipper overlay + control page load.
//
// Serve this file at /zipper-shared/panel-core.js. It speaks Streamer.bot's
// WebSocket protocol with a raw browser WebSocket (no @streamerbot/client / CDN,
// so it works offline in the OBS CEF browser source). It subscribes to
// General.Custom and unwraps every broadcast into window CustomEvents keyed by
// the payload's `type`, except a ticker:update the stamp rule drops (see dispatch):
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

  // The stamp rule (ZIP-11, decision 14): the newest generatedAt of a ticker:update this
  // page has applied, in ms, and when it applied it, on the monotonic clock.
  let newestStamp = null;
  let newestAt = null;
  // How long after applying its newest stamp the page drops an update stamped older.
  const STALE_WINDOW_MS = 5000;
  // window.performance, read at each call: the verify suite's browser shim hands the page a
  // stand-in clock there, so it can test the window without waiting for it.
  const monoNow = () => window.performance.now();

  // Every ticker:* payload becomes a same-named window CustomEvent with the
  // payload as detail — the overlay and control page pick what they need.
  //
  // A ticker:update whose generatedAt is strictly older than the newest one this page
  // has applied is dropped (ZIP-11), but only within STALE_WINDOW_MS of the moment it
  // applied that newest one: a page's connect-sync replay that read the stored payload
  // just before the sidecar replaced it arrives within moments of the new one. An older
  // one that comes later is applied, and its stamp becomes the newest, so later updates
  // are judged against it: a stamp from a clock that was ahead (a sidecar restarted after
  // its clock was set back, or a far-future stamp) cannot freeze the page past the window.
  // The window runs on the monotonic clock, so a change of the wall clock does not move it.
  // Stamps are compared as dates (Date.parse), never as text: the fixture's "…:00Z" and
  // toISOString's "…:00.500Z" sort the wrong way round as text. An equal stamp is applied
  // (a replay carries the same content), and so is an update with no stamp, or one that is
  // not a date, such as the C#'s connect-sync empty update; none of them moves the newest
  // stamp or restarts the window.
  function dispatch(d) {
    if (!d || typeof d.type !== 'string' || !d.type.startsWith('ticker:')) return;
    if (d.type === 'ticker:update') {
      const t = typeof d.generatedAt === 'string' ? Date.parse(d.generatedAt) : NaN;
      if (Number.isFinite(t)) {
        const now = monoNow();
        if (newestStamp !== null && t < newestStamp && now - newestAt <= STALE_WINDOW_MS) {
          log('older ticker:update ignored', d.generatedAt);
          return;
        }
        if (t !== newestStamp) { newestStamp = t; newestAt = now; } // newer, or older past the window
      }
    }
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

      // Ack for the in-flight queued DoAction → release the next one.
      if (m && m.id && m.id === inFlightId && !m.event) { advanceQueue(); return; }

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

    ws.onclose = () => {
      setOffline(true);
      // Drop any queued/in-flight sends — replaying stale commands after a
      // reconnect could clobber fresh state.
      sendQueue.length = 0;
      if (inFlightTimer) { clearTimeout(inFlightTimer); inFlightTimer = null; }
      inFlightId = null;
      log('closed; reconnecting');
      scheduleReconnect();
    };
    ws.onerror = () => { log('WebSocket error — is SB\'s WebSocket Server enabled on', WS_URL, 'with authentication OFF?'); };
  }

  function scheduleReconnect() {
    setTimeout(() => {
      reconnectDelay = Math.min(reconnectDelay * 1.5, RECONNECT_MAX_MS);
      connect();
    }, reconnectDelay);
  }

  // Outbound DoActions are SERIALIZED — one in flight at a time, the next sent
  // only after the previous is acked (or a short fail-safe timeout). Streamer.bot
  // bleeds arguments between action invocations that arrive in a burst: two
  // DoActions to the same action within a few ms get their args shuffled
  // together (verified against SB 1.0.4). A control page firing several settings
  // at once would corrupt them, so we never let two overlap on the wire.
  const sendQueue = [];
  let inFlightId = null;
  let inFlightTimer = null;
  const ACK_TIMEOUT_MS = 800;

  function pumpQueue() {
    if (inFlightId !== null || !sendQueue.length) return;
    if (!ws || ws.readyState !== WebSocket.OPEN) return; // resumes on reconnect
    const req = sendQueue.shift();
    req.id = String(++msgId);
    inFlightId = req.id;
    try { ws.send(JSON.stringify(req)); }
    catch { inFlightId = null; return; }
    inFlightTimer = setTimeout(advanceQueue, ACK_TIMEOUT_MS);
  }
  function advanceQueue() {
    if (inFlightTimer) { clearTimeout(inFlightTimer); inFlightTimer = null; }
    inFlightId = null;
    pumpQueue();
  }

  // Exposed for the control page: fire a named action with arguments (queued).
  window.__ZIPPER_DO_ACTION = (name, args) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    sendQueue.push({ request: 'DoAction', action: { name: String(name) }, args: args || {} });
    pumpQueue();
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
