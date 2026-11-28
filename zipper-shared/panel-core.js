// panel-core — the Streamer.bot transport the Zipper overlay + control page load.
//
// Serve this file at /zipper-shared/panel-core.js. It speaks Streamer.bot's
// WebSocket protocol with a raw browser WebSocket (no @streamerbot/client / CDN,
// so it works offline in the OBS CEF browser source). It subscribes to
// General.Custom and unwraps every ticker:* broadcast into a window CustomEvent
// named after the payload's `type`, with the payload as `detail`. Any program on
// the socket can broadcast one; Zipper's actions send these:
//
//   ticker:update   — the full ticker payload. One stamped older than the newest
//                     this page has applied can be dropped (the stamp rule, see dispatch).
//   ticker:command  — a command Ticker Command relayed: { command, value }. The
//                     sidecar's status arrives this way too, as command "__status"
//                     with the status JSON as its value.
//   ticker:announce — an announcement: { kind, text, duration }.
//   ticker:error    — an action threw, or refused what it was sent: { code, message,
//                     action }, action being the sender's name (an action pasted
//                     before 1.0.32 sends { message } alone). Read `message`; read
//                     `code` only when it is there. Nothing here or on the pages
//                     reads `action`.
//
// It raises these of its own, named outside ticker:* so no broadcast can pass for one:
//
//   sb:connection     — { connected }. true once the socket is open and the Subscribe
//                       has been sent, so a request sent from a listener cannot reach
//                       Streamer.bot before it. false when the socket closes, or when
//                       the browser refuses to open one (a blocked port, say), with
//                       notSent (how many requests waiting here were never sent),
//                       notSentIds (their ids) and inFlight ({ id, action, replied }
//                       for the request on its way, which may not have run; or null).
//                       With a refused ?sbport (below) it is raised once, false, with
//                       reason 'sbport-refused' and nothing dropped; no socket is opened
//                       at all. It comes on a zero timer that starts at DOMContentLoaded,
//                       or as this file runs when the document is already parsed. That
//                       can be before the window's load event, so a listener added later
//                       reads __ZIPPER_SB_PORT_REFUSED instead.
//   sb:action-queued  — { action, id, sync, queueName, defaultQueue }: Streamer.bot
//                       answered ok. The action was QUEUED; it has not run yet.
//                       defaultQueue is true when queueName is "Default", the queue in
//                       which two runs of one action can share one run's arguments.
//   sb:action-error   — { action, id, sync, code, error }: an error answer, with
//                       Streamer.bot's own text in `error`. code is 'action-missing'
//                       for "The action '…' was not found", and 'error' otherwise.
//                       Nothing ran.
//   sb:action-timeout — { action, id, sync }: no answer within 800 ms. One that comes
//                       later still raises one of the two above, unless the socket
//                       closed first or more than 32 requests were waiting for an
//                       answer (the oldest is then no longer looked for).
//   In the last three, `id` is the request's id and `sync` is true for the
//   connect-sync request (below), false for one sent through __ZIPPER_DO_ACTION.
//
// Streamer.bot has no state-on-connect replay. We recreate it: after the
// Subscribe is acknowledged we fire a DoAction naming the push action, which
// re-broadcasts the persisted payload — so a source added mid-tournament paints
// immediately instead of staying blank. That request goes on its own id, not
// through the send queue below, and its reply is read like any other.
//
// Config (set a window global before this loads, or — only when served over
// http:// — pass a URL query; file:// OBS sources cannot take query params):
//   window.__SB_WS_URL      — SB WebSocket Server URL (default ws://127.0.0.1:8080/)
//   ?sbport=<n>             — shorthand for ws://127.0.0.1:<n>/  (http:// sources only).
//                             Digits only, giving 1 to 65535. Leading zeros are allowed,
//                             however many (008080 is 8080). A ?sbport that is there and
//                             is anything else (an empty one too) is refused, and the page
//                             dials nothing: no socket and no reconnect. It is marked
//                             offline, __ZIPPER_SB_PORT_REFUSED is true, and sb:connection
//                             says why (above). window.__SB_WS_URL, when set, still wins
//                             over ?sbport. With no ?sbport, the default port is dialled.
//   window.__SB_SYNC_ACTION — action to DoAction on connect (default 'Ticker Push'; '' to skip)
//   window.__SB_DEBUG / ?sbdebug=1 — log the connection + message flow
//
// For a page: window.__ZIPPER_DO_ACTION(name, args) queues a DoAction and returns its
// request id (a string), or false when the socket is not open (nothing is kept).
// window.__ZIPPER_SEND_COMMAND(command, value) does the same for Ticker Command.
// window.__ZIPPER_PANEL_CORE is { api: 2 } (frozen), set as this file runs, so a page can
// tell this file from an older cached copy: the copy before it (Release 1.0.24 and
// earlier) set no such marker, raised no sb:* event, and its send functions returned
// true, not a request id. window.__ZIPPER_SB_PORT_REFUSED (true or false) is set then too.

(() => {
  'use strict';

  const DEFAULT_WS_URL = 'ws://127.0.0.1:8080/';
  const q = (() => { try { return new URLSearchParams(location.search); } catch { return new URLSearchParams(''); } })();
  // ?sbport= goes into a URL, so it must be a port and nothing else (ZIP-18). A value
  // such as "1@evil.example" would make 127.0.0.1 a user name and the rest the host.
  // Only the number goes into the URL, so leading zeros, however many, change nothing.
  const SBPORT = (() => {
    const raw = q.get('sbport') || '';
    const n = /^\d+$/.test(raw) ? Number(raw) : 0;
    return n >= 1 && n <= 65535 ? n : 0;
  })();
  // A ?sbport that is there and refused (an empty one too) dials nothing (the owner's
  // ruling of 2026-11-26): the page was meant for another port, so 8080 is not dialled
  // in its place. window.__SB_WS_URL still wins over ?sbport, a refused one too.
  const SBPORT_REFUSED = !window.__SB_WS_URL && q.has('sbport') && !SBPORT;
  const WS_URL = window.__SB_WS_URL
    || (SBPORT ? `ws://127.0.0.1:${SBPORT}/` : DEFAULT_WS_URL);
  // What a page can read of this file: the marker (api 2: request ids and the sb:* events;
  // the copy before it set none, and its send functions returned true), and whether the
  // address's ?sbport was refused. Both are set as this file runs.
  window.__ZIPPER_PANEL_CORE = Object.freeze({ api: 2 });
  window.__ZIPPER_SB_PORT_REFUSED = SBPORT_REFUSED;
  const SYNC_ACTION = (typeof window.__SB_SYNC_ACTION === 'string')
    ? window.__SB_SYNC_ACTION
    : 'Ticker Push';
  const DEBUG = /[?&]sbdebug=1/.test(location.search) || !!window.__SB_DEBUG;
  // Reconnecting (decision 11 (b)): the wait grows by half each time, up to
  // RECONNECT_MAX_MS, and goes back to RECONNECT_BASE_MS only once a connection has stayed
  // open STABLE_OPEN_MS. A Streamer.bot that accepts a connection and drops it is then not
  // dialled every 2 s for good. Each wait gets up to RECONNECT_JITTER of itself on top, at
  // random, so the pages that lost Streamer.bot together do not all come back (and all ask
  // for a sync) in the same instant.
  const RECONNECT_BASE_MS = 2000;
  const RECONNECT_MAX_MS = 15000;
  const RECONNECT_JITTER = 0.25;
  const STABLE_OPEN_MS = 5000;
  const SYNC_FALLBACK_MS = 400; // fire the sync DoAction even if the ack shape isn't recognized
  // The send queue's fail-safe (decision 13): the longest one request holds up the next,
  // and how long a request waits for its answer before sb:action-timeout.
  const SEND_RELEASE_MS = 800;
  const AWAITING_MAX = 32; // requests whose answer is still looked for; the oldest goes first

  let ws = null;
  let reconnectDelay = RECONNECT_BASE_MS;
  let msgId = 0;

  const log = (...a) => { if (DEBUG) console.log('[panel-core-sb]', ...a); };
  const emit = (type, detail) => window.dispatchEvent(new CustomEvent(type, { detail }));

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

  // ── Sending ────────────────────────────────────────────────────────────────
  // Outbound DoActions are SERIALIZED: one in flight at a time per page. When two runs of
  // one action overlap in Streamer.bot's Default queue, both see one request's arguments
  // (first seen on SB 1.0.4, measured again on 1.0.7, from overlapping runs of one action,
  // and across sockets too). A blocking action queue in Streamer.bot removes it for every
  // sender (README, setup step 1); this queue is a second line of defence for an install
  // without one, and orders only this page's own requests.
  //
  // Streamer.bot answers a DoAction when the action is QUEUED, not when it has run, so the
  // answer cannot pace the next send (decision 13). What shows that a run is over is its
  // own broadcast: for Ticker Command the ticker:command echo of the same command and
  // value; for Ticker Announce the ticker:announce of the same kind and text, trimmed as
  // the C# trims. The next request goes once the answer AND that broadcast are both in, in
  // either order, or after SEND_RELEASE_MS. An error answer lets the next go at once
  // (nothing was queued). A broadcast that comes after its request was released is that
  // request's, not the next one's (see releasedEarly below).
  // The sidecar's own queue follows the same rule, and also takes a ticker:error as the
  // end of a run; this one does not (decision 13 names only the three above), so a run
  // that throws holds the next request for SEND_RELEASE_MS.
  //
  // Everything waiting is dropped when the socket closes, and nothing is sent again after
  // a reconnect: a stale command could undo a newer change. sb:connection says how many
  // requests were dropped, and which one was on its way.
  const sendQueue = [];            // requests not sent yet
  let inFlight = null;             // the request sent and not yet released
  const awaitingReply = new Map(); // request id → request, until its answer is read

  const about = (job) => ({ action: job.name, id: job.id, sync: job.sync });
  const newJob = (name, args, sync) => ({
    id: String(++msgId), name: String(name), args: args || {}, sync, replied: false, echoed: false, timer: null,
  });

  // Trimmed as the C# actions trim (.NET String.Trim, Char.IsWhiteSpace), so a text is
  // compared with what its run broadcasts. JavaScript's trim() differs at two characters:
  // it leaves U+0085 (next line), which .NET takes, and takes U+FEFF, which .NET keeps.
  const CS_TRIM = /^[\t\n\v\f\r \u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+|[\t\n\v\f\r \u0085\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000]+$/g;
  const csTrim = (s) => String(s ?? '').replace(CS_TRIM, '');

  // Whether a broadcast is the run of this request ending (see above). Other actions have
  // no broadcast to wait for: they go on the answer's error or the fail-safe.
  function isOwnBroadcast(job, d) {
    const a = job.args || {};
    if (job.name === 'Ticker Command') {
      return d.type === 'ticker:command' && d.command === csTrim(a.command)
        && d.value === (a.value == null ? '' : String(a.value));
    }
    if (job.name === 'Ticker Announce') {
      return d.type === 'ticker:announce' && d.text === csTrim(a.text)
        && d.kind === (csTrim(a.kind) || 'custom');
    }
    return false;
  }

  // Requests the fail-safe released before their own broadcast came. Two requests can be
  // alike (two clicks on Poll now): the late broadcast of the first must not pass for the
  // second's and release the request after it while the second still runs. So a broadcast
  // is first offered here, oldest first, and ends the request in flight only when none of
  // these takes it. One is kept up to LATE_KEEP_MS, and only the newest LATE_MAX; one
  // whose answer turns out to be an error is dropped (it never runs); all go on a close.
  const LATE_KEEP_MS = 5000;
  const LATE_MAX = 8;
  const releasedEarly = []; // { job, at }, oldest first
  function keepReleased(job) {
    if (job.name !== 'Ticker Command' && job.name !== 'Ticker Announce') return;
    releasedEarly.push({ job, at: monoNow() });
    if (releasedEarly.length > LATE_MAX) releasedEarly.shift();
  }
  function forgetReleased(job) {
    const i = releasedEarly.findIndex((r) => r.job === job);
    if (i >= 0) releasedEarly.splice(i, 1);
  }
  // Whether a broadcast is the late one of a request released before it came.
  function takenByReleased(d) {
    const now = monoNow();
    while (releasedEarly.length && now - releasedEarly[0].at > LATE_KEEP_MS) releasedEarly.shift();
    const i = releasedEarly.findIndex((r) => isOwnBroadcast(r.job, d));
    if (i < 0) return false;
    releasedEarly.splice(i, 1);
    return true;
  }

  function expectReply(job) {
    awaitingReply.set(job.id, job);
    if (awaitingReply.size > AWAITING_MAX) {
      const [oldId, old] = awaitingReply.entries().next().value;
      if (old !== inFlight && old.timer) { clearTimeout(old.timer); old.timer = null; }
      awaitingReply.delete(oldId);
    }
  }

  // Sends one request on the socket now, with its answer looked for. False when it could
  // not be sent.
  function sendNow(job) {
    expectReply(job);
    try {
      ws.send(JSON.stringify({ request: 'DoAction', id: job.id, action: { name: job.name }, args: job.args }));
    } catch {
      awaitingReply.delete(job.id);
      return false;
    }
    job.timer = setTimeout(() => {
      job.timer = null;
      if (!job.replied) { log('no answer within', SEND_RELEASE_MS, 'ms to DoAction id', job.id); emit('sb:action-timeout', about(job)); }
      if (inFlight === job && !job.echoed) keepReleased(job);
      release(job);
    }, SEND_RELEASE_MS);
    return true;
  }

  function pump() {
    if (inFlight || !sendQueue.length || !ws || ws.readyState !== WebSocket.OPEN) return;
    const job = sendQueue.shift();
    inFlight = job;
    if (!sendNow(job)) { inFlight = null; sendQueue.unshift(job); } // the close that follows reports it
  }

  function release(job) {
    if (inFlight !== job) return;
    if (job.timer) { clearTimeout(job.timer); job.timer = null; }
    inFlight = null;
    pump();
  }

  // An answer to one of our DoActions (never the action's result: it has not run yet).
  function readReply(job, m) {
    awaitingReply.delete(job.id);
    job.replied = true;
    if (m.status && m.status !== 'ok') {
      const error = typeof m.error === 'string' ? m.error : '';
      log('DoAction id', job.id, 'refused:', error);
      emit('sb:action-error', { ...about(job), code: /was not found/i.test(error) ? 'action-missing' : 'error', error });
      forgetReleased(job); // released before this answer came: it will not run
      release(job);
      return;
    }
    const queueName = (m.args && typeof m.args.queueName === 'string') ? m.args.queueName : null;
    emit('sb:action-queued', { ...about(job), queueName, defaultQueue: queueName === 'Default' });
    if (job.echoed) release(job);
  }

  // Drops everything waiting or on its way, and says what it dropped.
  function clearQueue() {
    const notSentIds = sendQueue.map((j) => j.id);
    const was = inFlight ? { id: inFlight.id, action: inFlight.name, replied: inFlight.replied } : null;
    for (const job of awaitingReply.values()) if (job.timer) { clearTimeout(job.timer); job.timer = null; }
    if (inFlight && inFlight.timer) { clearTimeout(inFlight.timer); inFlight.timer = null; }
    sendQueue.length = 0;
    inFlight = null;
    awaitingReply.clear();
    releasedEarly.length = 0;
    return { notSent: notSentIds.length, notSentIds, inFlight: was };
  }

  // Exposed for the control page: queue a named action with arguments. Returns the
  // request's id, or false when the socket is not open (nothing is kept).
  window.__ZIPPER_DO_ACTION = (name, args) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return false;
    const job = newJob(name, args, false);
    sendQueue.push(job);
    pump();
    return job.id;
  };
  // Shorthand for the common case: a Ticker Command.
  window.__ZIPPER_SEND_COMMAND = (command, value) => window.__ZIPPER_DO_ACTION('Ticker Command', {
    command: String(command),
    value: value == null ? '' : String(value),
  });

  // ── Connecting ─────────────────────────────────────────────────────────────
  function scheduleReconnect() {
    const delay = Math.round(reconnectDelay * (1 + RECONNECT_JITTER * Math.random()));
    reconnectDelay = Math.min(reconnectDelay * 1.5, RECONNECT_MAX_MS);
    log('reconnecting in', delay, 'ms');
    setTimeout(connect, delay);
  }

  function connect() {
    log('connecting to', WS_URL);
    let sock;
    try {
      sock = new WebSocket(WS_URL);
    } catch (err) {
      // A URL the constructor refuses (ZIP-18), such as a port the browser blocks. Keep
      // trying: the transport must not die here. The page hears it as a closed socket, so
      // it says Streamer.bot is offline instead of saying nothing.
      ws = null;
      log('could not open a WebSocket to', WS_URL, '-', err && err.message);
      setOffline(true);
      emit('sb:connection', { connected: false, ...clearQueue() });
      scheduleReconnect();
      return;
    }
    ws = sock;

    let subId = null;
    let syncFired = false;
    let syncTimer = null;
    let stableTimer = null;
    let closed = false;

    function fireSync() {
      if (syncFired || closed) return;
      syncFired = true;
      if (syncTimer) { clearTimeout(syncTimer); syncTimer = null; }
      if (!SYNC_ACTION) return;
      log('requesting state via DoAction', JSON.stringify(SYNC_ACTION));
      sendNow(newJob(SYNC_ACTION, { reason: 'overlay-connect' }, true));
    }

    sock.onopen = () => {
      // The wait goes back to its start only once this connection has stayed open.
      stableTimer = setTimeout(() => { stableTimer = null; reconnectDelay = RECONNECT_BASE_MS; }, STABLE_OPEN_MS);
      setOffline(false);

      // The event-SOURCE key is LOWERCASE ('general') in the Subscribe request even
      // though delivered events carry a capitalized source ('General'). This
      // asymmetry is per Streamer.bot's docs; capital 'General' here silently
      // receives nothing.
      subId = String(++msgId);
      sock.send(JSON.stringify({ request: 'Subscribe', id: subId, events: { general: ['Custom'] } }));
      log('sent Subscribe (id', subId + '); waiting for ack before sync');

      syncTimer = setTimeout(fireSync, SYNC_FALLBACK_MS);
      // After the Subscribe, so a request a listener sends now goes behind it.
      emit('sb:connection', { connected: true });
    };

    sock.onmessage = (evt) => {
      let m;
      try { m = JSON.parse(evt.data); } catch { return; }

      // Subscribe acknowledgement (response carrying our id, not an event) → now
      // safe to ask for the state re-broadcast.
      if (m && m.id && m.id === subId && !m.event) {
        log('Subscribe ack:', m.status || '(no status field)');
        fireSync();
        return;
      }

      // The answer to one of our DoActions: the connect-sync or a queued request.
      if (m && m.id != null && !m.event && awaitingReply.has(String(m.id))) {
        readReply(awaitingReply.get(String(m.id)), m);
        return;
      }

      // The payload we care about: General.Custom broadcasts.
      if (m && m.event && m.event.source === 'General' && m.event.type === 'Custom') {
        let d = m.data;
        if (typeof d === 'string') { try { d = JSON.parse(d); } catch { /* leave as string */ } }
        if (!d || typeof d !== 'object' || !d.type) { log('General.Custom with no data.type — ignored'); return; }
        log('General.Custom →', d.type);
        // The release comes before the page hears of the broadcast: a request a listener
        // sends in answer to it is a new run, which this broadcast does not end. A late
        // broadcast of a request already released is not taken for the one in flight.
        if (takenByReleased(d)) log('the late broadcast of a request already released');
        else if (inFlight && !inFlight.echoed && isOwnBroadcast(inFlight, d)) {
          inFlight.echoed = true;
          if (inFlight.replied) release(inFlight);
        }
        dispatch(d);
        return;
      }
    };

    sock.onclose = () => {
      if (closed) return;
      closed = true;
      if (stableTimer) { clearTimeout(stableTimer); stableTimer = null; }
      if (syncTimer) { clearTimeout(syncTimer); syncTimer = null; }
      const dropped = clearQueue();
      setOffline(true);
      log('closed; reconnecting');
      emit('sb:connection', { connected: false, ...dropped });
      scheduleReconnect();
    };
    sock.onerror = () => { log('WebSocket error — is SB\'s WebSocket Server enabled on', WS_URL, 'with authentication OFF?'); };
  }

  function init() {
    setOffline(true);
    if (!SBPORT_REFUSED) { connect(); return; }
    // A refused ?sbport: no socket and no reconnect. The page hears it once, on a zero
    // timer, so a listener added after this file ran (in a script after it) hears it too;
    // one added later still reads window.__ZIPPER_SB_PORT_REFUSED.
    log('?sbport= refused (not a port from 1 to 65535): not connecting');
    setTimeout(() => emit('sb:connection', { connected: false, reason: 'sbport-refused', notSent: 0, notSentIds: [], inFlight: null }), 0);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
  else init();
})();
