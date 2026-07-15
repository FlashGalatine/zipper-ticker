// Zipper overlay renderer — consumes ticker:update CustomEvents from panel-core
// and rebuilds the seamless marquee (StreamOverlay-NFC technique: duplicate the
// segment and translateX 0 → -50%; repeat items first if the content is shorter
// than the viewport so the -50% wrap never shows a gap).

(() => {
  'use strict';

  const q = (() => { try { return new URLSearchParams(location.search); } catch { return new URLSearchParams(''); } })();

  // Width: ?w=<px> (http-served sources only) or window.__TICKER_WIDTH, default 1920.
  const w = Number(q.get('w')) || Number(window.__TICKER_WIDTH) || 0;
  if (w >= 200) document.documentElement.style.setProperty('--ticker-width', `${Math.round(w)}px`);
  // Height: ?h=<px> or window.__TICKER_HEIGHT, default 72 (see ticker.css).
  const h = Number(q.get('h')) || Number(window.__TICKER_HEIGHT) || 0;
  if (h >= 20) document.documentElement.style.setProperty('--ticker-height', `${Math.round(h)}px`);
  const speedParam = Number(q.get('speed')) || Number(window.__TICKER_SPEED) || 0;
  if (speedParam > 0) document.documentElement.style.setProperty('--ticker-speed', String(speedParam));

  const track = document.getElementById('track');
  const wrap = document.getElementById('track-wrap');
  const capLeft = document.getElementById('cap-left');
  const capRight = document.getElementById('cap-right');

  function esc(s) {
    const d = document.createElement('div');
    d.textContent = String(s == null ? '' : s);
    return d.innerHTML;
  }

  function nameSpan(p, isWinner) {
    return `<span class="name${isWinner ? ' win' : ''}">${esc(p?.name ?? 'TBD')}</span>`;
  }

  function itemHtml(m) {
    const round = m.round ? `<span class="round">${esc(m.round)}</span>` : '';
    if (m.state === 'completed') {
      // Unreported or negative scores (DQ, forfeit) → "Winner def. Loser".
      const bad = (v) => v == null || v < 0;
      if (bad(m.p1?.score) || bad(m.p2?.score)) {
        const [w, l] = m.winner === 2 ? [m.p2, m.p1] : [m.p1, m.p2];
        return `<span class="item">${round}${nameSpan(w, true)}` +
          `<span class="vs">def.</span>${nameSpan(l, false)}</span>`;
      }
      return `<span class="item">${round}${nameSpan(m.p1, m.winner === 1)}` +
        `<span class="score">${esc(m.p1?.score ?? '')} – ${esc(m.p2?.score ?? '')}</span>` +
        `${nameSpan(m.p2, m.winner === 2)}</span>`;
    }
    if (m.state === 'in_progress') {
      const score = (m.p1?.score != null && m.p2?.score != null)
        ? `<span class="score">${esc(m.p1.score)} – ${esc(m.p2.score)}</span>`
        : '<span class="vs">vs</span>';
      return `<span class="item"><span class="live-pip"></span><span class="live-tag">LIVE</span>` +
        `${round}${nameSpan(m.p1, false)}${score}${nameSpan(m.p2, false)}</span>`;
    }
    // upcoming
    return `<span class="item"><span class="round">Up next${m.round ? ' · ' + esc(m.round) : ''}</span>` +
      `${nameSpan(m.p1, false)}<span class="vs">vs</span>${nameSpan(m.p2, false)}</span>`;
  }

  function standingHtml(s) {
    return `<span class="item"><span class="place">${esc(ordinal(s.placement))}</span>` +
      `<span class="name">${esc(s.name)}</span></span>`;
  }

  function messageHtml(line) {
    return `<span class="item message"><span class="msg">${esc(line)}</span></span>`;
  }

  function ordinal(n) {
    const v = Number(n) || 0;
    const suf = (v % 100 >= 11 && v % 100 <= 13) ? 'th' : ({ 1: 'st', 2: 'nd', 3: 'rd' }[v % 10] || 'th');
    return `${v}${suf}`;
  }

  function setStatic(text) {
    track.className = 'track static';
    track.style.removeProperty('--dur');
    track.innerHTML = `<span class="item">${esc(text)}</span>`;
  }

  function rebuild(items) {
    if (!items.length) return; // caller handles the empty state
    track.className = 'track';
    let seg = `<div class="seg">${items.join('')}</div>`;
    track.innerHTML = seg;
    // Repeat the content until one segment fills the viewport, so the -50% wrap
    // is seamless even with a single short item.
    const wrapW = wrap.clientWidth || 1;
    let inner = items.join('');
    let guard = 0;
    while (track.scrollWidth < wrapW && guard++ < 50) {
      inner += items.join('');
      track.innerHTML = `<div class="seg">${inner}</div>`;
    }
    const segEl = track.firstElementChild;
    const segW = segEl.scrollWidth || 1;
    track.innerHTML = `<div class="seg">${inner}</div><div class="seg">${inner}</div>`;
    // Duration = one segment width / speed (px per second) → constant apparent speed.
    const speed = Number(getComputedStyle(document.documentElement).getPropertyValue('--ticker-speed')) || 80;
    track.style.setProperty('--dur', `${(segW / speed).toFixed(2)}s`);
  }

  function applyCaps(caps) {
    const logo = caps && typeof caps.logo === 'string' ? caps.logo.trim() : '';
    const text = caps && typeof caps.text === 'string' ? caps.text.trim() : '';
    if (logo) {
      capLeft.innerHTML = '';
      const img = document.createElement('img');
      img.src = logo;
      img.alt = '';
      capLeft.appendChild(img);
    }
    capLeft.classList.toggle('on', !!logo);
    capRight.textContent = text;
    capRight.classList.toggle('on', !!text);
  }

  function apply(payload) {
    applyCaps(payload.caps);
    // Persistent message mode takes the strip over: while the payload carries a
    // message block, its lines crawl instead of the results underneath, which
    // keep updating so toggling it off restores them immediately.
    const lines = Array.isArray(payload.message?.lines)
      ? payload.message.lines.map((l) => String(l ?? '').trim()).filter(Boolean)
      : [];
    if (lines.length) { rebuild(lines.map(messageHtml)); return; }
    const items = [];
    for (const m of Array.isArray(payload.matches) ? payload.matches : []) items.push(itemHtml(m));
    if (!items.length) {
      for (const s of Array.isArray(payload.standings) ? payload.standings : []) items.push(standingHtml(s));
    }
    if (items.length) rebuild(items);
    else setStatic(payload.tournament?.name ? `${payload.tournament.name} — waiting for results…` : 'waiting for results…');
  }

  window.addEventListener('ticker:update', (e) => {
    try { apply(e.detail || {}); } catch (err) { console.error('[zipper] render failed:', err); }
  });

  // ── Announcement takeover: pause the crawl, show the announcement, resume.
  // Queued so back-to-back subs/raids play one after another.
  const zipper = document.querySelector('.zipper');
  const announceWrap = document.getElementById('announce');
  const announcePill = document.getElementById('announce-pill');
  const announceText = document.getElementById('announce-text');
  const PILL_LABELS = { sub: 'New Sub', raid: 'Raid', milestone: 'Milestone' };
  const announceQueue = [];
  let announcing = false;

  function pumpAnnouncements() {
    if (announcing || !announceQueue.length) return;
    announcing = true;
    const a = announceQueue.shift();
    const kind = PILL_LABELS[a.kind] ? a.kind : 'custom';
    announceWrap.dataset.kind = kind;
    announcePill.textContent = PILL_LABELS[kind] || 'Announcement';
    announceText.textContent = a.text;
    zipper.classList.add('announcing');
    const durationMs = Math.min(30, Math.max(2, Number(a.duration) || 8)) * 1000;
    setTimeout(() => {
      zipper.classList.remove('announcing');
      announcing = false;
      pumpAnnouncements();
    }, durationMs);
  }

  window.addEventListener('ticker:announce', (e) => {
    const a = e.detail || {};
    if (typeof a.text !== 'string' || !a.text.trim()) return;
    if (announceQueue.length >= 10) return; // sanity cap during raid trains
    announceQueue.push({ kind: a.kind, text: a.text.trim().slice(0, 200), duration: a.duration });
    pumpAnnouncements();
  });

  setStatic('waiting for results…');
})();
