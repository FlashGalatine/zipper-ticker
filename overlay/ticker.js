// Zipper overlay renderer: takes the ticker:update CustomEvents panel-core raises and runs
// the crawl.
//
// The crawl. #track holds copies (.seg) of one segment: the items, repeated until one
// copy is at least as wide as the strip's middle (#track-wrap). The ticker-scroll
// keyframes move the track left by one copy's width in px (--seg-w), over that width
// divided by --ticker-speed (--dur), and then start again: the second copy is then where
// the first one was, so the loop has no seam.
//
// New results never cut into the crawl (ZIP-04, decisions 4, 8 and 14):
//   - Content equal to what is on the strip, or to what is already waiting, does nothing.
//     The end caps are applied at once either way.
//   - When the wrap is FADE_THRESHOLD_MS or less away, the newest content is held. At the
//     wrap, the copies that are off screen take it, so it scrolls in from the right behind
//     the copy in view; the loop still ends at the old copy's width. At the next wrap every
//     copy holds it, and the loop starts again at offset 0 with its own width and duration.
//   - When the wrap is further off, it is swapped in at once under a short fade of #track,
//     and the crawl carries on from where it was (the owner's ruling of 2026-11-26): the new
//     loop starts at the offset the old one had at the swap, modulo the new copy's width. So
//     updates that come often never keep the end of a long list out of view.
//   - While an announcement hides the track, it is swapped in at once, and the loop starts
//     again at offset 0 (a fade under way when the announcement comes is cut short the same
//     way). With no new content the crawl only pauses there, and goes on where it was. A
//     re-measure with no new content (a late web font, see Re-measuring) while the track is
//     hidden, or under a fade the announcement cuts short, keeps the crawl's place too (the
//     owner's ruling of 2026-11-27). The
//     static line (the first paint, "waiting for results…", the blank strip after a new
//     tournament URL) is always applied at once, and the first crawl after it starts at 0.
// A running loop never gets a new --seg-w or --dur: each change starts a new animation in
// the same frame (restartLoop), at offset 0, or, under the fade, at the old one's offset.
// Only the track's own ticker-scroll events count: the LIVE pip's blink bubbles up to the
// track too.
//
// Re-measuring (ZIP-19): a web font that arrives late changes the width of the items in
// view, and a logo or a cap that changes size changes the width of the strip's middle. A
// ResizeObserver on #track-wrap and on the first copy, the FontFaceSet's loadingdone event
// and a document.fonts.ready re-armed after each build all call remeasure(). Copies that
// changed width are swapped in under the fade, which keeps the crawl's place (at once, from
// offset 0, while hidden). A middle that grew past the copies gets one more copy off screen
// at once, and the loop is built again at its end. Widths are the strip's own px (layoutW),
// so a scaled strip loops true.

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
  const zipper = document.querySelector('.zipper');

  // How near the wrap must be for new content to wait for it. Further off, it comes in at
  // once under the fade. Decision 4's "about 20 seconds"; the owner sets the figure after
  // the look in OBS (decision 8).
  const FADE_THRESHOLD_MS = 20000;
  // Each half of that fade (out, then back in), as the .track transition in ticker.css has
  // it. The swap waits for the opacity transition #track really has (a theme may make it
  // longer), and this is used only when that cannot be read.
  const FADE_MS = 300;
  // A transition starts on the next frame, not when its class is set: the swap waits this
  // much longer than the fade out, so the track is out of sight when it comes.
  const FADE_START_SLACK_MS = 50;
  // A logo that did not load is asked for again, at the same address, by an update that
  // comes at least this long after the failure. The cap stays hidden until it loads.
  const LOGO_RETRY_MS = 60000;
  // How much of a copy may be in view at the strip's right edge and still count as off
  // screen: the width of the edge fade in ticker.css's mask, where the default theme shows
  // nothing. (Each item also starts with 30 px of padding.)
  const EDGE_PX = 24;
  const SCROLL = 'ticker-scroll';
  const MAX_REPEATS = 50; // copies of the items in one segment, at most
  const MAX_COPIES = 10;  // copies of the segment on the track, at most

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
      const bad = (v) => v == null || v < 0;
      if (bad(m.p1?.score) || bad(m.p2?.score)) {
        // No scores and no winner: there is nothing to say who won (ZIP-19).
        if (m.winner !== 1 && m.winner !== 2) {
          return `<span class="item">${round}${nameSpan(m.p1, false)}` +
            `<span class="vs">vs</span>${nameSpan(m.p2, false)}</span>`;
        }
        // Unreported or negative scores (DQ, forfeit) with a winner → "Winner def. Loser".
        const [win, lose] = m.winner === 2 ? [m.p2, m.p1] : [m.p1, m.p2];
        return `<span class="item">${round}${nameSpan(win, true)}` +
          `<span class="vs">def.</span>${nameSpan(lose, false)}</span>`;
      }
      return `<span class="item">${round}${nameSpan(m.p1, m.winner === 1)}` +
        `<span class="score">${esc(m.p1?.score ?? '')}\u2009\u2013\u2009${esc(m.p2?.score ?? '')}</span>` +
        `${nameSpan(m.p2, m.winner === 2)}</span>`;
    }
    if (m.state === 'in_progress') {
      const score = (m.p1?.score != null && m.p2?.score != null)
        ? `<span class="score">${esc(m.p1.score)}\u2009\u2013\u2009${esc(m.p2.score)}</span>`
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

  // An entry of matches or standings that can be drawn. A null one (or any non-object) is
  // skipped, so it no longer drops the whole update (ZIP-19).
  const isEntry = (x) => x !== null && typeof x === 'object';
  const listOf = (x) => (Array.isArray(x) ? x : []);

  function speedNow() {
    return Number(getComputedStyle(document.documentElement).getPropertyValue('--ticker-speed')) || 80;
  }

  // ── The crawl ──────────────────────────────────────────────────────────────
  // Content is { key, items }: the items' markup, and the markup joined, to compare by.
  let staticText = null; // the static line on the strip, or null while the crawl runs
  let shown = null;      // the content of the first copy, with its markup and width (the loop ends there)
  let entering = null;   // the content the other copies hold, while it differs from shown
  let held = null;       // the newest content, waiting for the wrap or for the fade
  let rebuildAtEnd = false; // copies were added for a wider middle: shown is built again at the loop's end
  let fading = false;
  let fadeTimer = null;

  const copies = () => Array.from(track.children).filter((el) => el.classList.contains('seg'));
  const announcingNow = () => !!zipper && zipper.classList.contains('announcing');
  function scrollAnim() {
    if (typeof track.getAnimations !== 'function') return null;
    return track.getAnimations().find((a) => a.animationName === SCROLL) || null;
  }
  // How long until the loop next wraps, in ms; null when it cannot be told.
  function msToWrap() {
    const a = scrollAnim();
    const t = a && a.effect && typeof a.effect.getComputedTiming === 'function' ? a.effect.getComputedTiming() : null;
    const d = t ? Number(t.duration) : NaN;
    if (!(d > 0)) return null;
    const p = t.progress == null ? 1 : Number(t.progress); // null: at its end, waiting for animationend
    return Math.max(0, (1 - p) * d);
  }

  // A box's width in the strip's own px, the px that --seg-w moves the track by.
  // getBoundingClientRect() would also count a transform on the strip (a theme or OBS
  // Custom CSS that scales .zipper), so the loop would wrap at the wrong place.
  function layoutW(el) {
    const cs = getComputedStyle(el);
    let v = parseFloat(cs.width);
    if (!(v >= 0)) return el.getBoundingClientRect().width;
    if (cs.boxSizing !== 'border-box') {
      v += (parseFloat(cs.paddingLeft) || 0) + (parseFloat(cs.paddingRight) || 0)
        + (parseFloat(cs.borderLeftWidth) || 0) + (parseFloat(cs.borderRightWidth) || 0);
    }
    return v;
  }

  // One segment for these items: repeated until it is as wide as the strip's middle.
  // Measured inside #track itself, after the copies, so the theme's rules apply as they
  // will on screen; nothing is painted in between. The width is only a guide: a theme can
  // style a .seg by its place (.seg + .seg, :last-child), so the loop's end is read from the
  // first copy once it is in place (showNow).
  function measure(items) {
    const unit = items.join('');
    const need = wrap.clientWidth || 1;
    const probe = document.createElement('div');
    probe.className = 'seg';
    track.appendChild(probe);
    let inner = unit;
    probe.innerHTML = inner;
    for (let n = 1; layoutW(probe) < need && n < MAX_REPEATS; n++) {
      inner += unit;
      probe.innerHTML = inner;
    }
    const segW = layoutW(probe) || 1;
    probe.remove();
    return { inner, w: segW };
  }

  // How far the running loop has moved the track, in px (0 up to the loop's end), or 0 when
  // no loop runs (the static line, or a theme that takes the animation off .track).
  function loopOffset() {
    const a = scrollAnim();
    const t = a && a.effect && typeof a.effect.getComputedTiming === 'function' ? a.effect.getComputedTiming() : null;
    const p = t && t.progress != null ? Number(t.progress) : NaN;
    const w = shown ? shown.w : 0; // the loop's end, as restartLoop set it
    return p > 0 && w > 0 ? p * w : 0;
  }

  // Starts a new loop, ending at segW px, in this frame: a CSS animation keeps its elapsed
  // time when its duration changes, so only a new animation takes the new end and duration.
  // It starts at offset 0, or `from` px on, modulo segW: the swap under the fade carries the
  // crawl on from where it was (the owner's ruling of 2026-11-26). It starts at 0 when there
  // is no new loop to move on (a theme that takes the animation off .track).
  function restartLoop(segW, from = 0) {
    track.style.setProperty('--seg-w', `${segW}px`);
    track.style.setProperty('--dur', `${(segW / speedNow()).toFixed(3)}s`);
    track.style.removeProperty('animation-iteration-count');
    track.style.removeProperty('animation-fill-mode');
    track.style.animationName = 'none';
    void track.offsetWidth; // a style flush: the animation is cancelled here...
    track.style.removeProperty('animation-name'); // ...and this one is new, at offset 0
    if (!(from > 0 && segW > 0)) return;
    const a = scrollAnim(); // the new one: reading the animations flushes the style again
    const d = a && a.effect ? Number(a.effect.getComputedTiming().duration) : NaN;
    if (d > 0) a.currentTime = ((from % segW) / segW) * d; // ...moved on to the old place
  }

  // The loop ends at the end of this iteration and holds there (fill forwards) until the
  // animationend handler starts the next one. With the loop left to repeat by itself, the
  // compositor could draw the first copy at offset 0 for a frame before that handler has
  // put the new content in it; held at the end, the copy in view is the new one either way.
  function endAfterThisLoop() {
    const a = scrollAnim();
    const k = a && a.effect ? Number(a.effect.getComputedTiming().currentIteration) || 0 : 0;
    track.style.animationIterationCount = String(k + 1);
    track.style.animationFillMode = 'forwards';
  }

  // Swaps c in at once: every copy holds it and a new loop starts, at offset 0, or `from` px
  // on (restartLoop).
  function showNow(c, from = 0) {
    clearFade();
    staticText = null;
    track.classList.remove('static'); // before measuring: a theme may style the static line apart
    const m = measure(c.items);
    track.innerHTML = `<div class="seg">${m.inner}</div><div class="seg">${m.inner}</div>`;
    const segW = layoutW(track.firstElementChild) || m.w; // the loop ends where the first copy does
    shown = { key: c.key, items: c.items, inner: m.inner, w: segW };
    entering = null;
    held = null;
    rebuildAtEnd = false;
    restartLoop(segW, from);
    watchFirstCopy();
    armFonts();
  }

  function setStatic(text) {
    clearFade();
    held = null;
    entering = null;
    shown = null;
    rebuildAtEnd = false;
    if (staticText === text) return; // unchanged: nothing to do
    staticText = text;
    track.classList.add('static');
    for (const p of ['--dur', '--seg-w', 'animation-iteration-count', 'animation-fill-mode', 'animation-name']) track.style.removeProperty(p);
    track.innerHTML = `<span class="item">${esc(text)}</span>`;
    watchFirstCopy();
  }

  function clearFade() {
    if (fadeTimer) { clearTimeout(fadeTimer); fadeTimer = null; }
    fading = false;
    track.classList.remove('swap-fade');
  }

  // How long #track's opacity transition takes, as the theme has it now, in ms (0 with none).
  function fadeOutMs() {
    try {
      const cs = getComputedStyle(track);
      const ms = (s) => { const v = parseFloat(s); return !(v >= 0) ? 0 : /ms\s*$/.test(s) ? v : v * 1000; };
      const props = cs.transitionProperty.split(',').map((s) => s.trim());
      const durs = cs.transitionDuration.split(',');
      const dels = cs.transitionDelay.split(',');
      let out = 0;
      props.forEach((p, i) => {
        if (p === 'opacity' || p === 'all') out = Math.max(out, ms(durs[i % durs.length]) + ms(dels[i % dels.length]));
      });
      return out;
    } catch { return FADE_MS; }
  }

  // Fades #track out, swaps in the newest content (measured again), and fades it back in.
  // The crawl carries on from where it was (the owner's ruling of 2026-11-26): the new loop
  // starts at the offset the old one has at the swap, modulo the new copy's width. So updates
  // that come more often than a loop takes still let the end of a long list scroll into view.
  function fadeTo() {
    if (fading) return; // the swap at the end of this fade takes the newest content
    fading = true;
    track.classList.add('swap-fade');
    // The swap waits until the track has faded out, however long the theme's transition is.
    fadeTimer = setTimeout(() => {
      fadeTimer = null;
      const c = held || entering || shown;
      if (c) showNow(c, loopOffset()); // the offset read now, with the track out of sight
      else clearFade();
    }, fadeOutMs() + FADE_START_SLACK_MS);
  }

  // Whether every copy but the first is off screen (beyond the strip's right edge, but for
  // EDGE_PX), so it can take new content with nothing in view changing.
  function othersOffScreen() {
    const cs = copies();
    if (cs.length < 2) return false;
    return cs[1].getBoundingClientRect().left >= wrap.getBoundingClientRect().right - EDGE_PX;
  }

  // At a wrap: the copies off screen take c, and the loop is set to end at this
  // iteration's end (still at the first copy's width). When the copy after the first is in
  // view there (the middle grew and nothing has measured it again yet), c comes in under
  // the fade instead, so no copy in view changes.
  function enter(c) {
    if (!othersOffScreen()) { held = c; fadeTo(); return; }
    const m = measure(c.items);
    for (const s of copies().slice(1)) s.innerHTML = m.inner;
    entering = { key: c.key, items: c.items, inner: m.inner, w: m.w };
    held = null;
    endAfterThisLoop();
  }

  // New crawl content arrived.
  function take(c) {
    if (staticText !== null || !shown) { showNow(c); return; } // the first paint, or from the static line
    // Unchanged (decision 14): equal to what the strip shows, or will show once the copies
    // off screen are in. Nothing to do, and content held before it is no longer wanted.
    if (c.key === (entering || shown).key) { held = null; return; }
    held = c;                                            // one held content: the newest replaces it
    if (fading) return;                                  // the fade's swap takes it
    if (announcingNow() || !scrollAnim()) { showNow(c); return; } // hidden, or no loop to wait for
    const toWrap = msToWrap();
    if (toWrap === null || toWrap > FADE_THRESHOLD_MS) fadeTo();
    // Otherwise it waits: for the wrap (onIteration), or, when the copies off screen
    // already hold an older one or the loop is to be built again, for the end of this
    // loop (onEnd).
  }

  function ownScroll(e) {
    return e.target === track && e.animationName === SCROLL;
  }
  track.addEventListener('animationiteration', (e) => {
    if (!ownScroll(e) || fading || entering || !held) return;
    enter(held);
  });
  track.addEventListener('animationend', (e) => {
    if (!ownScroll(e) || fading || !(entering || rebuildAtEnd)) return;
    const c = entering || shown;
    const next = held;
    // The copy in view already holds c: every copy now does, from offset 0, measured for
    // the strip's middle as it is now.
    showNow(c);
    if (next && next.key !== c.key) enter(next); // this is a wrap too
  });

  // ── Re-measuring (ZIP-19) ──────────────────────────────────────────────────
  let remeasureQueued = false;
  function remeasureSoon() {
    // Out of the ResizeObserver's callback, so the changes below cannot start a loop of it.
    if (remeasureQueued) return;
    remeasureQueued = true;
    setTimeout(() => { remeasureQueued = false; remeasure(); }, 0);
  }
  function remeasure() {
    if (staticText !== null || !shown || fading) return;
    const cs = copies();
    if (!cs.length) return;
    const first = layoutW(cs[0]);
    if (Math.abs(first - shown.w) > 0.5) {
      // The copies changed width (a web font arrived): the loop no longer ends where the
      // next copy starts. Held to the wrap it would jump there, so the newest content is
      // measured again and swapped in now, under the fade, at the crawl's place (at once
      // while hidden, at the crawl's place too: under the bar nothing is held or entering,
      // so this is the content the strip already shows).
      if (announcingNow()) showNow(held || entering || shown, loopOffset());
      else fadeTo();
      return;
    }
    // At the loop's end the strip's middle shows the copies after the first one. When it
    // has grown past them (a logo that failed, a shorter cap), one more copy goes at the
    // end, off screen, so the view never runs past the last copy. Nothing in view changes.
    // The copy after the first is then in view at the wrap, where it could not take new
    // content, so at this loop's end every copy is built again for the wider middle (in
    // view that is the same items in the same place), and the loop starts at offset 0.
    const need = wrap.clientWidth || 1;
    let rest = 0;
    let added = false;
    for (const s of cs.slice(1)) rest += layoutW(s);
    while (rest < need && track.children.length < MAX_COPIES) {
      const extra = track.lastElementChild.cloneNode(true);
      track.appendChild(extra);
      rest += layoutW(extra) || need;
      added = true;
    }
    if (added && !entering && !rebuildAtEnd) { rebuildAtEnd = true; endAfterThisLoop(); }
  }

  let watched = null;
  const resizeObs = typeof ResizeObserver === 'function' ? new ResizeObserver(remeasureSoon) : null;
  if (resizeObs) resizeObs.observe(wrap);
  // The first copy is watched too: its width changes when a font arrives, local or not.
  function watchFirstCopy() {
    if (!resizeObs) return;
    if (watched) resizeObs.unobserve(watched);
    watched = copies()[0] || null;
    if (watched) resizeObs.observe(watched);
  }
  const fonts = document.fonts;
  if (fonts && typeof fonts.addEventListener === 'function') fonts.addEventListener('loadingdone', remeasureSoon);
  // A font requested by the content just built (the 600 and 800 weights first appear with
  // the first results) settles fonts.ready again.
  function armFonts() {
    try { if (fonts && fonts.ready) fonts.ready.then(remeasureSoon, () => {}); } catch { /* no FontFaceSet */ }
  }

  // ── End caps ───────────────────────────────────────────────────────────────
  // One logo img, kept: its src is set only when the logo changes (ZIP-19), so an update
  // neither collapses the cap for a frame nor asks for the logo again. A logo that does not
  // load hides the left cap (a sidecar can hand on an address with a key's label in it, such
  // as [Challonge key]). It stays hidden until the logo changes, or until the same logo,
  // asked for again by an update LOGO_RETRY_MS or more after the failure, loads: a short
  // outage of the logo's host does not hide it for the rest of the stream.
  const logoImg = document.createElement('img');
  logoImg.alt = '';
  let logoWanted = '';   // the trimmed logo the payload gave
  let logoFailed = false;
  let logoFailedAt = 0;  // performance.now() at the last failure, or at the last retry
  logoImg.addEventListener('load', () => { logoFailed = false; capLeft.classList.toggle('on', !!logoWanted); });
  logoImg.addEventListener('error', () => {
    if (!logoWanted) return;
    logoFailed = true;
    logoFailedAt = performance.now();
    capLeft.classList.remove('on');
  });
  // The right cap's text sits in a span of its own, which ends in an ellipsis when the cap
  // reaches its maximum width (--ticker-cap-max in ticker.css).
  const capText = document.createElement('span');
  capText.className = 'cap-text';
  capRight.textContent = '';
  capRight.appendChild(capText);

  function applyCaps(caps) {
    const logo = caps && typeof caps.logo === 'string' ? caps.logo.trim() : '';
    const text = caps && typeof caps.text === 'string' ? caps.text.trim() : '';
    if (logo !== logoWanted || (logo && logoImg.getAttribute('src') !== logo)) {
      logoWanted = logo;
      logoFailed = false;
      if (logo) {
        logoImg.setAttribute('src', logo);
        if (logoImg.parentNode !== capLeft) capLeft.appendChild(logoImg);
      } else {
        logoImg.removeAttribute('src');
        logoImg.remove();
      }
    } else if (logo && logoFailed && performance.now() - logoFailedAt >= LOGO_RETRY_MS) {
      logoFailedAt = performance.now(); // one request per wait, however many updates come
      logoImg.setAttribute('src', logo); // the same address again: the browser asks for it again
    }
    capLeft.classList.toggle('on', !!logo && !logoFailed);
    if (capText.textContent !== text) capText.textContent = text;
    capRight.classList.toggle('on', !!text);
  }

  function apply(payload) {
    applyCaps(payload.caps);
    // Persistent message mode takes the strip over: while the payload carries a
    // message block, its lines crawl instead of the results underneath, which
    // keep updating, so toggling it off brings them back (as any new crawl comes in).
    const lines = Array.isArray(payload.message?.lines)
      ? payload.message.lines.map((l) => String(l ?? '').trim()).filter(Boolean)
      : [];
    const items = lines.length ? lines.map(messageHtml) : [];
    if (!items.length) for (const m of listOf(payload.matches)) if (isEntry(m)) items.push(itemHtml(m));
    if (!items.length) for (const s of listOf(payload.standings)) if (isEntry(s)) items.push(standingHtml(s));
    if (items.length) take({ key: items.join(''), items });
    else setStatic(payload.tournament?.name ? `${payload.tournament.name} — waiting for results…` : 'waiting for results…');
  }

  window.addEventListener('ticker:update', (e) => {
    try { apply(e.detail || {}); } catch (err) { console.error('[zipper] render failed:', err); }
  });

  // ── Announcement takeover: pause the crawl, show the announcement, resume.
  // Queued so back-to-back subs/raids play one after another.
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
    // The announcement hides the track: content waiting for the wrap or the fade goes in now,
    // and its loop starts at offset 0. A re-measure's fade (a late web font) with nothing
    // new is cut short too, and the same content goes on from the crawl's place. With nothing
    // waiting and no fade, the crawl pauses where it is.
    if (held || entering || fading) showNow(held || entering || shown, held || entering ? 0 : loopOffset());
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
