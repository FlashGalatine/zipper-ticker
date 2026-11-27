// Ticker platform registry — same shape as Tally's roster registry, but each
// module fetches tournament PROGRESS, not just participants:
//
//   name             — short id ("challonge", "startgg")
//   displayName      — human label
//   matches(input)   — returns true if this platform should handle the input
//   parseId(input)   — returns a tournament identifier string, or null
//   fetchTicker(id, config, originalUrl, signal)
//                    — returns Promise<{ tournamentName, state, matches[], standings[] }>
//                      where matches[] entries are the normalized shape in
//                      docs/PROTOCOL.md: { id, round, state, p1:{name,score},
//                      p2:{name,score}, winner, order }.
//                      `config` holds the keys in use and the maxItems and topN the
//                      poll is made with.
//                      The result may also carry `cutShort: true` when the adapter
//                      stopped reading early (enough matches for maxItems): an option
//                      change that asks for more (a higher maxItems, or topN switched
//                      on) is rebuilt from what it holds and pushed at once, and the
//                      sidecar then fetches again. start.gg sets it (its paging).
//                      And `standingsAsked`: the number of standings the adapter asked
//                      the site for. A topN raised above it is rebuilt and pushed at
//                      once, and the sidecar then fetches again, cut short or not (the
//                      owner's ruling of 2026-11-27). start.gg sets it (max(topN, 8),
//                      at most 100); a result without it is fetched again only for
//                      cutShort.
//                      And `pollNotice`: a non-fatal fact about this good poll, in fixed
//                      words, which the status carries until the next good poll or a
//                      setUrl that changes the URL (start.gg: an event cut at its page
//                      limit). Never a site's own text: it reaches every page of the bus.
//                      `signal` is the poll's AbortSignal (its 45-second deadline,
//                      or a setUrl that replaced the URL). Pass it to every request,
//                      and make every request through guardedFetch
//                      (./guarded-fetch.js), which also gives it a 10-second timeout,
//                      a size cap and Retry-After; throw a refused answer with its
//                      statusError, so the sidecar sees the status and Retry-After.
//
// Order matters for `detectPlatform` — first match wins. Put URL-host-specific
// platforms before any platform that accepts loose bare-string input
// (Challonge accepts bare slugs, so it goes last).
//
// Adding a new service: drop a module into this folder, import it here, add it
// to PLATFORMS. No other code changes required. (guarded-fetch.js in this folder is
// the request wrapper the platforms share, not a platform.)

import { tourneybot } from './tourneybot.js';
import { matcherino } from './matcherino.js';
import { startgg } from './startgg.js';
import { roundone } from './roundone.js';
import { challonge } from './challonge.js';

export const PLATFORMS = [tourneybot, matcherino, startgg, roundone, challonge];

export function detectPlatform(input) {
  for (const p of PLATFORMS) {
    if (p.matches(input)) return p;
  }
  return null;
}

// Input pasted without a scheme ("start.gg/tournament/x/event/y", "challonge.com/abc")
// is given https:// before platform detection (decision 11 d), as Tally's normalizeInput
// does (roster-lib.mjs). The input is trimmed. Anything with a scheme (https:, stub:) is
// left as it is, and so is a bare word with no dot (Challonge accepts a bare slug). The
// sidecar's setUrl stores the result; zipper-shared/control.html holds a copy of this
// body, so its URL box settles on the address the sidecar stores (verify.mjs checks the
// two copies match).
// Tally tests the address with /^[^\s\/]+\.[^\s\/]+(\/|$)/, which backtracks in
// quadratic time on a long run of dots (40,000 of them take seconds, and the setUrl value
// has no length cap). This gives the same answer in linear time: the text up to the first
// space or slash ends the string or is followed by a slash, and holds a dot that is
// neither its first nor its last character (verify.mjs compares the two on every short
// string, and times a long one).
export function normalizeInput(input) {
  const s = String(input || '').trim();
  if (!s || /^[a-z][a-z0-9+.-]*:/i.test(s)) return s;
  const head = /^[^\s\/]*/.exec(s)[0];
  const next = s.charAt(head.length);
  return (next === '' || next === '/') && head.slice(1, -1).includes('.') ? 'https://' + s : s;
}

export function listSupportedPlatforms() {
  return PLATFORMS.map((p) => ({ name: p.name, displayName: p.displayName }));
}
