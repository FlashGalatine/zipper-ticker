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
//                      It may also carry `cutShort: true` when the adapter stopped
//                      reading early (enough matches for maxItems): an option change
//                      that asks for more (a higher maxItems, or topN switched on)
//                      is rebuilt from what it holds and pushed at once, and the
//                      sidecar then fetches again. No adapter sets it yet.
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

export function listSupportedPlatforms() {
  return PLATFORMS.map((p) => ({ name: p.name, displayName: p.displayName }));
}
