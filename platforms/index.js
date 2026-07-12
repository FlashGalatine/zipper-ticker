// Ticker platform registry — same shape as Tally's roster registry, but each
// module fetches tournament PROGRESS, not just participants:
//
//   name             — short id ("challonge", "startgg")
//   displayName      — human label
//   matches(input)   — returns true if this platform should handle the input
//   parseId(input)   — returns a tournament identifier string, or null
//   fetchTicker(id, config, originalUrl)
//                    — returns Promise<{ tournamentName, state, matches[], standings[] }>
//                      where matches[] entries are the normalized shape in
//                      docs/PROTOCOL.md: { id, round, state, p1:{name,score},
//                      p2:{name,score}, winner, order }.
//
// Order matters for `detectPlatform` — first match wins. Put URL-host-specific
// platforms before any platform that accepts loose bare-string input
// (Challonge accepts bare slugs, so it goes last).
//
// Adding a new service: drop a module into this folder, import it here, add it
// to PLATFORMS. No other code changes required.

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
