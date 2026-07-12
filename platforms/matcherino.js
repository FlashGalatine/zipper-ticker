// Matcherino — standings-only. The (undocumented) public JSON endpoint
//   GET https://api.matcherino.com/__api/bounties/findById?id=<id>
// returns `body.individualParticipants[]` with `{ userId, displayName,
// placement, ... }` but no per-match data, so the ticker falls back to the
// standings crawl ("1st Flash · 2nd Rival · …"). Participants without a
// placement yet are omitted; if nobody is placed the fetch throws so the
// sidecar surfaces a useful status instead of an empty strip.

const API_BASE = 'https://api.matcherino.com/__api';

export const matcherino = {
  name: 'matcherino',
  displayName: 'Matcherino',

  matches(input) {
    if (!input) return false;
    try {
      const u = new URL(input);
      return /(^|\.)matcherino\.com$/i.test(u.hostname);
    } catch {
      return false;
    }
  },

  parseId(input) {
    const raw = String(input || '').trim();
    if (!raw) return null;
    try {
      const url = new URL(raw);
      if (!/(^|\.)matcherino\.com$/i.test(url.hostname)) return null;
      const m = url.pathname.match(/\/tournaments\/(\d+)/);
      return m ? m[1] : null;
    } catch {
      return null;
    }
  },

  async fetchTicker(tournamentId, _config) {
    if (!/^\d+$/.test(String(tournamentId))) {
      throw new Error('Missing or invalid Matcherino tournament id');
    }
    const res = await fetch(`${API_BASE}/bounties/findById?id=${encodeURIComponent(tournamentId)}`, {
      headers: { Accept: 'application/json', 'User-Agent': 'Zipper/0.1' },
    });
    if (!res.ok) throw new Error(`Matcherino ${res.status}: ${res.statusText}`);

    let env;
    try { env = await res.json(); } catch (err) {
      throw new Error(`Matcherino: failed to parse response (${err.message})`);
    }
    return normalizeMatcherino(env);
  },
};

// Pure normalizer (fixture-tested by verify.mjs).
export function normalizeMatcherino(env) {
  const body = env?.body;
  if (!body) throw new Error('Matcherino: empty response body (tournament may not exist)');

  const participants = Array.isArray(body.individualParticipants) ? body.individualParticipants : [];
  const standings = participants
    .filter((p) => Number.isFinite(Number(p?.placement)) && Number(p.placement) > 0 && p?.displayName)
    .map((p) => ({ placement: Number(p.placement), name: String(p.displayName).trim() }))
    .sort((a, b) => a.placement - b.placement);

  if (!standings.length) {
    throw new Error('Matcherino: no placements posted yet (Matcherino exposes standings only — results appear once the organiser records placements)');
  }
  return {
    tournamentName: String(body.title || body.name || '').trim(),
    state: 'in_progress',
    matches: [],
    standings,
  };
}
