// Challonge — two fetch paths (URL matching + scrape plumbing shared with
// Tally's roster importer; this module reads matches/results instead of just
// participants):
//   • API (preferred when a `challongeApiKey` is configured):
//       GET https://api.challonge.com/v1/tournaments/<id>.json
//           ?include_participants=1&include_matches=1
//     One call returns the tournament (name, tournament_type, state), the
//     participant list (id → display name), and every match with `state`
//     ('pending'|'open'|'complete'), `round` (negative = losers side),
//     `scores_csv` ("3-1"), `winner_id`, and `suggested_play_order`.
//   • Scrape (fallback without an API key):
//       GET https://[<subdomain>.]challonge.com/<slug>/module
//     The public bracket-embed page hydrates a Redux store
//     (`window._initialStoreState['TournamentStore']`) whose `matches_by_round`
//     carries each match's player1/player2. Score/winner fields on that store
//     vary by bracket age — extraction is defensive (scores_csv, per-side
//     *_score, or per-player score) and matches without readable scores are
//     surfaced as in-progress/upcoming rather than dropped.

const API_BASE = 'https://api.challonge.com/v1';
const TOURNAMENT_STORE_RE = /window\._initialStoreState\['TournamentStore'\]\s*=\s*/;

export const challonge = {
  name: 'challonge',
  displayName: 'Challonge',

  matches(input) {
    if (!input) return false;
    try {
      const u = new URL(input);
      return /(^|\.)challonge\.com$/i.test(u.hostname);
    } catch {
      const s = String(input).trim();
      return s.length > 0 && /^[a-z0-9][a-z0-9_-]*$/i.test(s) && !/^\d+$/.test(s);
    }
  },

  parseId(input) {
    const raw = String(input || '').trim();
    if (!raw) return null;
    let slug = raw;
    try {
      const url = new URL(raw);
      if (!/(^|\.)challonge\.com$/i.test(url.hostname)) return null;
      const subdomain = url.hostname.replace(/\.?challonge\.com$/i, '').replace(/\.$/, '');
      slug = url.pathname.replace(/^\/+/, '').split(/[\/?#]/)[0];
      if (!slug) return null;
      if (subdomain && subdomain !== 'www') return `${subdomain}-${slug}`;
      return slug;
    } catch {
      return slug.replace(/^\/+/, '').split(/[\/?#]/)[0] || null;
    }
  },

  async fetchTicker(tournamentId, config, originalUrl) {
    if (config?.challongeApiKey) {
      try {
        return await fetchViaApi(tournamentId, config.challongeApiKey);
      } catch (err) {
        // The v1 API 404s any tournament the key's account doesn't own —
        // public brackets run by someone else included. Fall back to the
        // public scrape for those; other API errors (auth, rate limit) rethrow.
        if (!/\b404\b/.test(err?.message || '')) throw err;
      }
    }
    return fetchViaScrape(originalUrl, tournamentId);
  },
};

// ── API path ──────────────────────────────────────────────────────────────────

async function fetchViaApi(tournamentId, apiKey) {
  if (!tournamentId) throw new Error('Missing tournament id');
  const url = `${API_BASE}/tournaments/${encodeURIComponent(tournamentId)}.json`
    + `?include_participants=1&include_matches=1&api_key=${encodeURIComponent(apiKey)}`;
  const res = await fetch(url, { headers: { Accept: 'application/json' } });
  if (!res.ok) {
    let detail = '';
    try { detail = await res.text(); } catch {}
    throw new Error(`Challonge API ${res.status}: ${detail.slice(0, 200) || res.statusText}`);
  }
  const body = await res.json();
  return normalizeChallongeApi(body);
}

// Pure normalizer (fixture-tested by verify.mjs). `body` is the raw
// /tournaments/<id>.json response: { tournament: { ..., participants, matches } }.
export function normalizeChallongeApi(body) {
  const t = body?.tournament;
  if (!t) throw new Error('Unexpected Challonge response shape (no tournament)');

  const names = new Map();
  const standings = [];
  for (const entry of Array.isArray(t.participants) ? t.participants : []) {
    const p = entry?.participant;
    if (!p?.id) continue;
    const name = String(p.display_name || p.name || p.username || '').trim();
    names.set(Number(p.id), name || `#${p.id}`);
    // Group-stage matches reference group_player_ids, not the participant id.
    for (const gid of Array.isArray(p.group_player_ids) ? p.group_player_ids : []) {
      names.set(Number(gid), name || `#${p.id}`);
    }
    // final_rank is set per participant once the tournament completes (and for
    // already-eliminated players while underway) — it feeds the topN filter
    // and the standings crawl. Requires the keyed API path; the keyless embed
    // store has no rank data.
    const rank = Number(p.final_rank);
    if (Number.isFinite(rank) && rank > 0 && name) {
      standings.push({ placement: rank, name });
    }
  }
  standings.sort((a, b) => a.placement - b.placement);
  const nameOf = (id) => (id == null ? 'TBD' : (names.get(Number(id)) || 'TBD'));

  const rawMatches = (Array.isArray(t.matches) ? t.matches : [])
    .map((e) => e?.match).filter(Boolean);
  const isDouble = /double/i.test(String(t.tournament_type || ''));
  const rounds = rawMatches.map((m) => Number(m.round) || 0);
  const maxWin = Math.max(0, ...rounds);
  const minLose = Math.min(0, ...rounds);

  const matches = rawMatches.map((m, i) => {
    const state = m.state === 'complete' ? 'completed' : m.state === 'open' ? 'in_progress' : 'upcoming';
    const [s1, s2] = parseScoresCsv(m.scores_csv);
    const p1id = m.player1_id;
    const p2id = m.player2_id;
    let winner = 0;
    if (state === 'completed' && m.winner_id != null) {
      if (Number(m.winner_id) === Number(p1id)) winner = 1;
      else if (Number(m.winner_id) === Number(p2id)) winner = 2;
    }
    return {
      id: String(m.id),
      round: roundLabel(Number(m.round) || 0, maxWin, minLose, isDouble),
      state,
      p1: { name: nameOf(p1id), score: state === 'upcoming' ? null : s1 },
      p2: { name: nameOf(p2id), score: state === 'upcoming' ? null : s2 },
      winner,
      order: Number(m.suggested_play_order) || i + 1,
    };
  });

  return {
    tournamentName: String(t.name || '').trim(),
    state: t.state === 'complete' ? 'completed' : t.state === 'pending' ? 'upcoming' : 'in_progress',
    matches,
    standings,
  };
}

// "3-1" → [3, 1]; multi-set "1-0,0-1,1-0" → set counts summed per side wins?
// Challonge stores game scores per set comma-separated; for the ticker we show
// the LAST reported pair when single, or set-wins when multiple sets exist.
function parseScoresCsv(csv) {
  const s = String(csv || '').trim();
  if (!s) return [null, null];
  const sets = s.split(',').map((pair) => {
    const m = pair.trim().match(/^(-?\d+)-(-?\d+)$/);
    return m ? [Number(m[1]), Number(m[2])] : null;
  }).filter(Boolean);
  if (!sets.length) return [null, null];
  if (sets.length === 1) return sets[0];
  let w1 = 0, w2 = 0;
  for (const [a, b] of sets) { if (a > b) w1++; else if (b > a) w2++; }
  return [w1, w2];
}

function roundLabel(round, maxWin, minLose, isDouble) {
  if (round > 0) {
    if (isDouble) {
      if (round === maxWin) return 'Grand Final';
      if (round === maxWin - 1) return 'Winners Final';
      if (round === maxWin - 2) return 'Winners Semis';
      return `Winners Round ${round}`;
    }
    if (round === maxWin) return 'Final';
    if (round === maxWin - 1) return 'Semis';
    return `Round ${round}`;
  }
  if (round < 0) {
    if (round === minLose) return 'Losers Final';
    if (round === minLose + 1) return 'Losers Semis';
    return `Losers Round ${-round}`;
  }
  return '';
}

// ── Scrape path (no API key) ──────────────────────────────────────────────────

async function fetchViaScrape(originalUrl, tournamentId) {
  const moduleUrl = deriveModuleUrl(originalUrl, tournamentId);
  if (!moduleUrl) throw new Error('Could not derive Challonge embed URL');

  const res = await fetch(moduleUrl, {
    headers: {
      Accept: 'text/html',
      'User-Agent': 'Mozilla/5.0 (compatible; Zipper/0.1)',
    },
  });
  if (!res.ok) {
    throw new Error(`Challonge ${res.status}: ${res.statusText} (no api key configured; fell back to public scrape)`);
  }
  const html = await res.text();
  const store = extractTournamentStore(html);
  if (!store) {
    throw new Error('Challonge: could not extract TournamentStore from embed page (set "challongeApiKey" in config.json for the authoritative API path)');
  }
  // The store itself carries no tournament name — pull it from the page title
  // ("Solution to Sunday 7: … - Challonge").
  const title = (html.match(/<title>([^<]*)<\/title>/i)?.[1] ?? '')
    .replace(/\s*-\s*Challonge\s*$/i, '')
    .replace(/\s+/g, ' ')
    .trim();
  return normalizeChallongeStore(store, title);
}

// Pure normalizer (fixture-tested). `store` is the embed page's TournamentStore;
// `pageTitle` is the fallback tournament name scraped from the page <title>.
export function normalizeChallongeStore(store, pageTitle = '') {
  const byRound = store?.matches_by_round || {};
  const isDouble = Object.keys(byRound).some((r) => Number(r) < 0)
    || /double/i.test(String(store?.tournament?.tournament_type || ''));
  const roundNums = Object.keys(byRound).map(Number).filter(Number.isFinite);
  const maxWin = Math.max(0, ...roundNums);
  const minLose = Math.min(0, ...roundNums);

  // The embed store rarely carries suggested_play_order, so synthesize a
  // chronological rank from bracket depth: winners round N ≈ before losers
  // round N (which drains winners-N losers), and the double-elim Grand Final
  // is always last. The sidecar sorts completed matches newest-first by this.
  const depth = (r) => {
    if (isDouble && r === maxWin) return 1000; // Grand Final (+ reset)
    return r > 0 ? 2 * r : 2 * -r + 1;
  };

  const matches = [];
  const depthById = new Map(); // match id → bracket depth, for deriveStandings
  let order = 0;
  for (const roundKey of roundNums.sort((a, b) => a - b)) {
    const list = byRound[String(roundKey)];
    if (!Array.isArray(list)) continue;
    for (const m of list) {
      order++;
      const p1 = m?.player1;
      const p2 = m?.player2;
      const [s1, s2] = extractStoreScores(m);
      let state = String(m?.state || '').toLowerCase();
      if (state === 'complete') state = 'completed';
      else if (state === 'open') state = 'in_progress';
      else if (state === 'pending') state = 'upcoming';
      else state = (s1 != null && m?.winner_id != null) ? 'completed' : (p1 && p2 ? 'in_progress' : 'upcoming');
      let winner = 0;
      if (state === 'completed') {
        if (m?.winner_id != null) {
          if (p1?.id != null && Number(m.winner_id) === Number(p1.id)) winner = 1;
          else if (p2?.id != null && Number(m.winner_id) === Number(p2.id)) winner = 2;
        } else if (s1 != null && s2 != null && s1 !== s2) {
          winner = s1 > s2 ? 1 : 2;
        }
      }
      depthById.set(String(m?.id ?? `r${roundKey}-${order}`), depth(roundKey));
      matches.push({
        id: String(m?.id ?? `r${roundKey}-${order}`),
        round: roundLabel(roundKey, maxWin, minLose, isDouble),
        state,
        p1: { name: playerName(p1), score: state === 'upcoming' ? null : s1 },
        p2: { name: playerName(p2), score: state === 'upcoming' ? null : s2 },
        winner,
        order: Number(m?.suggested_play_order) || depth(roundKey) * 100 + order,
      });
    }
  }

  if (!matches.length) {
    throw new Error('Challonge: bracket has no matches yet (pending tournaments without a generated bracket can\'t be scraped — set "challongeApiKey" to use the API instead)');
  }
  const standings = deriveStandings(matches, depthById);
  return {
    tournamentName: String(store?.tournament?.name || store?.name || pageTitle || '').trim(),
    // The store's tournament block has a state field but it's unreliable across
    // bracket ages — infer from the matches instead.
    state: standings.length ? 'completed'
      : matches.every((m) => m.state === 'upcoming') ? 'upcoming'
      : 'in_progress',
    matches,
    standings,
  };
}

// The embed store has no rank data, but a FINISHED bracket implies the
// placements: whoever never lost is champion, and everyone else places by how
// deep in the bracket their final loss happened (Grand Final loser 2nd, Losers
// Final loser 3rd, …). Ties within the same round share a placement, like
// Challonge's own final_rank. Only derived once the bracket is finished — any
// still-open match between two real players means the elimination order isn't
// settled, so this returns [] and the topN filter stays off. (A never-played
// Grand Final reset sits at TBD and doesn't block.)
function deriveStandings(matches, depthById) {
  const real = (n) => n && n !== 'TBD';
  const unfinished = matches.some((m) => m.state !== 'completed' && real(m.p1?.name) && real(m.p2?.name));
  if (unfinished) return [];

  const completed = matches.filter((m) => m.state === 'completed' && m.winner);
  if (!completed.length) return [];
  const deepest = completed.reduce((a, b) =>
    ((depthById.get(b.id) ?? 0) >= (depthById.get(a.id) ?? 0) ? b : a));

  // Per player: the bracket depth of their last loss.
  const lostAt = new Map();
  for (const m of completed) {
    const loser = m.winner === 1 ? m.p2?.name : m.p1?.name;
    if (!real(loser)) continue;
    const d = depthById.get(m.id) ?? 0;
    lostAt.set(loser, Math.max(lostAt.get(loser) ?? 0, d));
  }
  const champion = deepest.winner === 1 ? deepest.p1?.name : deepest.p2?.name;
  if (!real(champion)) return [];
  lostAt.delete(champion); // won the last match — never counts as eliminated

  const ranked = [...lostAt.entries()].sort((a, b) => b[1] - a[1]);
  const standings = [{ placement: 1, name: champion }];
  let placement = 2;
  for (let i = 0; i < ranked.length; i++) {
    if (i > 0 && ranked[i][1] !== ranked[i - 1][1]) placement = standings.length + 1;
    standings.push({ placement, name: ranked[i][0] });
  }
  return standings;
}

function playerName(p) {
  const n = String(p?.display_name || p?.name || '').trim();
  return n || 'TBD';
}

// The embed store's score fields vary; try the known shapes in order. Current
// pages (verified 2026-07 against a live bracket) carry `scores: [3, 0]`;
// older/csv spellings are kept as fallbacks.
function extractStoreScores(m) {
  if (Array.isArray(m?.scores) && m.scores.length >= 2) {
    return [numOrNull(m.scores[0]), numOrNull(m.scores[1])];
  }
  if (m?.scores_csv) return parseScoresCsv(m.scores_csv);
  if (typeof m?.scores === 'string') return parseScoresCsv(m.scores);
  if (Number.isFinite(m?.player1_score) || Number.isFinite(m?.player2_score)) {
    return [numOrNull(m.player1_score), numOrNull(m.player2_score)];
  }
  if (m?.player1 && (m.player1.score != null || m.player2?.score != null)) {
    return [numOrNull(m.player1.score), numOrNull(m.player2?.score)];
  }
  return [null, null];
}

function numOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function deriveModuleUrl(originalUrl, tournamentId) {
  // Prefer the original URL the operator pasted — it carries the right host.
  if (originalUrl) {
    try {
      const u = new URL(originalUrl);
      if (/(^|\.)challonge\.com$/i.test(u.hostname)) {
        const slug = u.pathname.replace(/^\/+/, '').split(/[\/?#]/)[0];
        if (slug) return `${u.origin}/${slug}/module`;
      }
    } catch {
      // Bare slug — fall through to challonge.com root.
    }
  }
  if (tournamentId && /^[a-z0-9_-]+$/i.test(tournamentId)) {
    return `https://challonge.com/${tournamentId}/module`;
  }
  return null;
}

function extractTournamentStore(html) {
  const re = /<script[^>]*>([\s\S]*?)<\/script>/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const body = m[1];
    if (!TOURNAMENT_STORE_RE.test(body)) continue;
    const idx = body.search(TOURNAMENT_STORE_RE);
    if (idx < 0) continue;
    const after = body.slice(idx).replace(TOURNAMENT_STORE_RE, '');
    const json = consumeJsonObject(after);
    if (!json) continue;
    try { return JSON.parse(json); } catch { /* try next match */ }
  }
  return null;
}

// Walk balanced braces, respecting strings and escapes, to find the end of
// the `{...}` JSON object that begins at position 0.
function consumeJsonObject(s) {
  if (s[0] !== '{') return null;
  let depth = 0, inStr = false, esc = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (inStr) {
      if (esc) { esc = false; continue; }
      if (c === '\\') { esc = true; continue; }
      if (c === '"') inStr = false;
      continue;
    }
    if (c === '"') { inStr = true; continue; }
    if (c === '{') depth++;
    else if (c === '}') {
      depth--;
      if (depth === 0) return s.slice(0, i + 1);
    }
  }
  return null;
}
