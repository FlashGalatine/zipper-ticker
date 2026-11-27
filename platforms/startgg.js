// start.gg — two fetch paths (URL matching + gql plumbing shared with Tally's
// roster importer; this module queries SETS instead of entrants):
//   • Official Developer API (preferred when `startggApiKey` is configured):
//       POST https://api.start.gg/gql/alpha   with Authorization: Bearer <token>
//   • Public proxy (fallback when no key):
//       POST https://www.start.gg/api/-/gql
//     The undocumented Next.js backend the start.gg website itself drives.
//     No token, but enforces a `client-version` header — when start.gg bumps
//     it, the server replies 400 with the required version and we retry once.
//     NOTE: the proxy may reject non-allowlisted queries; if the sets query
//     400s there, configure `startggApiKey` (docs/README.md).
//
// Set `state` mapping (start.gg ActivityState): 3 = completed, 2 = in progress
// (called/started), everything else = upcoming.
// Both URL shapes are accepted:
//   /tournament/<t>/event/<e>       (event landing)
//   /tournament/<t>/events/<e>/...  (any sub-page incl. brackets/standings)
// and either one behind the "/admin" prefix of the tournament's admin pages.
// A tournament URL (no event in it) is polled only when the tournament has one event;
// with several it stops with "paste a specific event URL" (decision 12).
//
// Paging (ZIP-20): sets come newest first, 50 a page, de-duplicated by id (a set that
// completes between two requests moves the list down by one). With topN off, paging
// stops once the sets read hold maxItems completed ones, which is all the strip can show
// (completed sets come first, cut to maxItems), and the result is marked `cutShort`, so
// the sidecar fetches again when a command asks for more. With topN on, every page is
// read. The standings ride the first page only: max(topN, 8) of them, at most 100, and
// the result records that number as `standingsAsked`, so the sidecar fetches again when
// topN is raised above it, cut short or not (the owner's ruling of 2026-11-27). At most
// 10 pages are read; an event cut there says so in the result's `pollNotice`.
// Only the queries' variables vary: their text is the one that passed the public proxy.

import { guardedFetch, statusError, USER_AGENT } from './guarded-fetch.js';

const PUBLIC_ENDPOINT = 'https://www.start.gg/api/-/gql';
const API_ENDPOINT = 'https://api.start.gg/gql/alpha';
const DEFAULT_CLIENT_VERSION = '20';
const PAGE_SIZE = 50;
const MAX_PAGES = 10; // safety cap (500 sets — plenty for a ticker)
// The standings asked for on the first page: at least the 8 the query asks for by
// default, more when topN needs them, and at most STANDINGS_MAX (a choice left open in
// row 8.3; max(topN, maxItems) would give a fuller standings crawl). Later pages ask for 1.
const STANDINGS_MIN = 8;
const STANDINGS_MAX = 100;
// The result's pollNotice when an event has more pages than MAX_PAGES. Fixed words only:
// nothing from the site or the request is in it.
const PAGE_LIMIT_NOTICE = `start.gg: this event has more than ${MAX_PAGES * PAGE_SIZE} sets; only the ${MAX_PAGES * PAGE_SIZE} most recent were read`;

// The path of a start.gg URL without the "/admin" prefix organisers copy from the
// tournament's admin pages (decision 11 c), as Tally's sgPath does.
function sgPath(url) { return url.pathname.replace(/^\/admin(?=\/)/, ''); }

// `videogame { id }` is no longer read (decision 12 removed the Street Fighter 6 pick);
// the text is kept as it is, because start.gg's public proxy may refuse a query that is
// not on its allow-list, and this one passed it.
const TOURNAMENT_EVENTS_QUERY = `
query ZipperTournamentEvents($slug: String!) {
  tournament(slug: $slug) {
    id
    name
    events { id name slug videogame { id } }
  }
}`;

const SETS_QUERY = `
query ZipperEventSets($slug: String!, $page: Int = 1, $perPage: Int = 50, $standingsPerPage: Int = 8) {
  event(slug: $slug) {
    id
    name
    state
    startAt
    tournament { name }
    standings(query: { perPage: $standingsPerPage, page: 1 }) {
      nodes { placement entrant { id name } }
    }
    sets(page: $page, perPage: $perPage, sortType: RECENT) {
      nodes {
        id
        state
        round
        fullRoundText
        displayScore
        winnerId
        slots {
          entrant { id name }
          standing { stats { score { value } } }
        }
      }
      pageInfo { page total perPage totalPages }
    }
  }
}`;

export const startgg = {
  name: 'startgg',
  displayName: 'start.gg',

  matches(input) {
    if (!input) return false;
    try {
      const u = new URL(input);
      return /(^|\.)start\.gg$/i.test(u.hostname);
    } catch {
      return false;
    }
  },

  parseId(input) {
    const raw = String(input || '').trim();
    if (!raw) return null;
    try {
      const url = new URL(raw);
      if (!/(^|\.)start\.gg$/i.test(url.hostname)) return null;
      const ev = sgPath(url).match(/^\/tournament\/([^\/]+)\/(?:event|events)\/([^\/]+)/);
      if (ev) return `${ev[1]}/${ev[2]}`;
      const t = sgPath(url).match(/^\/tournament\/([^\/]+)/);
      if (t) return t[1];
      return null;
    } catch {
      return null;
    }
  },

  // `signal` is the poll's deadline: it reaches every request below, so a poll that
  // pages through sets stops as a whole rather than paging on in the background.
  // `config.maxItems` and `config.topN` are what the sidecar polls with: they decide
  // when paging may stop, and how many standings the first page asks for.
  async fetchTicker(displayId, config, originalUrl, signal) {
    const apiKey = config?.startggApiKey ?? null;
    let eventSlug = deriveEventSlug(originalUrl, displayId);
    if (!eventSlug) {
      const tournamentSlug = deriveTournamentSlug(originalUrl, displayId);
      if (!tournamentSlug) {
        throw new Error('start.gg: paste a tournament or event URL (e.g. https://www.start.gg/tournament/<t> or .../tournament/<t>/event/<e>)');
      }
      eventSlug = await resolveEventSlug(tournamentSlug, apiKey, signal);
    }
    const { event, cutShort, pollNotice, standingsAsked } = await fetchAllSets(eventSlug, apiKey, signal, { maxItems: config?.maxItems, topN: config?.topN });
    const result = normalizeStartggEvent(event);
    if (cutShort) result.cutShort = true; // the sidecar fetches again when a command asks for more
    if (pollNotice) result.pollNotice = pollNotice;
    // The standings the first page asked for: a topN above it fetches again (row 4.3).
    result.standingsAsked = standingsAsked;
    return result;
  },
};

// Pure normalizer (fixture-tested by verify.mjs). `event` is the merged GraphQL
// event object with a flat sets.nodes array.
export function normalizeStartggEvent(event) {
  const nodes = event?.sets?.nodes;
  if (!Array.isArray(nodes)) throw new Error('start.gg: no sets visible on this event');

  const matches = nodes.map((s, i) => {
    const state = Number(s?.state) === 3 ? 'completed' : Number(s?.state) === 2 ? 'in_progress' : 'upcoming';
    const slots = Array.isArray(s?.slots) ? s.slots : [];
    const side = (slot) => {
      const name = String(slot?.entrant?.name || '').trim() || 'TBD';
      const raw = slot?.standing?.stats?.score?.value;
      const score = (state !== 'upcoming' && Number.isFinite(Number(raw)) && raw != null) ? Number(raw) : null;
      return { name, score, entrantId: slot?.entrant?.id };
    };
    const a = side(slots[0]);
    const b = side(slots[1]);
    let winner = 0;
    if (state === 'completed' && s?.winnerId != null) {
      if (a.entrantId != null && Number(s.winnerId) === Number(a.entrantId)) winner = 1;
      else if (b.entrantId != null && Number(s.winnerId) === Number(b.entrantId)) winner = 2;
    }
    return {
      id: String(s?.id ?? i),
      round: String(s?.fullRoundText || '').trim(),
      state,
      p1: { name: a.name, score: a.score },
      p2: { name: b.name, score: b.score },
      winner,
      // RECENT sort puts newest first; preserve that as descending order.
      order: nodes.length - i,
    };
  });

  // Event standings (current placements while live, final once complete) —
  // feeds the sidecar's topN filter and the standings crawl fallback.
  const standings = (Array.isArray(event?.standings?.nodes) ? event.standings.nodes : [])
    .filter((n) => Number.isFinite(Number(n?.placement)) && n?.entrant?.name)
    .map((n) => ({ placement: Number(n.placement), name: String(n.entrant.name).trim() }))
    .sort((a, b) => a.placement - b.placement);

  const tourneyName = String(event?.tournament?.name || '').trim();
  const eventName = String(event?.name || '').trim();
  // Event state arrives as a NUMBER via the keyed API (ActivityState 1/2/3)
  // but as a STRING enum ("CREATED"/"ACTIVE"/"COMPLETED") via the public proxy.
  const evState = String(event?.state ?? '').toUpperCase();
  const state = (evState === 'COMPLETED' || Number(event?.state) === 3) ? 'completed'
    : (evState === 'CREATED' || Number(event?.state) === 1) ? 'upcoming'
    : 'in_progress';

  // event.startAt is epoch SECONDS (may be null on undated events) — feeds the
  // sidecar's wait-for-start auto-scheduling.
  const startEpoch = Number(event?.startAt);
  const startAt = Number.isFinite(startEpoch) && startEpoch > 0
    ? new Date(startEpoch * 1000).toISOString()
    : null;

  return {
    tournamentName: tourneyName && eventName ? `${tourneyName} — ${eventName}` : (tourneyName || eventName),
    state,
    startAt,
    matches,
    standings,
  };
}

function deriveEventSlug(originalUrl, displayId) {
  if (originalUrl) {
    try {
      const url = new URL(originalUrl);
      if (/(^|\.)start\.gg$/i.test(url.hostname)) {
        const m = sgPath(url).match(/^\/tournament\/([^\/]+)\/(?:event|events)\/([^\/]+)/);
        if (m) return `tournament/${m[1]}/event/${m[2]}`;
      }
    } catch { /* fall through */ }
  }
  if (typeof displayId === 'string') {
    const parts = displayId.split('/');
    if (parts.length >= 2 && parts[0] && parts[1]) {
      return `tournament/${parts[0]}/event/${parts[1]}`;
    }
  }
  return null;
}

function deriveTournamentSlug(originalUrl, displayId) {
  if (originalUrl) {
    try {
      const url = new URL(originalUrl);
      if (/(^|\.)start\.gg$/i.test(url.hostname)) {
        const m = sgPath(url).match(/^\/tournament\/([^\/]+)/);
        if (m) return `tournament/${m[1]}`;
      }
    } catch { /* fall through */ }
  }
  if (typeof displayId === 'string') {
    const parts = displayId.split('/').filter(Boolean);
    if (parts.length >= 1 && parts[0]) return `tournament/${parts[0]}`;
  }
  return null;
}

// The event to poll from a tournament's events (decision 12, as Tally rules it): one
// event → that one, whatever its game; several → always stop, naming them, so the
// operator pastes the event's own URL (a guess could crawl another game's bracket);
// none → stop. Pure, and exported for verify.mjs.
export function chooseStartggEvent(events) {
  const list = Array.isArray(events) ? events : [];
  if (list.length === 0) throw new Error('start.gg: this tournament has no events yet');
  const chosen = list.length === 1 ? list[0] : null;
  if (!chosen || !chosen.slug) {
    const names = list.map((e) => `${e?.name} → ${e?.slug}`).join('; ');
    throw new Error(`start.gg: tournament has multiple events — paste a specific event URL. Events: ${names}`);
  }
  return chosen.slug; // already "tournament/<t>/event/<e>"
}

// Looks up a tournament-level URL's events and picks one (chooseStartggEvent).
async function resolveEventSlug(tournamentSlug, apiKey, signal) {
  const data = await gql({
    operationName: 'ZipperTournamentEvents',
    query: TOURNAMENT_EVENTS_QUERY,
    variables: { slug: tournamentSlug },
    apiKey,
    signal,
  });
  const tournament = data?.tournament;
  if (!tournament) {
    throw new Error('start.gg: tournament not found (private or draft events need an API token with admin access)');
  }
  return chooseStartggEvent(tournament.events);
}

// Reads the event's sets, page by page (see "Paging" at the top). Gives the event of the
// first page (its standings) with the sets read, de-duplicated by id, whether paging
// stopped early for having enough (cutShort) or at the page limit (pollNotice), and how
// many standings the first page asked for (standingsAsked). A read to the last page is
// neither cut short nor at the limit.
async function fetchAllSets(slug, apiKey, signal, { maxItems = null, topN = 0 } = {}) {
  const top = Math.round(Number(topN)) || 0;
  const max = Math.round(Number(maxItems));
  // With topN on, every page is read (the filter needs every match of the top players).
  // Without a usable maxItems, every page is read too, as before.
  const enough = top > 0 || !(max > 0) ? Infinity : max;
  const standingsFirst = Math.min(STANDINGS_MAX, Math.max(STANDINGS_MIN, top));
  let event = null;
  let page = 1;
  const nodes = [];
  const seen = new Set();
  let completed = 0;
  let cutShort = false;
  let pollNotice = '';

  for (;;) {
    const data = await gql({
      operationName: 'ZipperEventSets',
      query: SETS_QUERY,
      variables: { slug, page, perPage: PAGE_SIZE, standingsPerPage: page === 1 ? standingsFirst : 1 },
      apiKey,
      signal,
    });
    const ev = data?.event;
    if (!ev) throw new Error('start.gg: event not found (private, deleted, or wrong URL path)');
    event = event || ev;
    const conn = ev.sets;
    if (!conn || !Array.isArray(conn.nodes)) break;
    for (const s of conn.nodes) {
      const id = s?.id == null ? null : String(s.id);
      if (id !== null) {
        if (seen.has(id)) continue; // already read on the page before
        seen.add(id);
      }
      nodes.push(s);
      if (Number(s?.state) === 3) completed++;
    }
    const totalPages = Number(conn.pageInfo?.totalPages) || 1;
    if (page >= totalPages) break; // the last page: nothing left unread
    if (completed >= enough) { cutShort = true; break; }
    if (page >= MAX_PAGES) { pollNotice = PAGE_LIMIT_NOTICE; break; }
    page++;
  }

  if (!event) throw new Error('start.gg: event not found');
  return { event: { ...event, sets: { nodes } }, cutShort, pollNotice, standingsAsked: standingsFirst };
}

async function gql({ operationName, query, variables, apiKey, signal }) {
  return apiKey
    ? gqlOfficialApi({ operationName, query, variables, apiKey, signal })
    : gqlPublicProxy({ operationName, query, variables, signal });
}

async function gqlOfficialApi({ operationName, query, variables, apiKey, signal }) {
  const res = await guardedFetch(API_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
      'User-Agent': USER_AGENT,
    },
    body: JSON.stringify({ operationName, variables, query }),
    signal,
  });
  if (res.status === 401 || res.status === 403) {
    throw statusError('start.gg API: token rejected (check "startggApiKey" in config.json — get one at start.gg → developer settings)', res);
  }
  if (res.status === 429) {
    throw statusError('start.gg API: rate limit exceeded (default 80 requests / 60 seconds per token) — raise the poll interval', res);
  }
  if (!res.ok) throw statusError(`start.gg API ${res.status}: ${res.statusText}`, res);
  const obj = res.json();
  if (obj.errors) throw new Error(`start.gg API GraphQL: ${obj.errors[0]?.message ?? 'unknown'}`);
  return obj.data ?? null;
}

let cachedClientVersion = DEFAULT_CLIENT_VERSION;

async function gqlPublicProxy({ operationName, query, variables, signal }) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const res = await guardedFetch(PUBLIC_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': USER_AGENT,
        'client-version': cachedClientVersion,
        'x-web-source': 'gg-web-gql-client',
        'apollo-client-id': 'smashgg-legacy',
      },
      body: JSON.stringify([{ operationName, variables, query }]),
      signal,
    });

    if (res.status === 400) {
      const text = res.text;
      // Self-heal when start.gg bumps the required client-version.
      try {
        const obj = JSON.parse(text);
        const required = obj?.meta?.requiredClientVersion;
        if (required && String(required) !== cachedClientVersion && attempt === 0) {
          cachedClientVersion = String(required);
          continue;
        }
      } catch { /* fall through to throw below */ }
      throw statusError(`start.gg 400: ${text.slice(0, 240)} — the public proxy may not allow the sets query; set "startggApiKey" in config.json`, res);
    }
    if (!res.ok) throw statusError(`start.gg ${res.status}: ${res.statusText}`, res);
    const arr = res.json();
    const first = arr[0];
    if (first?.errors) throw new Error(`start.gg GraphQL: ${first.errors[0]?.message ?? 'unknown error'}`);
    return first?.data ?? null;
  }
  throw new Error('start.gg: gql retries exhausted');
}
