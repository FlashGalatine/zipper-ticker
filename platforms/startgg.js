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

const PUBLIC_ENDPOINT = 'https://www.start.gg/api/-/gql';
const API_ENDPOINT = 'https://api.start.gg/gql/alpha';
const DEFAULT_CLIENT_VERSION = '20';
const PAGE_SIZE = 50;
const MAX_PAGES = 10; // safety cap (500 sets — plenty for a ticker)
const SF6_VIDEOGAME_ID = 43868; // disambiguates multi-event tournaments toward SF6

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
      const ev = url.pathname.match(/^\/tournament\/([^\/]+)\/(?:event|events)\/([^\/]+)/);
      if (ev) return `${ev[1]}/${ev[2]}`;
      const t = url.pathname.match(/^\/tournament\/([^\/]+)/);
      if (t) return t[1];
      return null;
    } catch {
      return null;
    }
  },

  async fetchTicker(displayId, config, originalUrl) {
    const apiKey = config?.startggApiKey ?? null;
    let eventSlug = deriveEventSlug(originalUrl, displayId);
    if (!eventSlug) {
      const tournamentSlug = deriveTournamentSlug(originalUrl, displayId);
      if (!tournamentSlug) {
        throw new Error('start.gg: paste a tournament or event URL (e.g. https://www.start.gg/tournament/<t> or .../tournament/<t>/event/<e>)');
      }
      eventSlug = await resolveEventSlug(tournamentSlug, apiKey);
    }
    const event = await fetchAllSets(eventSlug, apiKey);
    return normalizeStartggEvent(event);
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

  return {
    tournamentName: tourneyName && eventName ? `${tourneyName} — ${eventName}` : (tourneyName || eventName),
    state,
    matches,
    standings,
  };
}

function deriveEventSlug(originalUrl, displayId) {
  if (originalUrl) {
    try {
      const url = new URL(originalUrl);
      if (/(^|\.)start\.gg$/i.test(url.hostname)) {
        const m = url.pathname.match(/^\/tournament\/([^\/]+)\/(?:event|events)\/([^\/]+)/);
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
        const m = url.pathname.match(/^\/tournament\/([^\/]+)/);
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

// Pick the event from a tournament-level URL. One event → use it; many → prefer
// the single SF6 event; otherwise throw, naming the events so the operator can
// paste a specific event URL.
async function resolveEventSlug(tournamentSlug, apiKey) {
  const data = await gql({
    operationName: 'ZipperTournamentEvents',
    query: TOURNAMENT_EVENTS_QUERY,
    variables: { slug: tournamentSlug },
    apiKey,
  });
  const tournament = data?.tournament;
  if (!tournament) {
    throw new Error('start.gg: tournament not found (private or draft events need an API token with admin access)');
  }
  const events = Array.isArray(tournament.events) ? tournament.events : [];
  if (events.length === 0) throw new Error('start.gg: this tournament has no events yet');

  let chosen = null;
  if (events.length === 1) {
    chosen = events[0];
  } else {
    const sf6 = events.filter((e) => Number(e?.videogame?.id) === SF6_VIDEOGAME_ID);
    if (sf6.length === 1) chosen = sf6[0];
  }
  if (!chosen || !chosen.slug) {
    const list = events.map((e) => `${e.name} → ${e.slug}`).join('; ');
    throw new Error(`start.gg: tournament has multiple events — paste a specific event URL. Events: ${list}`);
  }
  return chosen.slug; // already "tournament/<t>/event/<e>"
}

async function fetchAllSets(slug, apiKey) {
  let event = null;
  let page = 1;
  let totalPages = 1;
  const nodes = [];

  while (page <= totalPages && page <= MAX_PAGES) {
    const data = await gql({
      operationName: 'ZipperEventSets',
      query: SETS_QUERY,
      variables: { slug, page, perPage: PAGE_SIZE },
      apiKey,
    });
    const ev = data?.event;
    if (!ev) throw new Error('start.gg: event not found (private, deleted, or wrong URL path)');
    event = event || ev;
    const conn = ev.sets;
    if (!conn || !Array.isArray(conn.nodes)) break;
    nodes.push(...conn.nodes);
    totalPages = Number(conn.pageInfo?.totalPages) || 1;
    page++;
  }

  if (!event) throw new Error('start.gg: event not found');
  return { ...event, sets: { nodes } };
}

async function gql({ operationName, query, variables, apiKey }) {
  return apiKey
    ? gqlOfficialApi({ operationName, query, variables, apiKey })
    : gqlPublicProxy({ operationName, query, variables });
}

async function gqlOfficialApi({ operationName, query, variables, apiKey }) {
  const res = await fetch(API_ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
      'User-Agent': 'Zipper/0.1',
    },
    body: JSON.stringify({ operationName, variables, query }),
  });
  if (res.status === 401 || res.status === 403) {
    throw new Error('start.gg API: token rejected (check "startggApiKey" in config.json — get one at start.gg → developer settings)');
  }
  if (res.status === 429) {
    throw new Error('start.gg API: rate limit exceeded (default 80 requests / 60 seconds per token) — raise the poll interval');
  }
  if (!res.ok) throw new Error(`start.gg API ${res.status}: ${res.statusText}`);
  const obj = await res.json();
  if (obj.errors) throw new Error(`start.gg API GraphQL: ${obj.errors[0]?.message ?? 'unknown'}`);
  return obj.data ?? null;
}

let cachedClientVersion = DEFAULT_CLIENT_VERSION;

async function gqlPublicProxy({ operationName, query, variables }) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const res = await fetch(PUBLIC_ENDPOINT, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Zipper/0.1',
        'client-version': cachedClientVersion,
        'x-web-source': 'gg-web-gql-client',
        'apollo-client-id': 'smashgg-legacy',
      },
      body: JSON.stringify([{ operationName, variables, query }]),
    });

    if (res.status === 400) {
      const text = await res.text();
      // Self-heal when start.gg bumps the required client-version.
      try {
        const obj = JSON.parse(text);
        const required = obj?.meta?.requiredClientVersion;
        if (required && String(required) !== cachedClientVersion && attempt === 0) {
          cachedClientVersion = String(required);
          continue;
        }
      } catch { /* fall through to throw below */ }
      throw new Error(`start.gg 400: ${text.slice(0, 240)} — the public proxy may not allow the sets query; set "startggApiKey" in config.json`);
    }
    if (!res.ok) throw new Error(`start.gg ${res.status}: ${res.statusText}`);
    const arr = await res.json();
    const first = arr[0];
    if (first?.errors) throw new Error(`start.gg GraphQL: ${first.errors[0]?.message ?? 'unknown error'}`);
    return first?.data ?? null;
  }
  throw new Error('start.gg: gql retries exhausted');
}
