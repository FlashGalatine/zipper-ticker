// TourneyBot — URL detection is live so the sidecar reports a clear status, but
// results fetching is NOT implemented yet (v1.5): the public tournament page
// embeds a Next.js __NEXT_DATA__ blob whose pageProps carries `players`, and
// likely match state too — the match structure needs a recorded fixture from a
// live bracket before the normalizer can be written (see README.md roadmap).

export const tourneybot = {
  name: 'tourneybot',
  displayName: 'TourneyBot',

  matches(input) {
    if (!input) return false;
    try {
      const u = new URL(input);
      return /(^|\.)tourneybot\.gg$/i.test(u.hostname);
    } catch {
      return false;
    }
  },

  parseId(input) {
    const raw = String(input || '').trim();
    if (!raw) return null;
    try {
      const url = new URL(raw);
      if (!/(^|\.)tourneybot\.gg$/i.test(url.hostname)) return null;
      const m = url.pathname.match(/\/tourneys\/(\d+)/);
      return m ? m[1] : null;
    } catch {
      return null;
    }
  },

  async fetchTicker() {
    throw new Error('TourneyBot: results support is not implemented yet (planned — Zipper currently supports Challonge and start.gg)');
  },
};
