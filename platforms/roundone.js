// Round.one — URL detection is live so the sidecar reports a clear status, but
// results fetching is NOT implemented yet (v1.5):
//   GET https://round.one/api/tournaments/<slug>
// returns the full tournament document (Tally's roster importer reads its
// `players[]`); the match/bracket array shape needs a recorded fixture from a
// live bracket before the normalizer can be written (see README.md roadmap).

export const roundone = {
  name: 'roundone',
  displayName: 'Round.one',

  matches(input) {
    if (!input) return false;
    try {
      const u = new URL(input);
      return /(^|\.)round\.one$/i.test(u.hostname);
    } catch {
      return false;
    }
  },

  parseId(input) {
    const raw = String(input || '').trim();
    if (!raw) return null;
    try {
      const url = new URL(raw);
      if (!/(^|\.)round\.one$/i.test(url.hostname)) return null;
      const m = url.pathname.match(/^\/tournament\/([^\/?#]+)/);
      return m ? m[1] : null;
    } catch {
      return null;
    }
  },

  async fetchTicker() {
    throw new Error('Round.one: results support is not implemented yet (planned — Zipper currently supports Challonge and start.gg)');
  },
};
