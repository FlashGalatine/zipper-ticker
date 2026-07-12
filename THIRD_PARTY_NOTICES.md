# Third-party notices

Zipper's runtime dependency:

- **ws** (https://github.com/websockets/ws) — MIT License. Used by the sidecar
  and the verify suites to speak Streamer.bot's WebSocket protocol from Node.

Dev-only (installed with `--no-save`, never shipped):

- **playwright-core** (https://github.com/microsoft/playwright) — Apache-2.0.
  Drives a system Chromium for `npm run verify:render` and `npm run shots`.

Zipper talks to third-party tournament services (Challonge, start.gg,
Matcherino, Round.one, TourneyBot) over their public endpoints; no code from
those services is bundled. Respect each service's terms — configure API keys
(`challongeApiKey`, `startggApiKey`) where you have them, and keep the poll
interval reasonable (Zipper floors it at 15 seconds).
