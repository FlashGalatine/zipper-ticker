# Changelog

All notable changes to **Zipper** are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Nothing has been tagged since the initial release, so every change that landed
afterwards sits under **Unreleased**. Commit hashes and landing dates are noted
for traceability.

## [Unreleased]

### Added

- **Auto start / stop polling around the event window** (`config.autoSchedule`,
  default **on**). Polling now spends API calls only while the event is
  actually running:
  - A poll that returns `completed` pushes the final payload and then stops
    polling by itself — the final results stay on the strip.
  - A poll that returns `upcoming` sleeps until the platform's advertised start
    time and resumes normal polling when the event goes live, re-checking every
    5 minutes when there's no start time and at most every 6 hours for far-off
    dates (so a moved schedule is caught).
  - Errors keep the existing backoff; `start` / `pollNow` always force a poll.

  `startAt` now rides the normalized provider result (start.gg `event.startAt`
  epoch → ISO; Challonge `tournament.start_at`, read defensively from the embed
  store too). New `setAutoSchedule` command and an `auto` block in
  `ticker:status` (`eventState` / `startAt` / `waitingUntil` / `stopped`); the
  control page gains an "Auto start/stop" checkbox and a WAITING /
  event-complete status readout. `verify` 86/86, render 29/29.
  (`5ad2c7b`, 2026-07-29)
- **Persistent message mode** — a message typed into the control page can take
  the strip over instead of tournament results ("bracket starts at 8pm", a
  Discord plug, a rules line). One non-blank line per ticker item, capped at 20
  lines / 1000 characters. Results keep polling underneath in the same payload,
  so toggling the message off restores them on the spot with no re-poll. It
  needs no tournament set at all, which is the case that shaped the design:
  every other config change waits for the next poll to carry it to the overlay,
  and with polling off that poll never comes — so message edits push immediately
  via `pushCurrent()`, and the sidecar re-pushes a configured message when it
  connects to SB so a message-only setup survives a restart. The same fix
  applies to end-cap changes made while stopped, which had the same latent hole.
  It rides the existing `ticker:update` payload as an optional `message:{lines}`
  key, so no C# changes were needed. `verify` 74/74, render 29/29.
  (`116d321`, 2026-07-15)
- **`?h=<px>` overlay height parameter** (or `window.__TICKER_HEIGHT`), mirroring
  `?w=`; sets `--ticker-height`, default 72, minimum 20. (`3efdd9b`,
  2026-07-12)
- **Down-sidecar feedback instead of silently swallowed commands.** On real
  Streamer.bot the control page's Start / Poll / Stop / Options / end-cap
  buttons did nothing while Announcements worked: those buttons relay a
  `ticker:command` that only the sidecar consumes, and with no sidecar running
  the clicks were lost with zero feedback. Each `send()` now arms a 3 s
  liveness timer that any `ticker:status` reply clears; no reply raises a red
  "sidecar isn't responding" banner, with an on-load probe and a 10 s periodic
  re-probe. The sidecar calls `pushStatus()` at the top of `pollOnce()` so a
  command draws a prompt reply rather than waiting on a slow fetch, and Start
  reflects `polling:true` immediately. Adds an optional fourth SB action
  `Ticker Sidecar Start` so SB can auto-launch the sidecar from an
  application-started trigger. First real-SB 1.0.4 validation — announcements
  and polling confirmed live. (`27adbf5`, 2026-07-12)

### Fixed

- **Chat commands silently dropped.** A Streamer.bot Command trigger sets its
  own `command` argument (the matched chat command, `!ticker`) plus a
  `commandId`, shadowing the relayed one — so the action forwarded `!ticker`
  verbatim, the sidecar discarded it as unknown, and the `rawInput` fallback
  never ran. `Ticker Command` now detects the `commandId` argument and parses
  `rawInput` itself (`!ticker setUrl https://…` → `command=setUrl`), while
  dedicated chat commands like `!pollnow` relay as their own name. `verify`
  75/75. Same fix as Fortuna `0f8707b`, where the bug was found during live SB
  validation. (`5bd1489`, 2026-07-16)
- **Streamer.bot arg-bleed blanked the end caps.** Firing multiple `DoAction`s
  to the same SB action in a burst makes Streamer.bot shuffle their arguments
  together (verified on SB 1.0.4): the control page's "Apply caps" fired
  `setCapsText` and `setCapsLogo` back to back, so the empty logo value bled
  onto the text and silently blanked it. The control page now sends **one**
  command per button — batched `setCaps {text,logo}` and
  `setOptions {intervalMs,maxItems,topN}` carrying a JSON value — and
  `panel-core` serializes outbound `DoAction`s, one in flight at a time,
  ack-gated with an 800 ms fail-safe and the queue cleared on disconnect. The
  sidecar unpacks the batched commands; per-field commands are kept for chat.
  `verify` 65/65, confirmed live on SB 1.0.4. (`ef9df91`, 2026-07-12)

### Documentation

- README: clone / Download-ZIP as the first setup step. (`6aa3027`, 2026-07-21)
- Support links point at the personal Discord server. (`8ed932b`, 2026-07-20)
- Author & support and Credits blocks matching the other components; LICENSE
  and `package.json` author set to Ashe "Flash" Galatine. (`b6d3bb1`,
  2026-07-12)

## [0.1.0] — 2026-07-12

Initial release — scrolling tournament-results ticker for Streamer.bot.
(`d77a27f`)

### Added

- A scrolling OBS ticker strip showing live tournament results, driven by a
  Node sidecar polling the bracket site and three Streamer.bot C# actions
  (`Ticker Push` / `Ticker Command` / `Ticker Announce`).
- **Platforms** — Challonge (keyed API with `final_rank` standings and an auto
  404 fallback; keyless embed-store scrape with array scores, page-title name,
  bracket-depth chronology and derived standings on finished brackets) and
  start.gg (keyed GraphQL or keyless proxy with a self-healing client version;
  sets and standings, string/numeric event-state mapping). Matcherino is
  standings-only; Round.one and TourneyBot ship as detection stubs.
- **Overlay** — seamless marquee (seg + seg, speed scaling to content), `?w=<px>`
  width (1920 default, 640 retro), logo/text end caps, CSS-variable theming,
  "Winner def. Loser" rendering for DQ and unreported scores, and a `topN`
  top-placed filter.
- **Announcements** — a `ticker:announce` takeover for subs and raids (via SB
  Twitch triggers with auto-composed text), a StreamElements milestone poller
  (file-only JWT, persisted thresholds), and manual announcements from the
  control page.
- **Harness** — mock SB server (`:7474` maps + `:8080` envelope), `verify.mjs`
  63/63 (fixture normalizers, panel-core shim, sidecar and milestone E2E) and
  `verify-render.mjs` 20/20 (playwright-core real pixels, both widths, announce
  takeover).

Live-validated keyless against real start.gg and Challonge brackets across
pre-start, mid-event and completed states.

[Unreleased]: https://github.com/FlashGalatine/zipper-ticker/compare/d77a27f...main
