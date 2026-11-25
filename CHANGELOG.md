# Changelog

All notable changes to **Zipper** are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the patch number
tracks the repository's commit count at release time.

Nothing was tagged between the initial release and 1.0.11, so that release
collects every change in between. Commit hashes and landing dates are noted for
traceability.

## [Unreleased]

## [1.0.18] — 2026-11-25

Copy `zipper-sidecar.mjs` and the `zipper-shared` folder. Stop the sidecar and
start it again, and reload the dock or control page at the same time: an older
control page shows no reason for a refused command. Nothing to re-paste, and the
OBS overlay needs nothing.

### Security

- **A `stub:` tournament URL could make the sidecar read any file on the
  machine**, or connect to a file share on another computer, and a file that was
  not JSON was quoted in the error shown on the control page. Anyone who could
  send `setUrl` could do it, including chat when the `!ticker` command was left
  open. `stub:` URLs are a test hook; they now work only when the sidecar is
  started with `ZIPPER_ALLOW_STUB=1`, and only for a file inside the Zipper
  folder. Refusals and read failures give fixed messages that quote no path and
  no file content. **If you used a `stub:` URL for offline styling, it no longer
  works in a normal install**: start the sidecar with that variable set, with
  the file inside the Zipper folder.
- **The end-cap logo accepts only a web address or a path on the overlay's own
  server**, such as `https://…/logo.png` or `assets/logo.png`. Anything else
  (`data:`, `file:`, `javascript:`, another computer's share) is refused and the
  old logo stays. A logo saved in `config.json` that fails the rule is removed
  when the sidecar starts. If yours was removed, set it again from the control
  page; until the next update reaches the overlay, Streamer.bot may still replay
  the old one, so click **Apply** on the caps or **Poll now** once after
  restarting.

### Fixed

- **Command names work whatever their capitalisation.** A dedicated chat command
  such as `!pollnow`, and `!ticker seturl …` typed in lower case, were silently
  ignored before.
- **An unknown command is reported.** The control page's status shows
  `unknown command "…" — ignored` until the next accepted command. Only the
  name is shown, cut to 40 characters, never what followed it.
- **Clearing the tournament URL stops polling.** Before, polling stayed on and
  failed on every round with an unhelpful error. Start and Poll now with no URL
  set say "No tournament URL set".

### Changed

- The sidecar's status has a new field, `commandNotice`, for a refused command.
  Poll failures stay in `lastError`. `docs/PROTOCOL.md` describes both, and the
  test hook.

## [1.0.16] — 2026-11-25

**Node.js 20 or later is now required** (it was 18). Check with `node --version`
before updating. Then copy the new `start-zipper.bat`. Restarting the sidecar is
optional: `zipper-sidecar.mjs` changed only for the tests and behaves the same in
normal use. Nothing to re-paste.

### Changed

- **The minimum Node.js version is 20**, up from 18, everywhere it is stated:
  `package.json`, the README, the launcher and the header of
  `Ticker Sidecar Start`.
- **`start-zipper.bat` checks the Node version.** When Node is missing or older
  than 20 it stops with one line saying so and where to get it, before
  installing anything or starting the sidecar.
- **A fresh `npm install` also fetches `playwright-core` 1.65.0**, including the
  launcher's first run. It is a development dependency used only by the render
  tests; nothing a streamer runs uses it. It is the reason for the Node 20
  minimum.
- Batch files keep Windows line endings in every checkout and ZIP download.

### Added (development only)

- **`npm run verify:cs`** compiles the three bus actions with the .NET Framework
  compiler against a stand-in for Streamer.bot and runs one table of cases
  through them. The same table runs through the mock, so the two cannot drift
  apart. Windows only.
- **The mock answers as Streamer.bot 1.0.7 does**: a reply when the action is
  queued, naming its queue, and an error reply for an action that does not
  exist. Switches, off by default, make an action missing, silent or failing,
  close every connection, or reproduce mixed-up arguments
  (`/mock/bleed?on=1|0`).
- **Each suite refuses to run when something already answers on its ports**,
  where before it could quietly use a leftover process. A test sidecar that
  stops early fails the run, and every child process is stopped at the end.
- Checks for every command, every page (no page or console errors) and every
  numeric range. The handful of older checks that could not fail are replaced.

### Not checked

- The launcher's version check was tested with a stand-in for Node, not by
  double-clicking it on a machine with Node 18.
- The suites ran on Node 22, not on Node 20 itself.

## [1.0.14] — 2026-11-25

Docs only: nothing to copy, restart or re-paste. One setting to make in
Streamer.bot, described below.

### Changed

- **Setup now asks for a blocking action queue.** Put `Ticker Command`,
  `Ticker Push` and `Ticker Announce` in one queue with **Blocking** ticked.
  Streamer.bot can mix up the arguments of two runs of the same action when
  they overlap, and a blocking queue runs them one at a time. Measured on
  Streamer.bot 1.0.7 with a test action: back-to-back requests mixed their
  arguments in 25 of 25 tries in the default queue, 0 of 25 in a blocking
  queue. This covers every sender: the control page, chat, a Stream Deck,
  Twitch triggers and the sidecar. `docs/PROTOCOL.md` is corrected to match:
  the fault is overlapping runs of one action, and it is present on 1.0.7 as
  well as 1.0.4.
- **Setup now says to restrict the `!ticker` chat command** to moderators and
  the broadcaster, and what an open command allows: changing the tournament
  URL, replacing the results with any message, changing the end-cap text or
  image, and stopping polling. `Ticker Command` does not check who typed a
  command.
- The README says Zipper is tested on Streamer.bot 1.0.4 and 1.0.7, counts the
  optional fourth action, and no longer quotes a number of checks.
- The README's X handle was updated in `c8cede2`, after 1.0.11.

## [1.0.11] — 2026-07-29

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

[Unreleased]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.18...main
[1.0.18]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.16...v1.0.18
[1.0.16]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.14...v1.0.16
[1.0.14]: https://github.com/FlashGalatine/zipper-ticker/compare/dbdb789...v1.0.14
[1.0.11]: https://github.com/FlashGalatine/zipper-ticker/compare/d77a27f...dbdb789
