# Zipper wire protocol

Everything rides Streamer.bot's WebSocket bus as `General.Custom` broadcasts
(`CPH.WebsocketBroadcastJson`). Three payload types, all carrying a `type` key.

## ticker:update

Broadcast by the **Ticker Push** action — either because the sidecar pushed a
fresh payload (as the `tickerPayload` action argument), or because an overlay
(re)connected and asked for the persisted replay. The C# action stores the last
payload in the persisted global `ticker.payload`, so a freshly-added OBS source
paints immediately.

```json
{
  "type": "ticker:update",
  "tournament": {
    "name": "BLKO Weekly #42",
    "platform": "challonge",
    "url": "https://challonge.com/blko42",
    "state": "in_progress"
  },
  "generatedAt": "2026-07-11T20:15:00Z",
  "matches": [
    { "id": "9001", "round": "Winners Semis", "state": "completed",
      "p1": { "name": "Flash", "score": 2 }, "p2": { "name": "Newbie", "score": 0 },
      "winner": 1, "order": 1 }
  ],
  "standings": [ { "placement": 1, "name": "Flash" } ],
  "caps": { "text": "@FlashGalatine", "logo": "/zipper-overlay/assets/logo.png" }
}
```

Rules:

- `matches[].state` ∈ `completed | in_progress | upcoming`.
- `winner` ∈ `0 | 1 | 2` (0 = undecided). Scores are integers or `null`
  (`null` renders as "vs").
- Matches arrive pre-sorted: newest completed first, then in-progress, then
  upcoming — capped at the sidecar's `maxItems` (default 20). Upcoming
  TBD-vs-TBD matches are dropped before the cap.
- `standings` is populated when a platform can't provide matches (Matcherino);
  the overlay renders the standings crawl only when `matches` is empty.
- `caps` travels inside the payload so the overlay needs no separate config
  fetch; empty string hides that cap.
- `tournament.state` ∈ `upcoming | in_progress | completed`.

## ticker:status

Ephemeral sidecar status for the control page. It must not clobber the
persisted update payload, so it rides the **Ticker Command** relay as the
reserved command `__status` with the status JSON as `value`:

```json
{ "type": "ticker:command", "command": "__status",
  "value": "{\"type\":\"ticker:status\",\"polling\":true,\"url\":\"…\",\"platform\":\"challonge\",\"lastPollAt\":\"…\",\"lastError\":\"\",\"intervalMs\":30000,\"maxItems\":20,\"caps\":{…}}" }
```

The sidecar ignores its own `__status` echo; the control page unwraps `value`.
Sent after every poll and after every command.

## ticker:command

Control surfaces (control page, chat trigger, Stream Deck) call the **Ticker
Command** action with `command`/`value` arguments; the C# side re-broadcasts

```json
{ "type": "ticker:command", "command": "setUrl", "value": "https://challonge.com/blko42" }
```

which the sidecar consumes. Commands:

| command       | value                          | effect |
|---------------|--------------------------------|--------|
| `setUrl`      | tournament URL                 | switch tournaments (forces a re-push on next poll) |
| `start`       | —                              | start polling |
| `stop`        | —                              | stop polling |
| `pollNow`     | —                              | immediate poll (starts polling if stopped) |
| `setInterval` | milliseconds (min 15000)       | poll cadence |
| `setCaps`     | JSON `{text,logo}`             | **batched** — sets both end caps in one command (the control page uses this) |
| `setOptions`  | JSON `{intervalMs,maxItems,topN}` | **batched** — sets all three options in one command |
| `setCapsText` | string (≤120 chars)            | right-cap static text (single-field; for chat / Stream Deck) |
| `setCapsLogo` | URL/path (≤500 chars)          | left-cap logo image (single-field) |
| `setInterval` above, `setMaxItems`, `setTopN` | see below | single-field variants |
| `setMaxItems` | 1–100                          | ticker item cap |
| `setTopN`     | 0–100 (0 = off)                | only show matches involving the top-N placed participants (needs platform standings — start.gg always; Challonge via `final_rank` on the keyed API for your own tournaments, or derived from elimination order on finished brackets when scraping; also trims the standings crawl to N) |
| `status`      | —                              | request a status re-broadcast |

> **Why the batched `setCaps` / `setOptions` exist.** Streamer.bot bleeds
> arguments between action invocations that arrive in a burst — firing two
> `DoAction`s to the same action within a few milliseconds shuffles their args
> together (verified on SB 1.0.4). A control page that set caps text and logo
> as two back-to-back `setCapsText` + `setCapsLogo` commands would have the
> second's value land on the first, silently blanking the text. So each control
> button sends exactly **one** command carrying a JSON value, and `panel-core`
> additionally serializes outbound `DoAction`s (one in flight at a time). The
> single-field commands remain for chat / Stream Deck, where one action fires at
> a time.

## ticker:announce

Interrupts the strip: the overlay pauses the results crawl in place, shows the
announcement (kind pill + text) for `duration` seconds, then resumes.
Announcements queue (up to 10) and play back-to-back. Ephemeral — never
persisted, so reconnecting overlays don't replay them.

```json
{ "type": "ticker:announce", "kind": "raid", "text": "Flash is raiding with 42 viewers!", "duration": 8 }
```

- `kind` ∈ `sub | raid | milestone | custom` (styles the pill; anything else
  renders as custom). `duration` is clamped 2–30 s (default 8).
- Broadcast by the **Ticker Announce** action. Sources:
  - **SB Twitch triggers** (subs, gift subs, raids): add the action to the
    trigger; with no `text` arg it auto-composes from the trigger's `user` /
    `viewers` arguments. Pass explicit `text` (Set Argument) to customize.
  - **Control page**: the Announcement box DoActions it directly.
  - **Sidecar milestones**: when a StreamElements session counter crosses a
    configured step (see below), the sidecar fires it with kind `milestone`.

### StreamElements milestones (sidecar)

`config.json`:

```json
"streamElements": { "jwtToken": "…", "pollSeconds": 60 },
"milestones": { "followers": 100, "subs": 25, "tips": 50, "cheers": 0 }
```

The sidecar decodes the channel id from the JWT and polls
`api.streamelements.com/kappa/v2/sessions/<channel>` (the same recipe as the
Midnight Velvet goal meter). Each milestones entry is a step size — e.g.
`followers: 100` announces at 300, 400, 500 followers ("300 followers — thank
you!"). `0` disables a metric. The first successful poll only records the
current thresholds (no announcement blast when you first configure it), and
announced thresholds persist in `config.json` (`milestoneState`) so restarts
don't repeat them. **The JWT never travels the bus** — `ticker:status` carries
only `se: { configured, lastPollAt, lastError }`.

## ticker:error

Broadcast by either C# action when it throws, so failures are visible instead
of silent: `{ "type": "ticker:error", "message": "…" }`.

## Test hook

A tournament URL of the form `stub:<path-relative-to-repo>` makes the sidecar
load a normalized platform result (`{tournamentName, state, matches, standings}`)
from that JSON file instead of the network — used by `verify.mjs`
(`stub:platforms/fixtures/stub-result.json`) and handy for offline styling.
