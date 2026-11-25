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
  "caps": { "text": "@FlashGalatine", "logo": "/zipper-overlay/assets/logo.png" },
  "message": { "lines": ["Bracket starts at 8pm ET", "!discord for the lobby code"] }
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
- `message` is present only while persistent message mode is on **and** its text
  is non-blank; the overlay then crawls `message.lines` (one item per line)
  instead of the results. See below.

## Persistent message mode

A configured message that holds the strip until you switch it off — for
"bracket starts at 8pm", a Discord plug, a rules line. Set it from the control
page's **Persistent message** box (textarea + a *Show this instead of results*
checkbox), or from chat / a Stream Deck via the single-field commands.

```json
"message": { "enabled": true, "text": "Bracket starts at 8pm ET\n!discord for the lobby code" }
```

Rules:

- One **non-blank line per ticker item**, trimmed, capped at 20 lines / 1000
  characters. The marquee repeats them to fill the strip, exactly as it does for
  a short results set.
- Enabled with blank text is the same as off — the payload's `message` key is
  omitted and results show. That's the fallback for any malformed line list too.
- Results **keep polling underneath**: the same payload still carries `matches`
  and `standings`, so switching the message off restores live results on the
  spot with no re-poll.
- It needs no tournament at all. Message edits push **immediately** rather than
  riding the next poll (which, with polling off, would never come), and the
  sidecar re-pushes a configured message when it connects to Streamer.bot, so a
  message-only setup survives a restart. For the same reason end-cap changes now
  push immediately when polling is off.
- `ticker:announce` still interrupts it — announcements cover the strip for
  their duration and the message resumes.
- Persisted in `config.json` (`message`), so it survives a sidecar restart.

## ticker:status

Ephemeral sidecar status for the control page. It must not clobber the
persisted update payload, so it rides the **Ticker Command** relay as the
reserved command `__status` with the status JSON as `value`:

```json
{ "type": "ticker:command", "command": "__status",
  "value": "{\"type\":\"ticker:status\",\"polling\":true,\"url\":\"…\",\"platform\":\"challonge\",\"lastPollAt\":\"…\",\"nextPollAt\":\"…\",\"lastError\":\"\",\"commandNotice\":\"\",\"intervalMs\":30000,\"maxItems\":20,\"caps\":{…}}" }
```

The sidecar ignores its own `__status` echo, in any casing; the control page
unwraps `value`. Sent after every poll and after every command.

- `lastError` — why the last poll failed; `""` after a good poll and after an
  accepted `setUrl`. This includes the limits below: a site that took longer
  than 10 s to answer, a poll that took longer than 45 s, or an answer over
  16 MB. The message names the site's host and the limit, never the address.
  A request that could not be made at all reads `<host>: the request failed`,
  with the system's code when there is one (for example `ECONNREFUSED` or
  `ENOTFOUND`), never the address or a header's value.
  A poll of the previous URL that ends after a `setUrl` leaves it alone.
- `nextPollAt` — when the next poll runs (ISO time), or `null` when none is
  waiting (polling off, or a poll under way). It counts the interval, the error
  backoff, a site's Retry-After and the minimum gap below, whichever is latest.
  Unlike `auto.waitingUntil`, it does not mean the sidecar is waiting for the
  event to start.
- `commandNotice` — why the last command was not carried out, or not in full:
  an unknown command (by its name only, cut to 40 characters, never its value),
  a refused `setUrl`, a refused logo, or `start` / `pollNow` with no tournament
  URL ("No tournament URL set"). `""` otherwise. The next accepted command clears
  it; `status`, which the control page sends every 10 seconds, does not. The
  control page shows it in its status line.

The status carries an `auto` block describing the auto-scheduler (see
`setAutoSchedule` below):

```json
"auto": { "enabled": true, "eventState": "upcoming",
  "startAt": "2026-08-01T00:00:00.000Z", "waitingUntil": "2026-08-01T00:00:00.000Z",
  "stopped": null }
```

- `eventState` / `startAt` — what the last successful poll said about the event
  (`startAt` is `null` when the platform has no date; Matcherino never reports
  either).
- `waitingUntil` — set while polling is on but sleeping for the start time; the
  next poll fires at that instant.
- `stopped` — `"completed"` after the scheduler stopped polling itself, `null`
  otherwise.

### When the sidecar polls

- **One poll at a time**, with 45 s in all. Each request in it has 10 s, until
  its whole answer is in, and an answer may be at most 16 MB. A poll that runs
  past a limit is stopped and fails (`lastError`).
- **At least 15 s between polls**, counted from the end of one to the start of
  the next, whatever asks for them. A command that asks for a poll inside that
  gap (`pollNow`, `start`, `setUrl`, or an option change while polling) is
  answered with a status at once and polled when the gap ends, so a burst of
  commands costs one poll. A `pollNow` or `start` that arrives while a poll is
  under way runs after it. A cap or option change made while polling reaches
  the strip with that poll, so it too waits for the gap, and for a site's
  Retry-After.
- **After an error** the wait doubles with each failure in a row, up to 5
  minutes, and is never shorter than the poll interval.
- **A site's Retry-After** (on a 429 or 503) holds back every poll of that
  platform until it ends, up to 5 minutes or the interval, whichever is longer.
  No command cuts it short, `pollNow` and `setUrl` included. The wait belongs to
  the platform: a URL on another platform is polled as usual, and a URL on the
  waiting platform (set again, set back, or set after clearing the URL) still
  waits for its end.
- **A changed URL.** A poll still running for the old URL when `setUrl` arrives
  is stopped, and whatever it brings back is thrown away.

## ticker:command

Control surfaces (control page, chat trigger, Stream Deck) call the **Ticker
Command** action with `command`/`value` arguments; the C# side re-broadcasts

```json
{ "type": "ticker:command", "command": "setUrl", "value": "https://challonge.com/blko42" }
```

which the sidecar consumes. Command names match without regard to case
(`pollnow` is `pollNow`, as a dedicated chat command relays it); values are
passed as they are. An unknown name is ignored and reported in the status's
`commandNotice`. Commands:

| command       | value                          | effect |
|---------------|--------------------------------|--------|
| `setUrl`      | tournament URL (empty = none)  | switch tournaments (forces a re-push on next poll). A poll of the old URL still running is stopped and its result thrown away; while polling, the new URL is polled once the gap allows (see [When the sidecar polls](#when-the-sidecar-polls)). An empty value clears the URL and **stops polling**. A refused `stub:` URL (see [Test hook](#test-hook)) keeps the old URL, saves nothing and says why in `commandNotice` |
| `start`       | —                              | start polling |
| `stop`        | —                              | stop polling |
| `pollNow`     | —                              | a fresh poll as soon as the 15 s gap after the last poll and any Retry-After allow (starts polling if stopped); the status answers at once, with `nextPollAt` |
| `setInterval` | milliseconds (min 15000)       | poll cadence |
| `setCaps`     | JSON `{text,logo}`             | **batched** — sets both end caps in one command (the control page uses this). A logo `setCapsLogo` would refuse is refused here too: the old logo stays, the text still applies, and `commandNotice` says why |
| `setOptions`  | JSON `{intervalMs,maxItems,topN}` | **batched** — sets all three options in one command. While polling, a changed `maxItems` or `topN` asks for a poll, as `setMaxItems` and `setTopN` do; a new interval alone (or no change) only re-times the next poll, so it never cuts an error backoff or a Retry-After short |
| `setMessage`  | JSON `{text,enabled}`          | **batched** — persistent message text + on/off in one command (the control page uses this) |
| `setMessageText` | string (≤1000 chars, `\n`-separated lines) | message text (single-field; for chat / Stream Deck) |
| `setMessageEnabled` | `on`/`off` (also `true`/`false`, `1`/`0`, `yes`) | show the message instead of results |
| `setCapsText` | string (≤120 chars)            | right-cap static text (single-field; for chat / Stream Deck) |
| `setCapsLogo` | an http(s) URL, or a path on the overlay's server (≤500 chars; empty = no logo) | left-cap logo image (single-field). Anything else — `data:`, `file:`, `javascript:`, `//host`, `\\host` — is refused: the old logo stays and `commandNotice` says why. A saved logo that breaks this rule is dropped when `config.json` loads |
| `setInterval` above, `setMaxItems`, `setTopN` | see below | single-field variants |
| `setMaxItems` | 1–100                          | ticker item cap |
| `setTopN`     | 0–100 (0 = off)                | only show matches involving the top-N placed participants (needs platform standings — start.gg always; Challonge via `final_rank` on the keyed API for your own tournaments, or derived from elimination order on finished brackets when scraping; also trims the standings crawl to N) |
| `setAutoSchedule` | `on`/`off` (default on)    | auto start/stop: a poll that returns `completed` pushes the final payload then **stops polling**; one that returns `upcoming` **sleeps until the platform's start time** (re-checking every 5 min without one, and at most every 6 h for far-off dates) and resumes normal polling when the event goes live. `start`/`pollNow` get a poll even during that sleep or after that stop, but it still waits for the 15 s gap after the last poll and for a site's Retry-After (see [When the sidecar polls](#when-the-sidecar-polls)). |
| `status`      | —                              | request a status re-broadcast |

> **Why the batched `setCaps` / `setOptions` exist.** Streamer.bot can mix up
> the arguments of two runs of the same action when the runs overlap: two
> `DoAction`s to one action within a few milliseconds can both run with the
> later one's arguments (seen on SB 1.0.4; measured on 1.0.7, 25 of 25 tries in
> the default queue). A control page that set caps text and logo as two
> back-to-back `setCapsText` + `setCapsLogo` commands would have the second's
> value land on the first, silently blanking the text.
>
> Putting `Ticker Command`, `Ticker Push` and `Ticker Announce` in one
> **blocking** action queue removes this for every sender (0 of 25 tries), and
> the README's setup asks for it. The batching stays as a second guard for an
> install without that queue: each control button sends exactly **one** command
> carrying a JSON value, and `panel-core` additionally serializes outbound
> `DoAction`s (one in flight at a time). The single-field commands remain for
> chat / Stream Deck.

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

It works only when the sidecar is started with `ZIPPER_ALLOW_STUB=1` (the verify
suite sets it; a normal install never does), and only for a file inside the repo
folder. The path is checked as text before any file is opened, so a path outside
the folder — an absolute path elsewhere, `..`, a network share — is never opened
or looked up. An absolute path counts as inside only when it begins with the
folder's own path exactly, letter case included. Otherwise `setUrl` refuses the URL, keeps the old one and says why
in `commandNotice`, and a poll of a `stub:` URL saved in `config.json` fails with
the same message in `lastError`. A file that cannot be read, or that is not a
JSON object, fails the poll with a fixed message that quotes none of its content.

With `ZIPPER_ALLOW_STUB=1` a stub file may also name a hold file
(`"stubHoldFile": "<path-relative-to-repo>"`): the poll then waits while that file
exists, so a test can hold a poll open and release it by deleting the file. The
poll's 45 s deadline, or a `setUrl`, ends the wait as it would a request. With
`"stubHoldIgnoresAbort": true` beside the hold file, the wait does not end when
the poll is stopped: it goes on until the file is deleted, so the poll's result
(or its `stubFail`) comes back after a `setUrl` has moved on, as an answer
already on its way would. It is still bounded: the wait ends by itself 45 s
after it began (the poll's deadline), and the poll then fails as a stopped one
does. Without a hold file the key does nothing. A stub file may also make its
poll fail as a rate-limited site would
(`"stubFail": { "status": 429, "retryAfter": "120" }`, the Retry-After in seconds
or as an HTTP date), with the fixed message `stub: the test file answered as
HTTP <status>`.

The tests shorten the poll loop's limits with `ZIPPER_REQUEST_TIMEOUT_MS` (10 s
per request), `ZIPPER_POLL_DEADLINE_MS` (45 s per poll) and
`ZIPPER_MIN_POLL_GAP_MS` (15 s between polls). Each is clamped, so it can only
shorten its limit.
