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
- `generatedAt` is when the sidecar built the payload; each one it stamps in one
  run is later than the last. A page (the overlay and the control page, through
  `panel-core.js`) ignores a `ticker:update` whose `generatedAt` is strictly
  older than the newest it has applied, compared as dates (`…:00Z` and
  `…:00.500Z` are told apart by time, not as text), when it arrives within
  5 seconds of the moment the page applied that newest one. So a page's
  connect-sync replay that read the stored payload just before the sidecar
  replaced it cannot put the older results back. An older one that arrives
  later than that is applied, and its stamp becomes the newest, so the updates
  after it are judged against it: after a stamp from a clock that was ahead (a
  sidecar restarted after the computer's clock was set back, or a stamp far in
  the future), a page drops only the older updates that arrive within those
  5 seconds, and applies the first one after them by itself, with no refresh.
  An update it dropped stays dropped: the sidecar counted that push as queued
  (its broadcast came), so it does not send it again, and the page keeps what it
  shows until the next update it applies. The 5 seconds are timed on the page's
  own steady clock, so a change of the computer's clock does not move them. An
  update with the same stamp is applied, and so is one with no
  `generatedAt` or one that is not a date, such as the empty update Ticker Push
  replays when nothing is stored; none of these changes the newest stamp or
  starts the 5 seconds again.

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
  spot with no re-poll. The exception is while the current payload is the
  blank, just after a restart or a `setUrl`: see *Nothing kept yet* below.
- It needs no tournament at all. Message edits push **immediately** rather than
  riding the next poll (which, with polling off, would never come), and with
  polling off the sidecar re-pushes a configured message when it connects to
  Streamer.bot, so a message-only setup survives a restart (just after a restart
  that push carries no results: see *Nothing kept yet* below). End-cap changes
  push immediately too, polling or not, except that just after a restart with
  a URL set one fetch runs first (see
  [What the sidecar pushes](#what-the-sidecar-pushes)).
- `ticker:announce` still interrupts it — announcements cover the strip for
  their duration and the message resumes.
- Persisted in `config.json` (`message`), so it survives a sidecar restart
  (unless saving has stopped: see `configWarning`).

## ticker:status

Ephemeral sidecar status for the control page. It must not clobber the
persisted update payload, so it rides the **Ticker Command** relay as the
reserved command `__status` with the status JSON as `value`:

```json
{ "type": "ticker:command", "command": "__status",
  "value": "{\"type\":\"ticker:status\",\"seq\":42,\"polling\":true,\"url\":\"…\",\"platform\":\"challonge\",\"lastPollAt\":\"…\",\"nextPollAt\":\"…\",\"lastError\":\"\",\"commandNotice\":\"\",\"actionNotice\":\"\",\"configWarning\":\"\",\"intervalMs\":30000,\"maxItems\":20,\"caps\":{…}}" }
```

The sidecar ignores its own `__status` echo, in any casing; the control page
unwraps `value`. A status is sent after every poll and after every command, but
one still waiting to go out is replaced by the newer one (see
[The sidecar's sends](#the-sidecars-sends)), so a burst of commands can bring
fewer statuses than commands; each status says what is true when it goes out.
`seq` counts the sidecar's statuses; it is how the sidecar knows the broadcast
of each one, and the control page ignores it.

- `lastError` — why the last poll failed, including the limits below: a site
  that took longer than 10 s to answer, a poll that took longer than 45 s, or an
  answer over 16 MB. The message names the site's host and the limit, never the
  address. A request that could not be made at all reads `<host>: the request
  failed`, with the system's code when there is one (for example `ECONNREFUSED`
  or `ENOTFOUND`), never the address or a header's value. It is `""` after a
  good poll and after a `setUrl` that changes the URL. A `setUrl` with the URL
  already set leaves it as it is, and leaves the count of failed polls in a row
  (which the error back-off is timed from) as it is too; while polling, it asks
  for a poll, as `pollNow` does. A poll of the previous URL that ends after a
  `setUrl` leaves it alone.
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
- `actionNotice` — `"Ticker Push not found"` or `"Ticker Announce not found"`
  (both, joined by ` · `, when both are), when Streamer.bot answered one of the
  sidecar's own sends to that action with "was not found" (any other error
  answer reads `<action>: Streamer.bot answered with an error (<its text>)`);
  `""` otherwise. An ok answer from that action clears it. Only the answer to
  the newest send to an action sets or clears it, late or not: a late answer to
  an older send changes nothing, so an older push's late ok does not clear the
  notice a newer push's refusal set. The control page shows it in its status
  line. "Ticker Command not found" cannot travel here, since the status itself
  goes through `Ticker Command`: the sidecar logs it instead.
- `configWarning` — what is wrong with the sidecar's settings, in fixed words
  that never quote the file or a key; `""` when nothing is. Each of these,
  joined by ` · ` when more than one applies:
  - `config.json is not valid JSON (line N)` (the line is left out when V8 gives
    no position), `config.json could not be read`, `config.json does not hold
    a JSON object`, `config.json is saved as UTF-16; save it as UTF-8` (a file
    that starts with a UTF-16 byte-order mark, `FF FE` or `FE FF`, or without one
    has a NUL as every second byte at its start), `config.json is empty` (no
    bytes, or only white space once a UTF-8 byte-order mark is stripped), or
    `config.json is not saved as UTF-8; save it as UTF-8` (bytes that are not
    valid UTF-8, such as a file saved as ANSI with an accented letter in it; a
    file of plain ASCII saved as ANSI is valid UTF-8, so it loads),
    each followed by `; the sidecar runs on its defaults and saves nothing until the
    file is fixed and the sidecar restarted`. Such a file is never written over,
    and keys from the environment are not used either while it is broken. The
    same tail follows `config.json is missing`, which is said only when there is
    no `config.json` but `config.json.tmp` holds a whole copy of the settings:
    the example is then not copied in, and nothing is saved.
  - `a whole copy of the settings is in config.json.tmp`, after one of the
    sentences above, when the temp file a save writes before renaming it over
    `config.json` was left behind and parses to a JSON object: a crash between
    the write and the rename leaves it, and so does a save whose write in place
    failed (below), which can leave `config.json` cut short. While `config.json`
    is refused the sidecar keeps that file as it is, writes nothing, and says
    nothing about a temp file that is not whole; it removes the temp file at
    start only once `config.json` has loaded, and only when it holds its
    single-instance guard port (one that goes on without it, because the port
    could not be opened, leaves the file alone). To recover, fix or replace
    `config.json` (for example: delete the broken one and rename
    `config.json.tmp` to `config.json`), then restart the sidecar.
  - `<field> in config.json contains a space, a line break or another character
    that cannot be part of a key, so it is not used. Paste it again.` (for
    `challongeApiKey`, `startggApiKey` or `streamElements.jwtToken`), or the same
    for `CHALLONGE_API_KEY (environment variable)` or `STARTGG_API_KEY
    (environment variable)`. A key must be printable ASCII.
  - `config.json: the saved logo was dropped (…)`, for a saved logo that breaks
    the rule under `setCapsLogo` below.
  - `config.json was edited while the sidecar was running, so the sidecar no
    longer saves it; …`: the sidecar reads `config.json` only when it starts, and
    leaves a hand edit made while it runs as it is (a file deleted meanwhile
    counts as edited, and is not made again).
  - `config.json could not be saved (<code>); …`, which the next good save clears.
    When the rename over `config.json` keeps failing (another program holds it),
    the save writes the file in place; if that write fails too, `config.json`
    may be left cut short, so `config.json.tmp`, which holds the settings whole,
    is kept until a save writes `config.json` whole again. A restart before then
    refuses the cut-short `config.json` and names the copy.

  Only a restart clears the others. A good poll and a `setUrl` never clear it.
  The control page shows it in its status line.

A key value in `lastError`, the StreamElements
error, `commandNotice` or `url` (and in the sidecar's log, and in the
`tournament.url` of every `ticker:update` it pushes) is replaced by a label such
as `[Challonge key]`, `[start.gg key]` or `[StreamElements token]`. The sidecar
keeps the URL as it was set, key and all, and so does `config.json`. Only the
keys in `config.json` and the two environment variables are known, and only
at 8 characters or more: a different key pasted inside a URL is not replaced,
and a site's answer that an adapter cut off inside a key can keep its first 1
to 7 characters. A key typed into the caps text, the caps logo or the message
text is replaced by its label too, in the status (`caps` and `message.text`)
and in every `ticker:update` the sidecar pushes (`caps` and `message.lines`).
A payload Streamer.bot stored from an older version is replayed as it was
stored until the sidecar's next push. In a logo address
it is replaced as typed and as a URL encodes it. The sidecar keeps what was
typed, and so does `config.json`. The commands themselves are not changed:
Streamer.bot broadcasts each `ticker:command` to every client of the bus as it
was sent, so a key typed into the caps, the message or the URL still crosses
the bus once, in that command. A caps text cut at 120 characters, a logo cut
at 500, or a message cut at 1000, can keep the first 1 to 7 characters of a
key the cut fell inside. The control page fills its boxes from the status, so they show
the label, and applying them again (the message's checkbox too) saves the
label in place of the key. A box the streamer has typed in is the exception: it
keeps what was typed until a status carries the value its own Apply or Set sent,
so a box holding a key stays as typed, and marked, until Esc or a reload.

The status carries an `auto` block describing the auto-scheduler (see
`setAutoSchedule` below):

```json
"auto": { "enabled": true, "eventState": "upcoming",
  "startAt": "2026-08-01T00:00:00.000Z", "waitingUntil": "2026-07-31T21:15:00.000Z",
  "stopped": null }
```

- `eventState` / `startAt` — what the last successful poll said about the event
  (`startAt` is `null` when the platform has no date; Matcherino never reports
  either).
- `waitingUntil` — set while polling is on and auto start/stop is waiting for an
  upcoming event; the next poll fires at that instant. That is the platform's
  start time when it is near, but never more than 15 minutes after the last poll
  (or one poll interval, if that is longer), so in the example above, with the
  start hours away, it is 15 minutes after the last poll.
- `stopped` — `"completed"` after the scheduler stopped polling itself, `null`
  otherwise.

### When the sidecar polls

- **One poll at a time**, with 45 s in all. Each request in it has 10 s, until
  its whole answer is in, and an answer may be at most 16 MB. A poll that runs
  past a limit is stopped and fails (`lastError`).
- **At least 15 s between polls**, counted from the end of one to the start of
  the next, whatever asks for them. A command that asks for a poll inside that
  gap (`pollNow`, `start`, `setUrl`, or a cap or option change that needs a
  fetch, see [What the sidecar pushes](#what-the-sidecar-pushes)) is answered
  with a status at once and polled when the gap ends, so a burst of commands
  costs one poll. A `pollNow` or `start` that arrives while a poll is under way
  runs after it. A cap or option change does not wait for a poll: it is pushed
  at once from the payload and the last result the sidecar keeps.
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
- **Before an upcoming event** (auto start/stop on), the next poll is at the
  platform's start time, but at most 15 minutes away (or one poll interval, if
  that is longer), so a bracket that starts early is picked up. With no start
  time it is 5 minutes away (or one interval, if that is longer).

### What the sidecar pushes

- **Only a change.** After a poll, the payload is pushed when what the strip
  shows has changed: the tournament's name and state, the caps, the message, the
  standings, and each match's id, state, round, winner, scores and names. So a
  corrected winner with the same scores is pushed, and so is an event that
  completes with no set changing (the stored payload then says `completed`).
- **A new URL** (`setUrl` with a value other than the one set) pushes a blank
  payload at once: no matches and no standings, under the new URL, with the
  caps and the message kept. The old bracket leaves the strip, and stays gone if
  the new URL keeps failing. The new URL's first good poll is pushed. A refused
  `setUrl` pushes nothing, and the URL already set pushes nothing either.
- **Cap changes** (`setCaps`, `setCapsText`, `setCapsLogo`) push the current
  payload with the new caps at once, polling or not, so they show while a site
  is failing (just after a restart there is no current payload yet: see
  *Nothing kept yet* below).
- **Option changes** (`setMaxItems`, `setTopN`, and `setOptions` when one of
  them changes) rebuild the payload from the last good result the sidecar keeps,
  with the URL and platform it was fetched under, and push it at once, polling
  or not. When that result was cut short by its platform (`cutShort`) and the
  change asks for more than it held (a higher `maxItems`, or `topN` switched
  on), the sidecar also fetches again.
- **Nothing kept yet.** After a restart, until its first good poll, the
  sidecar holds no result and, until a message is pushed (below), no payload. A cap or option change with a URL set
  then runs one fetch, which pushes what it brings with the change; with polling
  off that fetch does not start polling, and it waits for the 15 s gap and a
  site's Retry-After like a poll. If that fetch fails, the change reaches the
  strip with the next good one. A change made while such a fetch is under way
  is carried by it, so it costs no second fetch unless that one fails. With no
  URL, an option change is only saved, and a cap change pushes the blank payload
  with the caps (a message-only strip). A message is pushed even with nothing
  kept: with a message on and polling off, the sidecar pushes its payload with
  the message each time it connects to Streamer.bot, so just after a restart it
  pushes the blank payload (no results, the caps kept) with the message. A
  message edit before the first good poll, polling or not, pushes that blank
  payload with the message too. That replaces the results Streamer.bot stored:
  the message shows at once, and from then on the blank is the current payload,
  as it is after a `setUrl` (below). A cap change then pushes the blank with the
  new caps and fetches nothing, and a message switched off leaves the strip with
  no results. The results come back with the next good poll or fetch: `start`,
  `pollNow`, or an option change, which still runs one fetch (see *No blank over
  the stored strip* below). With polling on, nothing is pushed on connect;
  the first good poll pushes the results with the message.
- **Since a new URL,** the blank payload `setUrl` pushed is the current one
  until the new URL's first good poll: a cap change pushes it with the new caps,
  and an option change runs one fetch, as above.
- **No blank over the stored strip.** Apart from `setUrl`'s own blank, a cap or
  option change never pushes a blank payload over the one Streamer.bot stored.
  Message changes are not held back this way: after a restart, before the first
  good poll, a message edit (and, with polling off, the configured message the
  sidecar pushes again when it connects) goes out on a payload with no results,
  which replaces the results Streamer.bot stored.
- **A push that does not get through** (Streamer.bot down or still starting,
  `Ticker Push` missing, or no answer within 5 s) is made good when the sidecar
  next connects to Streamer.bot: it pushes the payload it holds again. While
  the connection stays open (an action added in Streamer.bot does not reconnect
  it), it is pushed again 30 s later, and on the next `status` command, which
  the control page sends every 10 s; until a push is queued, that repeats
  (every 30 s, and on each `status`), and "Ticker Push not found" is still
  logged only once. An answer that comes late, inside the 5 s, counts: the push
  got through (see [The sidecar's sends](#the-sidecars-sends)). One that comes
  after the 5 s does not: that push stays timed out, so with a Streamer.bot that
  takes longer than 5 s to answer, each repeat is timed out too and the repeats
  go on. Only the newest push's outcome counts: once a newer push has been made
  (it carries the whole payload), an older one's outcome changes nothing,
  whether the older one times out while the newer one is queued or still
  waiting for its answer (nothing is owed), or its late ok comes after the newer
  one was refused (the newer one stays owed).

### The sidecar's sends

The sidecar sends one `DoAction` at a time. Streamer.bot answers a `DoAction`
when the action is queued, not when it has run, so the next one goes once the
answer **and** the run's own broadcast are both in (the `ticker:update` with
that `generatedAt`, compared as a time, since Streamer.bot can write a date in
another form, `…:00.120Z` as `…:00.12Z`; the `__status` with that `seq`; the
`ticker:announce` with that kind and text), or after 800 ms. An error answer
lets the next go at once, and a `ticker:error` counts as the end of the run. A
push waiting to go is replaced by a newer push, and a status by a newer status;
announcements are never merged.

Each send ends with one outcome. The 800 ms only let the next send go: a send
that has no answer then, and had neither its own broadcast nor a `ticker:error`
while it was in flight, keeps waiting for its answer, so several can be waiting
at once, and each answer is matched to its own send. A send with no answer that
had one of those two is settled when the next goes.

- **queued** — Streamer.bot answered ok (the action was queued; that says
  nothing of the run), within 5 s of the send, or, with no answer yet, the
  send's own broadcast arrived while it was the send in flight (the run has
  happened).
- **refused** — an error answer, such as "was not found".
- **failed** — a `ticker:error` arrived while it was the send in flight, before
  or after an ok answer, or with no answer at all (it names no sender, so a
  page's failed run at that moment, or the late run of an earlier send, counts
  against it).
- **timed out** — no answer within 5 s of the send, and no broadcast of its own
  while it was the send in flight. Its own broadcast that comes after the next
  send has gone is not looked at, so it does not save the send from timing out.
- **dropped** — the connection was closed when it was to be sent, or while it
  waited to be sent, or while it was the send in flight with no answer and either
  no broadcast of its own or a `ticker:error` (with no answer the error does
  not make it failed: the close comes first), or while it waited for its
  answer.

So a slow Streamer.bot that answers within 5 s neither leaves a push owed nor
repeats a milestone announcement. What follows from an outcome other than
queued: the newest push is owed (see
[What the sidecar pushes](#what-the-sidecar-pushes)), and a milestone's
threshold stays where it was (see
[StreamElements milestones](#streamelements-milestones-sidecar)).

This orders only the sidecar's own sends: the blocking action
queue the README asks for is what keeps a control page's, chat's or a Stream
Deck's runs from mixing with them. The sidecar logs once when an action answers
"was not found" (until it answers ok again; as for `actionNotice`, only the
answer to the newest send to that action counts) and once when an action runs
in Streamer.bot's Default queue (from any ok answer, late or not).

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
| `setUrl`      | tournament URL (empty = none)  | switch tournaments: a blank payload (no results; caps and message kept) is pushed at once, and the new URL's first good poll is pushed (see [What the sidecar pushes](#what-the-sidecar-pushes)). A poll of the old URL still running is stopped and its result thrown away; while polling, the new URL is polled once the gap allows (see [When the sidecar polls](#when-the-sidecar-polls)). An empty value clears the URL, pushes the blank and **stops polling**. The URL already set (after trimming) changes nothing on the strip; while polling it asks for a poll, as `pollNow` does. A refused `stub:` URL (see [Test hook](#test-hook)) keeps the old URL, saves nothing, pushes nothing and says why in `commandNotice` |
| `start`       | —                              | start polling |
| `stop`        | —                              | stop polling |
| `pollNow`     | —                              | a fresh poll as soon as the 15 s gap after the last poll and any Retry-After allow (starts polling if stopped); the status answers at once, with `nextPollAt` |
| `setInterval` | milliseconds (min 15000)       | poll cadence |
| `setCaps`     | JSON `{text,logo}`             | **batched** — sets both end caps in one command (the control page uses this), pushed at once, polling or not (just after a restart with a URL set, after one fetch). A logo `setCapsLogo` would refuse is refused here too: the old logo stays, the text still applies, and `commandNotice` says why |
| `setOptions`  | JSON `{intervalMs,maxItems,topN}` | **batched** — sets all three options in one command. A changed `maxItems` or `topN` rebuilds the payload from the last result and pushes it at once, as `setMaxItems` and `setTopN` do (see [What the sidecar pushes](#what-the-sidecar-pushes)); a new interval only re-times the next poll, so it never cuts an error backoff or a Retry-After short |
| `setMessage`  | JSON `{text,enabled}`          | **batched** — persistent message text + on/off in one command (the control page uses this) |
| `setMessageText` | string (≤1000 chars, `\n`-separated lines) | message text (single-field; for chat / Stream Deck) |
| `setMessageEnabled` | `on`/`off` (also `true`/`false`, `1`/`0`, `yes`) | show the message instead of results |
| `setCapsText` | string (≤120 chars)            | right-cap static text (single-field; for chat / Stream Deck) |
| `setCapsLogo` | an http(s) URL, or a path on the overlay's server (≤500 chars; empty = no logo) | left-cap logo image (single-field). Anything else — `data:`, `file:`, `javascript:`, `//host`, `\\host` — is refused: the old logo stays and `commandNotice` says why. A saved logo that breaks this rule is dropped when `config.json` loads, and `configWarning` says so |
| `setInterval` above, `setMaxItems`, `setTopN` | see below | single-field variants |
| `setMaxItems` | 1–100                          | ticker item cap; a change is rebuilt from the last result and pushed at once (after a fetch, when nothing is kept yet or the URL changed since) |
| `setTopN`     | 0–100 (0 = off)                | only show matches involving the top-N placed participants (needs platform standings — start.gg always; Challonge via `final_rank` on the keyed API for your own tournaments, or derived from elimination order on finished brackets when scraping; also trims the standings crawl to N); a change is rebuilt from the last result and pushed at once (after a fetch, when nothing is kept yet or the URL changed since) |
| `setAutoSchedule` | `on`/`off` (default on)    | auto start/stop: a poll that returns `completed` pushes the final payload then **stops polling**; one that returns `upcoming` **waits for the platform's start time**, checking again at least every 15 min (or every poll interval, if that is longer) so a bracket that starts early is picked up (every 5 min without a start time, or every poll interval if that is longer), and resumes normal polling when the event goes live. `start`/`pollNow` get a poll even during that sleep or after that stop, but it still waits for the 15 s gap after the last poll and for a site's Retry-After (see [When the sidecar polls](#when-the-sidecar-polls)). |
| `status`      | —                              | request a status re-broadcast; a push still owed is pushed first (see [What the sidecar pushes](#what-the-sidecar-pushes)) |

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
> `DoAction`s (one in flight at a time). The connect-sync `Ticker Push` a page
> sends when it connects goes on its own request, not through the page's queue.
> Streamer.bot answers a `DoAction` when the action is queued, not when it has
> run, so a page sends its next request
> only once Streamer.bot's answer and the run's own broadcast are both in (the
> `ticker:command` with the same command and value, or the `ticker:announce`
> with the same kind and text), or after 800 ms. Text is trimmed as the C#
> trims it, which takes U+0085 (next line) as well. An error answer lets the
> next go at once. A broadcast that comes after its request was let go at
> 800 ms belongs to that request, not to the next one alike, for up to 5 s.
> The sidecar's own sends follow the same rule, and also take a `ticker:error`
> as the end of a run; a page does not. The single-field commands remain for
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
don't repeat them (while saving has stopped, see `configWarning`, they are
not saved, and a restart can repeat one). **The JWT never travels the bus** — `ticker:status` carries
only `se: { configured, lastPollAt, lastError }`.

- **A threshold moves on only once its announcement was queued** by
  Streamer.bot. One that could not be sent (Streamer.bot down), was refused
  (`Ticker Announce` missing) or failed is announced again by the next poll, and
  its threshold saved only then. One that got no answer within 5 s is treated
  the same way (a repeat is possible, a loss is not); an ok answer that comes
  later than 800 ms but within the 5 s counts as queued. While one keeps failing,
  the log says so once, not on every poll.
- **A counter that falls** to 0, or to more than one step below the saved
  threshold, is a new session: the threshold is reset silently to the new
  value's, and the milestones above it are announced again as the counter
  climbs back. A dip of a step or less changes nothing, so an unfollow and a
  refollow within one step, or a sub that lapses and renews, never repeat an
  announcement.
- **The subs counter** is read from `subscriber-total`, or from
  `subscriber-count` when that is missing. The key each value came from is saved
  (`milestoneKeys`), and a reading from the other key is recorded silently, as a
  first sighting, not taken as a rise or a drop.
- **Polls never overlap**: each is timed from the end of the last, and two
  milestones crossed in one poll are announced one after the other.

## ticker:error

Broadcast by either C# action when it throws, so failures are visible instead
of silent: `{ "type": "ticker:error", "message": "…" }`. The control page shows
the message in its banner until the next `ticker:command` or `ticker:announce`
broadcast, a status, or a reconnect. It reads a `code` only when one is there.
A `ticker:error` carries no sender, so the page also stops waiting for the
echo of the clicks it has sent: it does not then say they "did not answer".

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

A stub file is a normalized result, so it may also carry what a platform
adapter's result carries: `"cutShort": true` marks a result its adapter stopped
reading early, which an option change asking for more then fetches again (see
[What the sidecar pushes](#what-the-sidecar-pushes)). That is not a test-only key:
no adapter sets it yet (start.gg will, from its paging change).

The tests shorten the poll loop's limits with `ZIPPER_REQUEST_TIMEOUT_MS` (10 s
per request), `ZIPPER_POLL_DEADLINE_MS` (45 s per poll) and
`ZIPPER_MIN_POLL_GAP_MS` (15 s between polls), and the wait before an owed
push is retried with `ZIPPER_PUSH_RETRY_MS` (30 s). Each is clamped, so it can
only shorten its limit, and each works only with `ZIPPER_ALLOW_STUB=1`: without
it the sidecar keeps the full limits (10 s, 45 s, 15 s and 30 s) whatever the
variables say, so no setting on a normal install can make it poll a site more
often, cut a request or a poll short, or retry a missing `Ticker Push` many
times a second. As it starts, the sidecar logs the limits in effect, for
example `limits: 10 s per request, 45 s per poll, 15 s between polls, 30 s
before an owed push is retried`.

`ZIPPER_SE_BASE` points the StreamElements requests (`/kappa/v2/sessions/…`,
with the JWT in their `Authorization` header) at another address instead of
`https://api.streamelements.com`; the tests point it at their StreamElements
mock. It works only with `ZIPPER_ALLOW_STUB=1`: without it the sidecar always
asks `api.streamelements.com`, so no setting on a normal install can send the
token anywhere else.
