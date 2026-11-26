# Zipper — tournament ticker for Streamer.bot

A scrolling ticker strip for OBS that shows **live results of an ongoing
tournament** — completed sets, matches in progress, and what's up next —
polled straight from the bracket site. Named after the Times Square news
"zipper."

![1920px ticker](docs/ticker-1920.png)

- **Platforms:** Challonge and start.gg today (API or keyless), Matcherino
  standings; Round.one and TourneyBot planned.
- **Any width:** 1920px broadcast strip by default, `?w=640` for old-school
  layouts — one overlay file.
- **End caps:** optional static logo (left) and streamer-set text like a social
  handle (right); the middle scrolls.
- **Top-N focus:** optionally show only matches involving the top-N placed
  participants (e.g. Top 4) — uses the platform's standings (start.gg always;
  Challonge via `final_rank` on your own keyed tournaments, or derived from the
  elimination order once a scraped bracket finishes).
- **Persistent message:** type a message in the control page and flip a
  checkbox to hold it on the strip instead of results ("bracket starts at 8pm",
  a Discord plug) — one line per ticker item. Results keep polling underneath
  and return the moment you switch it off (one exception, just after a restart
  with polling off, is under *5. Drive it* below), and it works with no tournament set
  at all.
- **Announcements:** subs, raids, milestones, or anything you type interrupt
  the crawl — the strip pauses, shows the alert with a kind pill, and resumes.
  Wire SB's own Twitch triggers to the `Ticker Announce` action for sub/raid
  alerts; milestone announcements poll StreamElements (JWT in `config.json`).
- **Streamer.bot-native:** three SB actions, plus an optional fourth, and a
  small Node sidecar. Overlays
  reconnect and repaint by themselves; last results survive an SB restart.

```
bracket site ──poll── zipper-sidecar.mjs ──DoAction──▶ Streamer.bot ──broadcast──▶ OBS overlay(s)
                             ▲                                                       + control page
                             └──────────── ticker:command (control page) ◀───────────┘
```

## Setup

Requires Node ≥ 20, Streamer.bot ≥ 1.0.4 (tested on 1.0.4 and 1.0.7) with its
**WebSocket Server** enabled
on `:8080` (authentication OFF) and its **HTTP Server** on `:7474`.

First, get the code onto the machine that runs Streamer.bot — either

```
git clone https://github.com/FlashGalatine/zipper-ticker.git
```

or click **<> Code → Download ZIP** on the repo page and extract it.

### 1. Streamer.bot actions

Create these actions — names must match exactly:

| Action name      | Sub-action | Paste |
|------------------|-----------|-------|
| `Ticker Push`    | Core → C# → Execute C# Code | [`actions/ticker-push.cs`](actions/ticker-push.cs) |
| `Ticker Command` | Core → C# → Execute C# Code | [`actions/ticker-command.cs`](actions/ticker-command.cs) |
| `Ticker Announce` | Core → C# → Execute C# Code | [`actions/ticker-announce.cs`](actions/ticker-announce.cs) |
| `Ticker Sidecar Start` *(optional)* | Core → C# → Execute C# Code | [`actions/ticker-sidecar-start.cs`](actions/ticker-sidecar-start.cs) |

Click **Compile** on each — all must report success. `Ticker Sidecar Start`
also needs `System.dll` added on its References tab (see the file's header);
edit its `BUNDLE` path and add SB's application-started trigger to auto-launch
the sidecar with Streamer.bot.

**Put the three bus actions in one blocking queue.** In Streamer.bot's
**Action Queues** tab, add a queue with **Blocking** ticked, then set
`Ticker Command`, `Ticker Push` and `Ticker Announce` to use it
(`Ticker Sidecar Start` stays in the default queue). Streamer.bot can mix up
the arguments of two runs of the same action when they overlap; a blocking
queue runs them one at a time. Measured on Streamer.bot 1.0.7 with a test
action: back-to-back requests mixed their arguments in 25 of 25 tries in the
default queue, 0 of 25 in a blocking queue. The queue covers every sender: the
control page, chat, a Stream Deck, Twitch triggers and the sidecar.

**Restrict who can use the chat command.** If you add a `!ticker` chat command
(see [step 5](#5-drive-it) below), `Ticker Command` does whatever the command passes it
and does not check who typed it. In the command's own settings (Commands tab),
set its permissions so only **moderators** and the **broadcaster** can use it.
Anyone the command allows can change the tournament URL (so someone else's
bracket crawls on your stream), replace the results with any message, change
the end-cap text or show an image from any web address there, and stop
polling. The same applies to every other chat command you point at
`Ticker Command`.

### 2. HTTP maps

Streamer.bot → Servers/Clients → HTTP Server → add two Path→Folder maps
(the `zipper-` prefixes avoid collisions with other components):

| Path | Folder |
|------|--------|
| `zipper-overlay` | `<this repo>\overlay` |
| `zipper-shared`  | `<this repo>\zipper-shared` |

### 3. OBS source

Add a Browser Source:

```
http://127.0.0.1:7474/zipper-overlay/ticker.html?w=1920&h=72
```

Set the OBS source size to match: `?w=` width (default 1920), `?h=` height
(default 72). Use the http URL, not a local file — OBS `file://` sources ignore
`?w=`/`?h=` and friends.

### 4. Sidecar

```
npm install
start-zipper.bat        (or: npm run sidecar)
```

**The sidecar must be running** — it's what turns control-page clicks into
polls and pushes. If it isn't up, the control page shows a red banner and the
Start / Poll / Stop / end-cap buttons do nothing (announcements still work, as
those go Streamer.bot → overlay without the sidecar). The `Ticker Sidecar
Start` action above lets Streamer.bot launch it for you.

Copy `config.example.json` → `config.json` for API keys (optional but
recommended: `challongeApiKey`, `startggApiKey` — without them Zipper falls
back to each site's public/keyless surface, which can be less reliable). The
sidecar copies it for you when `config.json` is missing. A key can also come
from the `CHALLONGE_API_KEY` or `STARTGG_API_KEY` environment variable, used
when `config.json` has none; the sidecar never writes it into `config.json`.
A key in `config.json` wins over the environment's, so a key an older version
of Zipper copied there from the environment overrides the variable until you
remove it from the file.

**Edit `config.json` with the sidecar stopped, or restart the sidecar
afterwards.** It reads the file only when it starts. An edit made while it runs
is not loaded until the restart, and the sidecar leaves it as it is: it stops
saving the file until then (so a setting changed on the control page meanwhile
lasts only until the restart), and from the first save it skips the control
page shows a config warning.

If `config.json` has a mistake in it (a trailing comma, a missing quote), is
saved as UTF-16 (Windows PowerShell's `>` and plain `Out-File` do that), is
empty, or is not saved as UTF-8 (an editor that saves as ANSI does that when
the file has an accented letter in it), the sidecar does not touch the file: it
runs on its built-in settings, without the keys from the file or from the
environment, saves nothing, and the control page shows a warning such as
"config.json is not valid JSON (line 4)", "config.json is saved as UTF-16; save
it as UTF-8", "config.json is empty" or "config.json is not saved as UTF-8;
save it as UTF-8". A file of plain ASCII saved as ANSI loads.
Fix or replace the file, then restart the sidecar. If the warning adds "a whole
copy of the settings is in config.json.tmp", a save was cut short (by a crash,
or by another program locking `config.json` while the sidecar wrote it) and left
that file, holding the settings it was saving. To use it, delete or rename the
broken `config.json`, rename `config.json.tmp` to `config.json`, then restart.
If `config.json` is missing while such a copy is there, the sidecar does not
copy the example in and says "config.json is missing": rename the copy to
`config.json` the same way, or delete `config.json.tmp` to start from the
example, then restart. A key with a space
or a line break inside it is not used either, and the warning names it; paste
it again.

The sidecar writes what it does to `config.log`, beside `config.json`; once that
passes 1 MB it is moved to `config.old.log` and started again. The keys
in `config.json` and in the two environment variables (8 characters or more)
are replaced by a label there, such as `[start.gg key]`. The sidecar saves `config.json` by writing a
new file over it, so a hard or symbolic link to the old file, or permissions set on
the file itself, are not kept. If the sidecar stops by itself, the reason is the last line there.

After upgrading from a version older than 1.0.24, press **Set** or **Poll now**
once. Streamer.bot keeps the last payload it was sent and replays it to the
overlay; one new push replaces a copy stored by an older version, which did not
replace keys by labels.

### 5. Drive it

Open the control page — in a browser or as an OBS custom dock:

```
http://127.0.0.1:7474/zipper-shared/control.html
```

Paste the tournament URL (e.g. `https://challonge.com/blko42` or
`https://www.start.gg/tournament/…/event/…`), **Set**, **Start polling**. **Set**
with a new URL clears the old tournament's results from the strip at once (the
end caps and the message stay), so an old bracket never crawls under a new
event's name; the new results appear with the first good poll. Set
the end-cap text/logo and the persistent message there too; end-cap and option
changes show on the strip at once, even while the bracket site is failing (just
after the sidecar starts, before it has fetched the bracket once, a change waits
for that fetch). The message is the exception: when the sidecar starts with the
persistent message on and polling off, it puts the message on the strip at once,
on a payload with no results, which replaces the results Streamer.bot kept (a
message edit before the first good fetch does the same). From then on an end-cap
change shows at once on that payload, and switching the message off leaves the
strip with no results until the bracket is fetched again: **Start polling**,
**Poll now**, or an option change. Chat control also works — point a `!ticker`
command trigger at the `Ticker Command` action and type
`!ticker setUrl https://…`, `!ticker setTopN 4`, `!ticker stop`, etc.

**Auto start/stop** (the checkbox next to the polling buttons, on by default)
keeps API calls to a minimum: start polling any time — before the event begins
the sidecar waits for the platform's advertised start time, checking again every
15 minutes (or at your poll interval, if that is longer) so a bracket that starts
early is picked up (every 5 minutes if there is no start time, or at your poll interval if that is longer), and once the
event completes it pushes the final results and stops polling by itself. The
results stay on the strip. To
turn it off from chat: `!ticker setAutoSchedule off`. **Start polling** /
**Poll now** get a fresh poll even before the start time or after that stop,
but no poll comes sooner than 15 seconds after the last one (a click inside
that gap is polled when it ends). When a site answers "too many requests" (or
"unavailable") with a time to wait, Zipper waits that long before polling that
site again, **Poll now** included, up to 5 minutes, or the poll interval if
that is longer.

### 6. Persistent message (optional)

Type into the control page's **Persistent message** box — one line per ticker
item — and tick *Show this instead of results*. It holds the strip until you
untick it; results keep polling underneath and come straight back (except just after a
restart with polling off, as described above). No
tournament needs to be set, so it doubles as a plain message ticker. From chat:
`!ticker setMessageText Bracket starts at 8pm ET` then
`!ticker setMessageEnabled on` / `off`.

### 7. Announcements (optional)

- **Sub/raid alerts:** in Streamer.bot, add the `Ticker Announce` action to
  your Subscription / Gift Sub / Raid triggers — it auto-composes the text from
  the trigger arguments ("UserName is raiding with 25 viewers!"), or pass an
  explicit `text` argument to customize.
- **Milestones:** put your StreamElements JWT in `config.json`
  (streamelements.com → account → Channel settings → Show secrets) and set step
  sizes, e.g. `"milestones": { "followers": 100, "subs": 25 }` — the ticker
  announces every time a counter crosses a step. Edit the file with the sidecar
  stopped, or restart it afterwards (see step 4). The token stays in the file;
  it is never broadcast, and never written to the log.
- **Manual:** the control page's Announcement box sends anything you type.

## Platform notes

| Platform | Status | Path |
|----------|--------|------|
| Challonge | ✅ | API (`challongeApiKey`; your own tournaments only — others 404 and auto-fall back) or keyless bracket-embed scrape |
| start.gg | ✅ | API (`startggApiKey`) or keyless public gql proxy¹ |
| Matcherino | ✅ standings-only | public JSON endpoint (no per-match data) |
| Round.one | 🔜 planned | public tournament JSON — match shape research pending |
| TourneyBot | 🔜 planned | `__NEXT_DATA__` scrape — match shape research pending |

¹ The keyless start.gg proxy may reject the sets query; if it 400s, add an API
key (start.gg → developer settings).

## Dev & tests

```
npm start               # mock Streamer.bot (:7474/:8080) + sample data at /mock/fixture
npm run verify          # normalizer fixtures, panel-core, sidecar E2E — no network
npm run verify:render   # real-pixel screenshots at 1920/640 (after npm install)
npm run verify:cs       # compiles the C# actions and runs them (Windows, .NET Framework)
npm run shots           # regenerate docs/ screenshots
```

Docs: [wire protocol](docs/PROTOCOL.md) · [theming](docs/THEMING.md)

## Author & support

Built by **Ashe "Flash" Galatine**.

- Email — [AsheJunius@gmail.com](mailto:AsheJunius@gmail.com)
- X — [@FlashGalatine](https://x.com/FlashGalatine) · BlueSky — [@projectgalatine.com](https://bsky.app/profile/projectgalatine.com)
- Twitch — [FlashGalatine](https://www.twitch.tv/FlashGalatine) · Discord — [Project Galatine](https://discord.gg/K6pRfSvu2Q)
- Support — Patreon [ProjectGalatine](https://www.patreon.com/ProjectGalatine) · CashApp [$ProjectGalatine](https://cash.app/$ProjectGalatine)

## Credits & license

MIT — see [LICENSE](LICENSE). The tournament platform matchers/scrapers are
adapted from the roster importer in [StreamScoreboard](https://github.com/FlashGalatine)
(same author, MIT), extended here to read live match results. Sole runtime
dependency is [ws](https://github.com/websockets/ws) (MIT). See
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
