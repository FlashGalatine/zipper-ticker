# Changelog

All notable changes to **Zipper** are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the patch number
tracks the repository's commit count at release time.

Nothing was tagged between the initial release and 1.0.11, so that release
collects every change in between. Commit hashes and landing dates are noted for
traceability.

## [Unreleased]

## [1.0.34] — 2026-11-28

Documentation only. Nothing to copy, re-paste, restart or reload: the three
action files changed in their comments only, and their code is the same as in
1.0.32.

### Changed

- **`docs/PROTOCOL.md` matches the code again.** It lists all five payload
  types, explains every field of `ticker:status`, says when a status is sent,
  what an empty update holds, how `nextPollAt` is counted, and how a value that
  is not a number is read by `setOptions` and the single-value commands.
- **`docs/THEMING.md`** says which CSS rules the `?w`, `?h` and `?speed` params
  win over, and gives the strip's anatomy and defaults as they are now.
- **`README.md`** says how to find the hidden sidecar in Task Manager, and that
  a milestone can repeat after a restart while `config.json` cannot be saved.
- **The headers of `Ticker Command`, `Ticker Announce` and `Ticker Push`** no
  longer state anything the code does not do. How a Stream Deck action carries
  its arguments is marked as not checked.
- **`docs/control.png` shows the whole control page**, and both strip
  screenshots are regenerated.

### Development

- `npm run verify` compares the docs with the code: status and config names,
  limits, defaults and timings are read from the code, and the docs' examples
  are run through the sidecar's own number reader.
- `npm run shots` takes the control page as a full page.
- `npm run verify` 586 checks (was 579), `npm run verify:render` 181,
  `npm run verify:cs` 45.

### Not checked

- Nothing was looked at in OBS or Streamer.bot.
- The Task Manager steps were not checked on a live Windows screen.
- Following the README from a clean folder is still to be done by the owner.

## [1.0.32] — 2026-11-28

Copy the folder and refresh the OBS overlay source. Then, in Streamer.bot,
re-paste **Ticker Command**, **Ticker Announce** and **Ticker Push**, and only
after `Ticker Announce` compiles add its **Gift Subscription** and **Gift Bomb**
triggers. The sidecar needs no restart and the control dock no reload. Re-pasting
**Ticker Sidecar Start** is optional; if you do, set `BUNDLE` to your Zipper
folder again and keep the `System.dll` reference.

### Changed

- **Chat can send five commands only:** `start`, `stop`, `pollNow`, `status` and
  `setTopN`, in any letter case. Every other command, `setUrl` and the message
  commands included, is refused with one warning in Streamer.bot's log and
  never reaches the sidecar. The control page and a Stream Deck are not
  limited. Before, the sidecar refused them; now the action does, too.
- **A gift sub reads "Gifter gifted a sub to Recipient!"**, and a gift bomb is
  announced once ("Gifter gifted 5 subs!"). The gift subs that belong to a bomb
  announce nothing. An anonymous gift reads "Someone".
- **The strip shows a repeated announcement once.** An announcement identical to
  the one just before it is dropped.
- **An action's error names the action and no longer carries the exception's
  text.** `ticker:error` is `{ type, code, message, action }`; the full error is
  in Streamer.bot's log. See `docs/PROTOCOL.md`.
- **`Ticker Push` refuses a payload that is not a JSON object** with the code
  `bad-payload`.

### Fixed

- **Streamer.bot's log no longer fills with status lines.** An open control page
  wrote two lines every 10 seconds, and every poll two more. A command typed in
  chat is still logged.
- **`Ticker Sidecar Start` no longer carries the author's own folder.** It has a
  placeholder `BUNDLE`, says so when the folder is wrong, does not start a
  second sidecar when one already holds port 7496, and says whether the new one
  came up within about 3 seconds.

### Development

- The mock refuses what the C# refuses and counts it (`refusedCount`);
  `/mock/oldcs?on=1` stands in for an action pasted before this release.
- `npm run verify:cs` compiles the launcher with and without `System.dll` and
  runs a copy of it on side ports.
- `npm run verify` 579 checks (was 568), `npm run verify:render` 181 (was 173),
  `npm run verify:cs` 45 (was 19).

### Not checked

- Nothing was compiled or run in Streamer.bot. The three actions and the
  launcher were compiled with the .NET compilers against a stub.
- Whether Streamer.bot gives `fromGiftBomb` and `gifts` to the action the way
  the stub does. No suite can tell; only a gift sub and a gift bomb in
  Streamer.bot show it.
- A live gift bomb: how its runs arrive, whether its gift subs carry
  `fromGiftBomb`, and what name an anonymous gift has. Test Trigger on Gift
  Subscription gave `fromGiftBomb` as true, so that test shows nothing.
- Streamer.bot's own compile error for the launcher without `System.dll`.
- Nothing was looked at in OBS. The suites ran on Node 22, not Node 20.

## [1.0.30] — 2026-11-27

Copy the folder, restart the sidecar, then reload the control dock (the dock has
a new status line and must match the sidecar). Nothing to re-paste, and the OBS
overlay source needs no refresh.

### Fixed

- **A large start.gg event no longer takes ten requests every poll.** The sets
  are read newest first, 50 at a time, and the reading stops once it holds
  enough finished sets for the strip (**Max ticker items**). With **Top N** on,
  every page is still read.
- **An event with more than 500 sets says so.** When a poll reads all 500 and
  more remain, the control page's status line carries a notice, and the strip
  shows the newest. Before, the older sets were dropped with no word.
- **Top N above 8 on start.gg now has the standings it needs.** The first page
  asks for as many standings as **Top N** (at least 8, at most 100). Raising
  **Top N** above what the last poll asked for fetches the event again at once.
- **A set that finishes between two requests is no longer listed twice.**
- **A start.gg admin page's address works.** The address is kept as you typed
  it; the lookup reads past `/admin`.
- **An address pasted without `https://` gets it**, and the URL box then shows
  it that way.

### Changed

- **A start.gg tournament URL with several events always stops** with "paste a
  specific event URL" and names the events. Before, a single Street Fighter 6
  event among them was picked without asking. A tournament with one event still
  works.
- The status has a new field, `pollNotice`: a fact about the last good poll that
  is not an error. It stays until the next good poll, or until a different URL
  is set. See `docs/PROTOCOL.md`.

### Development

- A result may carry `cutShort`, `standingsAsked` and `pollNotice`; a `stub:`
  file may carry them too. `docs/PROTOCOL.md` says how the sidecar reads each.
- The start.gg query texts are unchanged and pinned by SHA-256 in the suite.
  Only their variables vary.
- The address rule runs in linear time. The rule it was copied from took
  seconds on a very long run of dots.
- `npm run verify` 568 checks (was 506), `npm run verify:render` 173 (was 171),
  `npm run verify:cs` 19.

### Not checked

- The paging was checked once against one large public start.gg event with no
  key: one request with Max items 20, two with 60, ten with Top N 16, every one
  answered. That run was made before the fetch-again rule for Top N was added;
  the rule does not change what is asked for. With an API key and Top N at 100
  it is not checked.
- The Challonge half of this work (addresses with a language or a sub-page in
  them, and the embed page's address for those) is not in this release.
- Challonge's JSON routes and the new bracket API are not in this release: they
  wait on recordings of the live site.
- An address without `https://` typed by hand into `config.json` is not given
  it on load; only a `setUrl` does that.
- Nothing was looked at in OBS. The suites ran on Node 22, not Node 20.

## [1.0.28] — 2026-11-27

Copy the `overlay` folder, then refresh the OBS overlay source. No sidecar
restart and nothing to re-paste. If your OBS source has Custom CSS written for
the old crawl, check it: the loop's keyframes now end at `--seg-w` in px, not at
`-50%`, the track can hold more than two `.seg` copies, and `--dur` is set by
the script for each loop. See `docs/THEMING.md`.

### Fixed

- **New results no longer cut into the crawl.** Before, every update rebuilt
  the strip and the text jumped. Now an update that arrives 20 seconds or less
  before the loop wraps waits for the wrap and scrolls in from the right. One
  that arrives earlier comes in under a short fade (about 0.3 seconds out and
  0.3 back), and the crawl carries on from where it was.
- **The same results pushed again change nothing.** A dock or a second overlay
  connecting no longer restarts the crawl.
- **A font that loads late no longer leaves a gap or a jump** at the wrap: the
  strip is measured again when a font arrives or the strip changes width.
- **The logo is not asked for again on every update.** A logo that fails to
  load hides the left cap; it is tried again after a minute.
- **A long right cap stops at 40% of the strip** and ends in "…". Change the
  share with `--ticker-cap-max`.
- A finished match with no scores and no winner reads "P1 vs P2", not a bare
  dash.
- One empty entry among the matches or standings no longer drops the whole
  update.

### Changed

- **The message switches on and off the way new results come in:** at the wrap,
  or under the short fade. Before, it cut in at once.
- **During an announcement,** results that changed go in under the bar and the
  crawl comes back from its first item. With no change it comes back where it
  paused.
- A theme can lengthen the fade with a transition on `#track`; the swap waits
  for it.

### Development

- `overlay/ticker.js` holds the figures: `FADE_THRESHOLD_MS` 20000, `FADE_MS`
  300, `LOGO_RETRY_MS` 60000. README, `docs/PROTOCOL.md` and `docs/THEMING.md`
  name the 20 seconds, and a check fails when they and the constant differ.
- The render suite sets the animation's time; it does not wait for it. It
  passes with the threshold set to 5 or to 40 seconds, the docs check apart.
- `npm run verify` 506 checks, `npm run verify:render` 171 (was 107),
  `npm run verify:cs` 19.

### Not checked

- Nothing was looked at in OBS. Every check ran in a headless browser with the
  crawl's time set by the test. The 20 seconds is a starting figure until that
  look.
- In real time the place kept by the fade can be off by one frame of crawl.
- The copy of the ticker in the NFC Broadcast kit is unchanged by this release.
- The overlay with a refused `?sbport=` still has no check of its own.
- The suites ran on Node 22, not Node 20.

## [1.0.26] — 2026-11-26

Copy the `zipper-shared` folder, then reload the control dock and refresh the
OBS overlay source. Copy the whole folder. No sidecar restart and nothing to
re-paste. If the dock then says "An older copy of panel-core.js is loaded",
reload it again, and if the line stays, restart OBS. Check the address of the
dock and of the overlay source: a `?sbport=` that is empty or is not a port
from 1 to 65535 used to fall back to 8080 and now connects to nothing.

### Fixed

- **What you type on the control page stays.** Before, the status that arrives
  every few seconds could put the old value back while you typed. Now a box you
  have changed is marked and left alone until the sidecar confirms the value
  you applied. Press Esc in a box to get the current value back.
- **The red banner says what is wrong.** Before, every failure read as "the
  sidecar is not running". Now it tells apart: Streamer.bot offline, an action
  that is missing (named), an action Streamer.bot refused, an action that
  failed, an action that did not answer, and the sidecar being silent.
- **Announce no longer sends twice.** A second click on **Send** while the
  first is on its way sends nothing, and the box clears only when that
  announcement comes back.
- **Clicks made as the connection drops are counted.** The page says how many
  were not sent, and that one in flight may not have run.
- **Reconnects are paced.** A Streamer.bot that accepts a connection and drops
  it again no longer gets a new attempt every 2 seconds for good: the waits
  grow to 15 seconds, with a random part so pages do not all return at once.
- **Clicks no longer overlap.** The page sends one request at a time, so its
  own status check and a click cannot run with each other's values on an
  install without the blocking queue.
- A score in the preview's in-progress row is shown as text, whatever it holds.

### Changed

- **A bad `?sbport=` connects to nothing.** The control page says "The port in
  this page's address was refused"; the overlay stays blank. Fix the number
  and reload.
- The control page says when `panel-core.js` did not load at all (its dot is
  red, not green), and when OBS or the browser kept an older copy of it.
- The control page says when an action is in Streamer.bot's Default queue and
  points to setup step 1.
- A key typed into a box stays shown as typed and marked as an edit until Esc
  or a reload, because the status carries its label and never matches it.

### Development

- `panel-core.js` raises `sb:connection`, `sb:action-queued`,
  `sb:action-error` and `sb:action-timeout`; `__ZIPPER_DO_ACTION` returns the
  request id or false; `window.__ZIPPER_PANEL_CORE` is `{ api: 2 }` and
  `window.__ZIPPER_SB_PORT_REFUSED` is true or false. Its header comments
  match what it does.
- The suite's own panel-core client is closed once its sections are done. Left
  connected, it could add a push to a later section's count (about one run in
  six).
- `npm run verify` 506 checks (was 471), `npm run verify:render` 107 (was 46),
  `npm run verify:cs` 19.

### Not checked

- Nothing was run against a real Streamer.bot or in a real OBS dock. The
  older-copy case used the previous release's file in a test browser, not a
  real OBS cache.
- The status request the page sends right after it subscribes: that
  Streamer.bot 1.0.7 always handles the Subscribe first was not measured.
- The overlay with a refused `?sbport=` has no check of its own.
- The copy of the older `panel-core.js` in the NFC Broadcast kit is unchanged
  until the next release.
- The suites ran on Node 22, not Node 20.

## [1.0.24] — 2026-11-26

Copy `zipper-sidecar.mjs`, the `platforms` folder and the `zipper-shared`
folder, then stop the sidecar and start it again and reload the dock. Copy them
together. Nothing to re-paste. After the restart, press **Set** or **Poll now**
once: Streamer.bot replays the last payload it stored, and one new push replaces
a copy stored by an older version, which did not hide keys. The sidecar now
writes a `config.log` (and later a `config.old.log`) beside `config.json`.

### Fixed

- **A mistake in `config.json` no longer wipes it.** Before, a trailing comma or
  a missing quote made the sidecar start from the example and save that over
  your file, keys and all. Now the file is left alone: the sidecar runs on its
  built-in settings, saves nothing, and the control page says what is wrong,
  for example "config.json is not valid JSON (line 4)". Fix the file and
  restart the sidecar.
- **A file saved in the wrong encoding is named, not mangled.** A `config.json`
  saved as UTF-16 (Windows PowerShell's `>` does that), saved in ANSI with an
  accented letter in it, or left empty, is refused with its own sentence. A
  plain-ASCII file loads however it was saved, and so does a UTF-8 file with a
  byte-order mark.
- **A crash or power loss during a save no longer leaves half a file.** The
  sidecar writes a new file and swaps it in. If a save is cut short and
  `config.json` cannot be read afterwards, the control page says that a whole
  copy of the settings is in `config.json.tmp`; rename it to `config.json` and
  restart.
- **An edit made by hand while the sidecar runs is kept.** Before, the next
  save overwrote it. Now the sidecar notices the edit at its next save, stops
  saving until you restart it, and the control page says so.
- **A key from an environment variable is never written into `config.json`.**
- **API keys no longer appear in errors, the status or the log.** A key the
  sidecar knows is replaced by a label such as `[start.gg key]`. This includes a
  key pasted by mistake into the end-cap text, the logo address or the message,
  which would otherwise show on the stream.
- **A crash leaves its reason behind.** The last line of `config.log` says why
  the sidecar stopped.
- A key with a space or a line break inside it is not used, and the control
  page names it.

### Changed

- While `config.json` is refused, keys from the environment variables are not
  used either. Fix the file and restart.
- A key in `config.json` wins over the environment variable. A key that an
  older version copied from the environment into `config.json` stays there and
  overrides the variable until you remove it from the file.
- Saving replaces `config.json` with a new file, so a hard or symbolic link to
  it, or permissions set on the file itself, are not kept.
- While saving is stopped after a hand edit, reached milestones are not saved,
  so a restart can announce one again.
- If `config.json` is missing while a whole `config.json.tmp` is there, the
  sidecar does not copy the example in; it says "config.json is missing".
  Rename the copy, or delete it to start from the example, then restart.
- The control page's boxes show the label where a key was typed. Applying them
  again saves the label in place of the key.

### Development

- The status gains `configWarning`.
- `ZIPPER_SE_BASE`, `ZIPPER_REQUEST_TIMEOUT_MS`, `ZIPPER_POLL_DEADLINE_MS` and
  `ZIPPER_MIN_POLL_GAP_MS` work only with `ZIPPER_ALLOW_STUB=1`. A log line at
  start names the limits in effect.
- Test preloads in `tools/`: a crash, a guard-socket error and a fake `fetch`,
  so no check contacts an outside host.
- `npm run verify` 471 checks (was 373), `npm run verify:render` 46 (was 45),
  `npm run verify:cs` 19.

### Not checked

- Nothing was run against a real Streamer.bot or in OBS.
- The rename retries and the in-place fallback under a real lock by another
  program (a stand-in held the file), and a file saved as ANSI by a real editor.
- A clean shutdown while a save is in flight does not wait for it.
- A site's own error text that an adapter cut off inside a key, or a cap or
  message text cut at its length limit inside a key, can keep the key's first 1
  to 7 characters.
- The suites ran on Node 22, not Node 20.

## [1.0.22] — 2026-11-26

Copy `zipper-sidecar.mjs`, the `platforms` folder and the `zipper-shared`
folder, then stop the sidecar and start it again, reload the dock, and refresh
the OBS browser source. Copy them together. Nothing to re-paste.

### Fixed

- **A push made while Streamer.bot was unreachable is no longer lost.** Before,
  results fetched during a disconnect never reached the strip unless they
  changed again. The sidecar now remembers that a push is owed and sends it
  when it reconnects, when the dock asks for the status, and every 30 seconds
  until it goes through.
- **End-cap, logo and option changes show at once.** Before, with polling
  stopped or the bracket site failing, a new cap text, logo, item limit or
  top-N waited for the next good poll, which might never come. The sidecar now
  keeps the last result and rebuilds the strip from it.
- **Two milestones crossed in one check both show.** The sidecar now sends one
  thing at a time to Streamer.bot and waits for it to finish.
- **A corrected winner is pushed.** Before, a change to the winner, the round
  or the tournament's state with the scores unchanged was treated as "nothing
  changed".
- **A follower or sub count that dips and comes back no longer repeats a
  milestone,** and a count that falls well below the last milestone starts
  again from there quietly. A milestone announcement that Streamer.bot did not
  accept is tried again instead of being skipped.
- **A bracket that starts early is picked up.** Before an upcoming event the
  sidecar checks again at least every 15 minutes (or at your poll interval, if
  that is longer) instead of sleeping up to six hours.
- **An out-of-date update no longer replaces a newer one on the strip or in the
  dock preview.** If the computer's clock is set back, the strip follows again
  by itself after 5 seconds.

### Changed

- **Setting a new tournament URL clears the strip at once,** keeping the end
  caps and the message, so the old bracket never shows under the new address.
  Setting the same URL again changes nothing.
- **A missing Streamer.bot action is named on the control page.** If
  `Ticker Push` or `Ticker Announce` is not found, or answers with an error,
  the dock says so. A slow Streamer.bot that answers within 5 seconds is not
  treated as a failure.
- Before an upcoming event Zipper can make up to four requests an hour at the
  default settings, where it made one or none.
- With the persistent message on and polling off, the sidecar puts the message
  on the strip each time it starts. Just after a start that update carries no
  results, so switching the message off then leaves the strip empty until you
  press **Start polling** or **Poll now**.

### Development

- `mock-sb-server.mjs` can hold an action's reply (`/mock/delay`), for tests.
- `ZIPPER_PUSH_RETRY_MS` shortens the 30-second retry, only with
  `ZIPPER_ALLOW_STUB=1`.

### Not checked

- Nothing here was run against a real Streamer.bot or in OBS: slow and missing
  replies were tested against the mock only.
- How StreamElements' counters really behave when a count falls.

## [1.0.20] — 2026-11-25

Copy `zipper-sidecar.mjs` and the whole `platforms` folder, which has a new
file (`guarded-fetch.js`), then stop the sidecar and start it again. Copy both
together: an old sidecar with the new `platforms` files, or the other way
round, will not start correctly. Nothing to re-paste.

### Fixed

- **Changing the tournament URL during a poll no longer shows the old bracket
  under the new address.** Before, a poll already under way could finish after
  the change and put the old tournament's results on the strip with the new
  URL, and an old tournament that had ended could stop polling for the new one.
  The change now stops the poll in flight, and a result that comes back late is
  thrown away.
- **A tournament site that hangs no longer freezes polling in silence.** A
  single request gives up after 10 seconds and a whole poll after 45, with a
  line on the control page such as "www.start.gg took longer than 10 s to
  answer". An answer larger than 16 MB is refused. A **Poll now** sent while a
  poll is stuck runs afterwards; before, it was dropped.
- **A failing site is never polled more often than your interval.** With an
  interval longer than five minutes, the old back-off polled a failing site
  every five minutes.
- **"Too many requests" is respected.** When a site answers that way and says
  how long to wait, Zipper waits that long, up to five minutes or your interval
  if that is longer. **Poll now** does not cut the wait short; it applies per
  site, so a tournament on another site is polled as usual.

### Changed

- **A poll runs no sooner than 15 seconds after the last one ended.** Poll now,
  Start, a new URL and, while polling, a caps or options change all still
  answer at once, but the poll itself waits out those 15 seconds. A burst of
  clicks or chat commands costs one poll. While polling, a caps or logo change
  can therefore take up to 15 seconds to reach the strip, or as long as a
  site's "too many requests" wait.
- Error lines name the site and what went wrong (a timeout, the size limit, a
  connection that could not be made), and never the full address, which can
  carry an API key.
- The sidecar's status has a new field, `nextPollAt`, the time of the next
  poll. The control page does not show it yet.
- Zipper identifies itself to tournament sites with its real version.
- Poll timing no longer depends on the computer's clock, so a clock correction
  cannot stall polling.

### Development

- Three test-only switches work through `stub:` URLs, and only with
  `ZIPPER_ALLOW_STUB=1`: a poll that waits, a poll that fails with a chosen
  answer, and a held poll that ignores being stopped. `docs/PROTOCOL.md`
  describes them.

### Not checked

- No tournament site was contacted. The request limits are tested against a
  local server that never answers, and each adapter's wiring is checked
  in-process.
- The 16 MB limit is not checked against a real full-size bracket page.

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

[Unreleased]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.34...main
[1.0.34]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.32...v1.0.34
[1.0.32]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.30...v1.0.32
[1.0.30]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.28...v1.0.30
[1.0.28]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.26...v1.0.28
[1.0.26]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.24...v1.0.26
[1.0.24]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.22...v1.0.24
[1.0.22]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.20...v1.0.22
[1.0.20]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.18...v1.0.20
[1.0.18]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.16...v1.0.18
[1.0.16]: https://github.com/FlashGalatine/zipper-ticker/compare/v1.0.14...v1.0.16
[1.0.14]: https://github.com/FlashGalatine/zipper-ticker/compare/dbdb789...v1.0.14
[1.0.11]: https://github.com/FlashGalatine/zipper-ticker/compare/d77a27f...dbdb789
