# Theming the Zipper strip

The overlay is a single strip (`overlay/ticker.html`) styled by
`overlay/ticker.css`. Most of the look (colours, font, sizes, speed) comes from
the CSS custom properties below. The rest is fixed in its rules: the paddings,
the font weights and size ratios, the ◆ separator, and the announcement pill's
text colour (`#14171c`, or `#fff` on a raid). To change those, override the rule
itself (`.item`, `.item::after`, `.announce-wrap .pill`). You never need to touch
the JS.

## The variables

```css
:root {
  --ticker-width: 1920px;   /* strip width; ?w=<px> overrides over http */
  --ticker-height: 72px;    /* strip height; ?h=<px> overrides over http */
  --ticker-speed: 80;       /* px per second (unitless); read from :root only; ?speed=<n> overrides */

  --ticker-bg: #101318;     /* strip background */
  /* --ticker-announce-bg: not set by default; announcement bar background. Unset, the bar uses --ticker-bg */
  --ticker-fg: #e8ecf1;     /* base text */
  --ticker-accent: #ffd166; /* winner names, ◆ separators, standings places, sub + milestone pill */
  --ticker-live: #ff4d5e;   /* LIVE pip (and its ring) + tag, raid pill */
  --ticker-muted: #8b94a3;  /* round labels, "Up next", "vs"/"def.", the "waiting for results…" line, the default "Announcement" pill's background */
  --ticker-message-fg: #e8ecf1; /* persistent message text */
  --ticker-cap-bg: #1b2330; /* end caps */
  --ticker-cap-fg: #ffffff;
  --ticker-edge: #2a3342;   /* top border + cap dividers */
  --ticker-font: 'Segoe UI', system-ui, sans-serif;
  --ticker-font-size: 20px;
  --ticker-cap-max: 40%;    /* right cap's widest, as a share of the strip */
}
```

## Three ways to re-skin

1. **OBS Custom CSS** (per-source, no files touched) — paste overrides into the
   browser source's *Custom CSS* box:

   ```css
   :root { --ticker-bg:#12031a; --ticker-accent:#00ffc8; --ticker-live:#ff2bd6;
           --ticker-font:'Press Start 2P', monospace; --ticker-font-size:14px; }
   ```

   A font named here must be installed on the PC, or loaded by the Custom CSS
   itself: an `@import` as its first line, or an `@font-face` rule. Otherwise
   the strip falls back to the next font in the list (`monospace` above). A font
   that arrives after the crawl has started has it measured again: the copies
   are swapped in under the same `.track.swap-fade` fade as new results (see
   Anatomy), and the crawl keeps its place.

2. **Edit `ticker.css`** — change the `:root` block; every OBS source picks it
   up on refresh.

3. **URL params** (http-served sources only): `?w=640` width (200 or more),
   `?h=48` height (20 or more), `?speed=60` scroll speed (more than 0). A value
   under its limit, or one that is not a number, is ignored with no message. The
   params are set as inline style on the page's root element, so they win over
   the same variables set on `:root` in Custom CSS or `ticker.css`, unless that
   rule is marked `!important`. The root element only passes them down: a rule
   that sets `--ticker-width` or `--ticker-height` on `.zipper`, where ticker.css
   reads them, wins over `?w` and `?h`. A page that loads `ticker.js` can set
   `window.__TICKER_WIDTH`,
   `window.__TICKER_HEIGHT` and `window.__TICKER_SPEED` before it runs, in place
   of the params. They take the same limits. A param that is a number other than
   0 wins over them, even one under its limit, which then leaves the CSS value.
   `?sbport=8082` connects to a Streamer.bot WebSocket port other than 8080:
   digits giving 1 to 65535. Any other value, an empty one too, connects to
   nothing. The dev loop's links below add it for you.

   OBS `file://` sources ignore query params, and the overlay loads
   `/zipper-shared/panel-core.js` from the server's root: over `file://` that
   path points at the root of the disk, not at this repo's `zipper-shared`
   folder, so the strip never connects and stays on "waiting for results…".
   Always load the overlay through Streamer.bot's HTTP server
   (`http://127.0.0.1:7474/zipper-overlay/ticker.html?w=1920&h=72`).

   All the text sizes off `--ticker-font-size` (not the height), so for a very
   short strip also lower that variable in Custom CSS. Only the left cap's logo
   follows the height: it is at most the strip's height less 24 px tall and
   220 px wide. So at a height of 24 px or less (`?h=20` to `?h=24`; `?h` below
   20 is ignored) the logo shrinks to nothing and the left cap shows empty.

## Anatomy

```
.zipper
  [.cap.cap-left: img] [.track-wrap ◄◄ .track: .seg .seg … (each .item .item …) ►►] [.cap.cap-right: .cap-text]
                        .track-wrap also holds .announce-wrap (.pill + .text), laid over .track
```

- `.zipper` is the whole strip. `.track-wrap` is the scrolling area between the
  caps: it clips the crawl, and holds `.track` and the announcement bar (below).
- A cap shows only while ticker.js gives it `.on`: `.cap` is `display: none` and
  `.cap.on` is `display: flex`. ticker.js sets `.on` while the payload's
  `caps.logo` / `caps.text` is not blank (spaces alone count as blank), and on the
  left cap only while its logo has not failed to load (see the logo note below).
  Set them from the control page. A theme that changes `display` should do it on
  `.cap.on`: a `display` set on `.cap` alone shows the caps when they are empty.
  The scroll area (`.track-wrap`) gets whatever width remains.
- Each `.item` is one match. Live matches start with `.live-pip` + `.live-tag`.
  Then comes a `.round` label (on a completed or live match it is left out when
  the match has no round; on an upcoming match it reads "Up next" or
  "Up next · <round>"), then two `.name`s ("TBD" for a missing player; the
  winner of a completed match gets `.win`) with `.score` or `.vs` between them.
  `.vs` reads "vs", or "def." on a completed match that has a winner but a score
  missing or below 0 (a DQ or forfeit): the winner's name then comes first.
- Every `.item` ends in a ◆, including standings, message lines and the static
  "waiting for results…" line. The ◆ is `.item::after`, in `--ticker-accent`:
  restyle or remove the separator there.
- Standings items use `.place` + `.name`.
- In persistent message mode each configured line is an `.item.message` holding
  a single `.msg` span — style it via `--ticker-message-fg`, or reach for
  `.item.message` directly for a fuller treatment (background, size, casing).
- The marquee repeats the items until one copy of them (a `.seg`) is as wide as
  the scrolling area, and puts copies side by side on `.track`: two, or more
  when the scrolling area grows later. The `ticker-scroll` animation moves
  `.track` left by one copy's width in px (`--seg-w`), over that width ÷
  `--ticker-speed` seconds (`--dur`), and then starts again. So the speed stays
  the same however many results are showing.
- `ticker.js` sets `--seg-w` and `--dur` on `.track`, and starts a new animation
  each time they change. Leave both to it, and keep the `ticker-scroll`
  animation on `.track`: new results wait for its loop to wrap.
- New results never cut into the crawl. When the wrap is 20 seconds away or
  less, they wait for it. Then the copies off screen take them, so they scroll
  in from the right, and at the next wrap every copy holds them. When the wrap
  is further off, `.track` fades out (`.track.swap-fade`, opacity 0 over 0.3 s),
  takes them and fades back in. The crawl keeps its place under the fade: the
  new animation starts where the old one had got to (modulo the new copy's
  width), so results that come often never keep the end of the list out of view.
  A theme may give `.track` a longer opacity transition: the swap waits for it.
  Results equal to those on screen change nothing. Custom CSS written for the
  older crawl, which moved `.track` by `-50%` with exactly two `.seg`, may need
  changing.
- The crawl is also measured again when the items or the strip's middle change
  width: a web font that arrives late, or a logo or cap that changes size. When
  the copies change width, `.track` takes the same fade with no new results: the
  copies are built again under it, with any results that were waiting, and the
  crawl keeps its place. While an announcement hides the strip this happens at
  once, with no fade. When the middle grows wider than the copies, more copies
  (up to ten in all) are added at once at the end, off screen. At the end of
  that loop every copy is built again for the wider middle, and the loop starts
  again from its start with the same items in view.
- With nothing to crawl (the first paint, or an update with no message line,
  match or standing to draw), `.track` gets the class `static` and holds a single
  `.item`, with no `.seg` around it: "waiting for results…", or
  "<tournament name> — waiting for results…" when the payload names the
  tournament. The default theme draws it in `--ticker-muted` (its ◆ stays
  `--ticker-accent`) and takes the animation off
  (`.track.static { animation: none }`). It goes on at once, and the first crawl
  after it also comes in at once, from the start of its loop. `static` comes off
  before that crawl is measured.
- The right cap's text sits in `.cap-right .cap-text`. It ends in "…" when the
  cap reaches `--ticker-cap-max` (default `40%` of the strip). A logo that does
  not load hides the left cap. An update a minute or more later asks for the
  same logo again, and the cap comes back once it loads.
- An announcement (`ticker:announce`) adds `.announcing` to `.zipper`.
  `.announce-wrap` then covers the scrolling middle: not the caps, and under the
  same 24 px edge fade. It has no padding, so a line as wide as the bar runs
  under the fade at both ends, its "…" too. It holds a `.pill` and a `.text`
  (trimmed, cut to 200 characters, and ending in "…" if it still does not fit).
  Its background is `--ticker-announce-bg`, or `--ticker-bg` when that is not
  set. `.announce-wrap` carries `data-kind` = `sub`, `raid`, `milestone` or
  `custom` (any other kind shows as `custom`). The pill reads New Sub, Raid,
  Milestone or Announcement (uppercased by the CSS) on `--ticker-muted`, or on
  `--ticker-accent` for sub and milestone and `--ticker-live` for raid. Its text
  colour is fixed in ticker.css (#14171c, white on raid). To change it, restyle
  `.announce-wrap .pill`, and for raid `.announce-wrap[data-kind="raid"] .pill`
  as well, since that rule outranks the first. The bar fades in as it rises
  into place (`announce-in`, 0.35 s), and the pill pulses (`pill-pulse`). When
  its time is up (2 to 30 s, default 8) the bar goes at once. If another
  announcement is waiting, that one's pill and text take its place at once
  instead, and `announce-in` does not play again. Under the bar `.track` is
  hidden and its animation paused. With nothing new, the crawl goes on where it
  paused. Results that arrive while the bar is up, or that were waiting for the
  wrap or the fade, go in at once, and the loop starts again from its start.
- `.track-wrap` has an edge fade: a mask (`mask-image` and `-webkit-mask-image`)
  fades the first and last 24 px of the scrolling area out to transparent at the
  ends. At a wrap, ticker.js counts the copy after the first as off screen while
  its left edge is no more than 24 px inside the right edge, and puts new results
  into it and the copies after it. Nothing in view changes because each copy
  starts with an `.item`'s left padding (30 px by default), which is empty. The
  mask only fades those pixels, it does not hide them. So keep at least 24 px of
  left padding on `.item`, and with no mask the new results' first pixels show
  at full strength if you do not.
- `body.offline` is set while Streamer.bot is unreachable — the default theme
  dims the strip; style it however you like.

## Dev loop without Streamer.bot

```
npm start                 # mock SB on :7474 (http) / :8080 (ws)
```

Open `http://127.0.0.1:7474/`, click a ticker link, then hit
`http://127.0.0.1:7474/mock/fixture` to broadcast the sample tournament.
To style the announcement bar, hit
`http://127.0.0.1:7474/mock/announce?text=Hello&kind=raid&duration=8`
(kind `sub`, `raid`, `milestone` or `custom`, where any other kind shows as
`custom`; duration 2 to 30 seconds, 8 by default), or send one from the
Announcement box on the landing page's control.html link.
(Streamer.bot already running? Start the mock on other ports: in cmd
`set SB_HTTP_PORT=7476&& set SB_WS_PORT=8082&& npm start`, in PowerShell
`$env:SB_HTTP_PORT=7476; $env:SB_WS_PORT=8082; npm start`, in Git Bash
`SB_HTTP_PORT=7476 SB_WS_PORT=8082 npm start`. Then use 7476 in place of 7474
in the addresses above (`http://127.0.0.1:7476/`,
`http://127.0.0.1:7476/mock/fixture`); the landing page's links already add
`?sbport=8082`.)
Restyle, refresh, repeat. `npm run verify:render` runs the render checks in a
real browser against a mock of its own on 7476/8082 and writes
`test-render-1920.png` and `test-render-640.png`, among other screenshots. It
stops at once, testing nothing, when anything already answers on those ports, so
stop a mock you started there first. `npm run verify` and `npm run shots` stop
the same way.
