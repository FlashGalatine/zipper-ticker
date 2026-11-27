# Theming the Zipper strip

The overlay is a single strip (`overlay/ticker.html`) styled entirely by CSS
custom properties in `overlay/ticker.css`. You never need to touch the JS.

## The variables

```css
:root {
  --ticker-width: 1920px;   /* strip width; ?w=<px> overrides over http */
  --ticker-height: 72px;    /* strip height; ?h=<px> overrides over http */
  --ticker-speed: 80;       /* px per second (unitless); ?speed=<n> overrides */

  --ticker-bg: #101318;     /* strip background */
  --ticker-fg: #e8ecf1;     /* base text */
  --ticker-accent: #ffd166; /* winner names, separators */
  --ticker-live: #ff4d5e;   /* LIVE pip + tag */
  --ticker-muted: #8b94a3;  /* round labels, "Up next" */
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

2. **Edit `ticker.css`** — change the `:root` block; every OBS source picks it
   up on refresh.

3. **URL params** (http-served sources only): `?w=640` width, `?h=48` height,
   `?speed=60` scroll speed. OBS `file://` sources ignore query params — always
   load the overlay through Streamer.bot's HTTP server
   (`http://127.0.0.1:7474/...`). Note the item text sizes off `--ticker-font-size`
   (not the height), so for a very short strip also drop that var via Custom CSS.

## Anatomy

```
[cap-left: logo] [◄◄ scrolling .track: .item .item .item … ►►] [cap-right: text]
```

- Caps render only when the payload's `caps.logo` / `caps.text` are non-empty —
  set them from the control page. The scroll area gets whatever width remains.
- Each `.item` is one match: `.round` label, two `.name`s (winner gets `.win`),
  `.score` or `.vs`, and for live matches `.live-pip` + `.live-tag`.
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
- The right cap's text sits in `.cap-right .cap-text`. It ends in "…" when the
  cap reaches `--ticker-cap-max` (default `40%` of the strip). A logo that does
  not load hides the left cap. An update a minute or more later asks for the
  same logo again, and the cap comes back once it loads.
- `body.offline` is set while Streamer.bot is unreachable — the default theme
  dims the strip; style it however you like.

## Dev loop without Streamer.bot

```
npm start                 # mock SB on :7474 (http) / :8080 (ws)
```

Open `http://127.0.0.1:7474/`, click a ticker link, then hit
`http://127.0.0.1:7474/mock/fixture` to broadcast the sample tournament.
(Streamer.bot already running? Use off-default ports:
`SB_HTTP_PORT=7476 SB_WS_PORT=8082 npm start`.)
Restyle, refresh, repeat. `npm run verify:render` screenshots both widths.
