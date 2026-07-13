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
  --ticker-cap-bg: #1b2330; /* end caps */
  --ticker-cap-fg: #ffffff;
  --ticker-edge: #2a3342;   /* top border + cap dividers */
  --ticker-font: 'Segoe UI', system-ui, sans-serif;
  --ticker-font-size: 20px;
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
- The marquee duplicates its content (`.seg` twice) and animates
  `translateX(0 → -50%)`; duration is computed as segment-width ÷
  `--ticker-speed`, so the apparent speed stays constant regardless of how many
  results are showing.
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
