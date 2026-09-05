package schemadoc

// documentCSS is the page's whole appearance.
//
// It is one string rather than a file because the document is self-contained by
// requirement: a stylesheet the page links to is a stylesheet the page fetches.
//
// Colors are tokens on :root and are redefined in two places -- under
// prefers-color-scheme for a reader who has never chosen, and under
// [data-theme] for one who has. Neither block introduces a color the light
// block does not already define, so a token can never be missing in one theme
// and present in the other.
//
// The font stack is the reader's own. A web font would be a network request at
// the moment the page opens, which is the one thing this document does not do.
//
// The scale is ptah.run's: 1px rules, 2px radii, no shadows, one accent for
// links and references, and amber -- the color of the mark's capstone -- for
// the key a reader scans a column list to find. It is deliberately quieter than
// the site, because this is a reference document rather than a page selling
// one.
//
// cmd/internal/schemaserve composes this with a stylesheet of its own, so a
// token retired here is a declaration invalidated there. A custom property that
// resolves to nothing reports nothing anywhere, so the two are changed
// together.
const documentCSS = `
:root {
  color-scheme: light dark;
  --bg: #fbfbfa;
  --surface: #ffffff;
  --surface-2: #f3f4f4;
  --border: #d9dcdf;
  --text: #111417;
  --text-dim: #474d54;
  --text-mute: #7c838b;
  --accent: oklch(0.47 0.16 238);
  --amber: oklch(0.62 0.16 60);
  --amber-soft: oklch(0.96 0.04 75);
  --radius: 2px;
  --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
  --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #161311;
    --surface: #1b1815;
    --surface-2: #221e1a;
    --border: #2e2823;
    --text: #ede7de;
    --text-dim: #b6ada0;
    --text-mute: #83796d;
    --accent: oklch(0.78 0.11 225);
    --amber: oklch(0.8 0.15 70);
    --amber-soft: oklch(0.28 0.06 65);
  }
}
:root[data-theme="dark"] {
  --bg: #161311;
  --surface: #1b1815;
  --surface-2: #221e1a;
  --border: #2e2823;
  --text: #ede7de;
  --text-dim: #b6ada0;
  --text-mute: #83796d;
  --accent: oklch(0.78 0.11 225);
  --amber: oklch(0.8 0.15 70);
  --amber-soft: oklch(0.28 0.06 65);
}

* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 15px;
  line-height: 1.6;
  -webkit-font-smoothing: antialiased;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
code, .mono { font-family: var(--mono); font-size: .9em; }

.layout { display: grid; grid-template-columns: 240px minmax(0, 1fr); gap: 40px; max-width: 1240px; margin: 0 auto; padding: 0 24px; }
.sidebar { position: sticky; top: 0; align-self: start; max-height: 100vh; overflow-y: auto; padding: 32px 24px 48px 0; border-right: 1px solid var(--border); }
.content { min-width: 0; padding: 32px 0 80px; }

.brand { font-weight: 600; letter-spacing: -.01em; margin-bottom: 2px; }
.brand-sub { color: var(--text-mute); font-family: var(--mono); font-size: 13px; margin-bottom: 22px; }

.nav-group { margin-bottom: 18px; }
.nav-title { font: 500 11px var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--text-mute); margin-bottom: 6px; }
.nav a { display: block; padding: 3px 8px; margin-left: -8px; border-radius: var(--radius); color: var(--text-dim); font-family: var(--mono); font-size: 13.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nav a:hover { background: var(--surface-2); color: var(--text); text-decoration: none; }

h1 { font-size: 30px; font-weight: 600; letter-spacing: -.02em; line-height: 1.2; margin: 0 0 6px; }
h2 { font-size: 18px; font-weight: 600; letter-spacing: -.01em; margin: 44px 0 14px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
h3 { font-size: 15px; margin: 22px 0 8px; color: var(--text-dim); font-weight: 600; }
section[id] { scroll-margin-top: 20px; }

.lede { font-family: var(--mono); font-size: 13px; color: var(--text-mute); }

.stats { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border: 1px solid var(--border); border-radius: var(--radius); margin: 28px 0 0; }
.stats .stat { padding: 14px 18px; border-right: 1px solid var(--border); }
.stats .stat:last-child { border-right: 0; }
.stat-n { font-size: 24px; font-weight: 600; line-height: 1.2; letter-spacing: -.02em; }
.stat-l { font: 500 11px var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--text-mute); }

.card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); margin: 0 0 20px; overflow: hidden; }
.card-head { display: flex; align-items: baseline; gap: 12px; padding: 12px 18px; border-bottom: 1px solid var(--border); }
.card-head h3 { margin: 0; color: var(--text); font: 600 15px var(--mono); }
.card-note { color: var(--text-mute); font-size: 13px; }
.scroller { overflow-x: auto; }
.scroller + .scroller { border-top: 1px solid var(--border); }

table { border-collapse: collapse; width: 100%; font-size: 13.5px; }
th { text-align: left; font: 500 11px var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--text-mute); padding: 9px 18px; background: var(--surface-2); white-space: nowrap; }
td { padding: 8px 18px; border-top: 1px solid var(--border); vertical-align: top; }
td.name { font-family: var(--mono); font-weight: 500; white-space: nowrap; }
td.type { font-family: var(--mono); color: var(--text-dim); white-space: nowrap; }
td.comment { color: var(--text-dim); }

.tag { display: inline-block; font: 600 11px var(--mono); line-height: 1.6; padding: 1px 6px; border-radius: var(--radius); border: 1px solid var(--border); color: var(--text-dim); white-space: nowrap; }
.tag.key { background: var(--amber-soft); border-color: transparent; color: var(--amber); }
.tag.null { font-weight: 400; color: var(--text-mute); }
.none { color: var(--text-mute); }
.ref { font-family: var(--mono); font-size: 13px; color: var(--accent); white-space: nowrap; }

.values { display: flex; flex-wrap: wrap; gap: 8px; padding: 14px 18px; font-family: var(--mono); font-size: 13.5px; }
.values span { padding: 3px 10px; border: 1px solid var(--border); border-radius: var(--radius); }

.erd { padding: 18px; overflow-x: auto; }
.erd svg { display: block; max-width: 100%; height: auto; }
.erd .node { fill: var(--surface-2); stroke: var(--border); }
.erd .label { fill: var(--text); font-family: var(--mono); font-size: 12px; }
.erd .edge { stroke: var(--text-mute); fill: none; }
.erd .arrow { fill: var(--text-mute); }
.erd-note { font-family: var(--mono); font-size: 12.5px; color: var(--text-mute); margin-top: 12px; }

.empty { color: var(--text-mute); font-style: italic; padding: 18px; }
.footer { margin-top: 56px; padding-top: 18px; border-top: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between; gap: 16px; color: var(--text-mute); font-size: 12.5px; }
.footer-mark { display: inline-flex; align-items: center; gap: 7px; font-family: var(--mono); font-size: 12.5px; flex-shrink: 0; }

@media (max-width: 900px) {
  .layout { grid-template-columns: minmax(0, 1fr); gap: 0; }
  .sidebar { position: static; max-height: none; padding: 24px 0 12px; border-right: 0; }
  .nav { display: flex; flex-wrap: wrap; gap: 6px; }
  .nav-group { margin-bottom: 0; display: contents; }
  .nav-title { display: none; }
  .nav a { margin-left: 0; padding: 5px 10px; border: 1px solid var(--border); }
  .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .stats .stat:nth-child(2n) { border-right: 0; }
  .stats .stat:nth-child(-n+2) { border-bottom: 1px solid var(--border); }
  .footer { flex-direction: column; align-items: flex-start; gap: 8px; }
}
@media print {
  .sidebar { display: none; }
  .card { break-inside: avoid; }
}
`
