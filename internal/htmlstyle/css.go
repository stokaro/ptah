package htmlstyle

// tokensCSS is the declaration Tokens returns. See that function for why it is
// three blocks and why the theme blocks introduce nothing.
//
// The severity trio is here rather than in the pages that show it because
// "safe", "warning" and "destructive" mean the same thing on all of them, and
// while each page named its own they meant it in three different greens and
// three different reds.
//
// --amber is the mark's capstone and marks a primary key. It is deliberately
// not --warn: they sit near each other today, and a key is not a warning, so
// folding them would make one of the two impossible to change.
const tokensCSS = `
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
  --ok: oklch(0.52 0.13 155);
  --ok-soft: oklch(0.95 0.04 155);
  --warn: oklch(0.58 0.13 70);
  --warn-soft: oklch(0.95 0.05 75);
  --danger: oklch(0.53 0.19 27);
  --danger-soft: oklch(0.95 0.04 27);
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
    --ok: oklch(0.78 0.14 155);
    --ok-soft: oklch(0.27 0.05 155);
    --warn: oklch(0.8 0.15 70);
    --warn-soft: oklch(0.28 0.06 65);
    --danger: oklch(0.72 0.17 25);
    --danger-soft: oklch(0.28 0.08 25);
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
  --ok: oklch(0.78 0.14 155);
  --ok-soft: oklch(0.27 0.05 155);
  --warn: oklch(0.8 0.15 70);
  --warn-soft: oklch(0.28 0.06 65);
  --danger: oklch(0.72 0.17 25);
  --danger-soft: oklch(0.28 0.08 25);
}
`

// baseCSS is what Base returns: the parts these documents genuinely share.
const baseCSS = `
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
pre { margin: 0; white-space: pre-wrap; font-family: var(--mono); font-size: 13px; }

.page { max-width: 1240px; margin: 0 auto; padding: 32px 24px 80px; }

h1 { font-size: 30px; font-weight: 600; letter-spacing: -.02em; line-height: 1.2; margin: 0 0 6px; }
h2 { font-size: 18px; font-weight: 600; letter-spacing: -.01em; margin: 44px 0 14px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
h3 { font-size: 15px; margin: 22px 0 8px; color: var(--text-dim); font-weight: 600; }
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
td.num { font-family: var(--mono); color: var(--text-mute); text-align: right; width: 1%; }

.tag { display: inline-block; font: 600 11px var(--mono); line-height: 1.6; padding: 1px 6px; border-radius: var(--radius); border: 1px solid var(--border); color: var(--text-dim); white-space: nowrap; }
.tag.key { background: var(--amber-soft); border-color: transparent; color: var(--amber); }
.tag.ok { background: var(--ok-soft); border-color: transparent; color: var(--ok); }
.tag.warn { background: var(--warn-soft); border-color: transparent; color: var(--warn); }
.tag.danger { background: var(--danger-soft); border-color: transparent; color: var(--danger); }
.tag.null { font-weight: 400; color: var(--text-mute); }
.none { color: var(--text-mute); }

.empty { color: var(--text-mute); font-style: italic; padding: 18px; }
.footer { margin-top: 56px; padding-top: 18px; border-top: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between; gap: 16px; color: var(--text-mute); font-size: 12.5px; }
.footer-mark { display: inline-flex; align-items: center; gap: 7px; font-family: var(--mono); font-size: 12.5px; flex-shrink: 0; }

@media (max-width: 900px) {
  .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .stats .stat:nth-child(2n) { border-right: 0; }
  .stats .stat:nth-child(-n+2) { border-bottom: 1px solid var(--border); }
  .footer { flex-direction: column; align-items: flex-start; gap: 8px; }
}
@media print {
  .card { break-inside: avoid; }
}
`
