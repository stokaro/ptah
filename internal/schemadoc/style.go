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
const documentCSS = `
:root {
  color-scheme: light dark;
  --bg: #fbfcfd;
  --surface: #ffffff;
  --surface-2: #f4f6f8;
  --border: #dfe4ea;
  --border-strong: #c3cbd4;
  --text: #16202b;
  --text-dim: #5b6b7c;
  --accent: #2f6fed;
  --accent-soft: #e8f0fe;
  --key: #8a5cf6;
  --key-soft: #f1e9fe;
  --link: #2f6fed;
  --shadow: 0 1px 2px rgba(16, 32, 48, .06), 0 8px 24px rgba(16, 32, 48, .05);
  --radius: 10px;
  --mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
  --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #0f1319;
    --surface: #151b23;
    --surface-2: #1b232d;
    --border: #27313d;
    --border-strong: #3a4756;
    --text: #e6edf5;
    --text-dim: #9aa9b9;
    --accent: #6f9dff;
    --accent-soft: #1b2740;
    --key: #b48bff;
    --key-soft: #241d3a;
    --link: #6f9dff;
    --shadow: 0 1px 2px rgba(0, 0, 0, .4), 0 8px 24px rgba(0, 0, 0, .3);
  }
}
:root[data-theme="dark"] {
  --bg: #0f1319;
  --surface: #151b23;
  --surface-2: #1b232d;
  --border: #27313d;
  --border-strong: #3a4756;
  --text: #e6edf5;
  --text-dim: #9aa9b9;
  --accent: #6f9dff;
  --accent-soft: #1b2740;
  --key: #b48bff;
  --key-soft: #241d3a;
  --link: #6f9dff;
  --shadow: 0 1px 2px rgba(0, 0, 0, .4), 0 8px 24px rgba(0, 0, 0, .3);
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
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
code, .mono { font-family: var(--mono); font-size: .9em; }

.layout { display: grid; grid-template-columns: 260px minmax(0, 1fr); gap: 32px; max-width: 1400px; margin: 0 auto; padding: 0 24px; }
.sidebar { position: sticky; top: 0; align-self: start; max-height: 100vh; overflow-y: auto; padding: 28px 0 40px; }
.content { min-width: 0; padding: 28px 0 96px; }

.brand { font-weight: 600; letter-spacing: -.01em; margin-bottom: 4px; }
.brand-sub { color: var(--text-dim); font-size: 13px; margin-bottom: 20px; }

.nav-group { margin-bottom: 18px; }
.nav-title { font-size: 11px; letter-spacing: .08em; text-transform: uppercase; color: var(--text-dim); margin-bottom: 6px; }
.nav a { display: block; padding: 3px 8px; border-radius: 6px; color: var(--text-dim); font-size: 13.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nav a:hover { background: var(--surface-2); color: var(--text); text-decoration: none; }

h1 { font-size: 30px; letter-spacing: -.02em; margin: 0 0 6px; }
h2 { font-size: 20px; letter-spacing: -.01em; margin: 44px 0 14px; padding-bottom: 8px; border-bottom: 1px solid var(--border); }
h3 { font-size: 15px; margin: 22px 0 8px; color: var(--text-dim); font-weight: 600; }
section[id] { scroll-margin-top: 20px; }

.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; margin: 22px 0 8px; }
.stat { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 14px 16px; box-shadow: var(--shadow); }
.stat-n { font-size: 24px; font-weight: 600; letter-spacing: -.02em; }
.stat-l { font-size: 12px; color: var(--text-dim); text-transform: uppercase; letter-spacing: .06em; }

.card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); box-shadow: var(--shadow); margin: 14px 0 28px; overflow: hidden; }
.card-head { display: flex; align-items: baseline; gap: 10px; padding: 14px 18px; border-bottom: 1px solid var(--border); }
.card-head h3 { margin: 0; color: var(--text); font-size: 16px; font-family: var(--mono); }
.card-note { color: var(--text-dim); font-size: 13px; }
.scroller { overflow-x: auto; }

table { border-collapse: collapse; width: 100%; font-size: 13.5px; }
th { text-align: left; font-weight: 600; font-size: 11.5px; letter-spacing: .06em; text-transform: uppercase; color: var(--text-dim); padding: 10px 18px; background: var(--surface-2); white-space: nowrap; }
td { padding: 9px 18px; border-top: 1px solid var(--border); vertical-align: top; }
tbody tr:hover td { background: var(--surface-2); }
td.name { font-family: var(--mono); font-weight: 500; white-space: nowrap; }
td.type { font-family: var(--mono); color: var(--text-dim); white-space: nowrap; }

.tag { display: inline-block; font-size: 11px; line-height: 1.5; padding: 1px 7px; border-radius: 999px; border: 1px solid var(--border-strong); color: var(--text-dim); white-space: nowrap; }
.tag.key { background: var(--key-soft); border-color: transparent; color: var(--key); font-weight: 600; }
.tag.fk { background: var(--accent-soft); border-color: transparent; color: var(--accent); font-family: var(--mono); }
.tag.null { opacity: .75; }

.erd { padding: 18px; overflow-x: auto; }
.erd svg { display: block; max-width: 100%; height: auto; }
.erd .node { fill: var(--surface-2); stroke: var(--border-strong); }
.erd .label { fill: var(--text); font-family: var(--mono); font-size: 12px; }
.erd .edge { stroke: var(--border-strong); fill: none; }
.erd .arrow { fill: var(--border-strong); }

.empty { color: var(--text-dim); font-style: italic; padding: 18px; }
.footer { margin-top: 56px; padding-top: 18px; border-top: 1px solid var(--border); color: var(--text-dim); font-size: 12.5px; }

@media (max-width: 900px) {
  .layout { grid-template-columns: minmax(0, 1fr); gap: 0; }
  .sidebar { position: static; max-height: none; padding-bottom: 12px; }
  .nav { display: flex; flex-wrap: wrap; gap: 4px; }
  .nav a { padding: 3px 10px; background: var(--surface-2); }
}
@media print {
  .sidebar { display: none; }
  .card { break-inside: avoid; box-shadow: none; }
}
`
