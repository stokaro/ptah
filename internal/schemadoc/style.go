package schemadoc

import "go.5x5.cz/ptah/internal/htmlstyle"

// documentCSS is this document's own appearance, on top of the one every Ptah
// HTML page shares.
//
// It is one string rather than a file because the document is self-contained by
// requirement: a stylesheet the page links to is a stylesheet the page fetches.
//
// What is here is what only this page has -- the rail and its navigation, the
// diagram, the enum value list, and how they behave when the window is narrow.
// Colors, type, tables, cards, the count strip, tags and the footer come from
// internal/htmlstyle, so the dashboard, the safety report and the test report
// cannot drift away from them one edit at a time.
const documentCSS = `
.layout { display: grid; grid-template-columns: 240px minmax(0, 1fr); gap: 40px; max-width: 1240px; margin: 0 auto; padding: 0 24px; }
.sidebar { position: sticky; top: 0; align-self: start; max-height: 100vh; overflow-y: auto; padding: 32px 24px 48px 0; border-right: 1px solid var(--border); }
.content { min-width: 0; padding: 32px 0 80px; }

.brand { font-weight: 600; letter-spacing: -.01em; margin-bottom: 2px; }
.brand-sub { color: var(--text-mute); font-family: var(--mono); font-size: 13px; margin-bottom: 22px; }

.nav-group { margin-bottom: 18px; }
.nav-title { font: 500 11px var(--mono); letter-spacing: .08em; text-transform: uppercase; color: var(--text-mute); margin-bottom: 6px; }
.nav a { display: block; padding: 3px 8px; margin-left: -8px; border-radius: var(--radius); color: var(--text-dim); font-family: var(--mono); font-size: 13.5px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.nav a:hover { background: var(--surface-2); color: var(--text); text-decoration: none; }

section[id] { scroll-margin-top: 20px; }
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

@media (max-width: 900px) {
  .layout { grid-template-columns: minmax(0, 1fr); gap: 0; }
  .sidebar { position: static; max-height: none; padding: 24px 0 12px; border-right: 0; }
  .nav { display: flex; flex-wrap: wrap; gap: 6px; }
  .nav-group { margin-bottom: 0; display: contents; }
  .nav-title { display: none; }
  .nav a { margin-left: 0; padding: 5px 10px; border: 1px solid var(--border); }
}
@media print {
  .sidebar { display: none; }
}
`

// stylesheet is what the page carries: the shared appearance, then this
// document's own.
func stylesheet() string {
	return htmlstyle.Tokens() + htmlstyle.Base() + documentCSS
}
