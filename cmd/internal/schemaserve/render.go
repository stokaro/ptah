package schemaserve

import (
	"fmt"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/schemadoc"
	"go.5x5.cz/ptah/migration/safety"
)

// dashboardCSS is what this view adds to the document's own appearance.
//
// It is an addition rather than a second stylesheet: the schema sections below
// the status panel are rendered by internal/schemadoc and styled by its tokens,
// so this defines no color of its own -- the three severities included, which
// it used to spell as three hex literals -- and only arranges what the panel
// needs.
//
// Every var() here has to name a token internal/schemadoc still declares. A
// custom property that resolves to nothing invalidates the declaration it sits
// in and reports nothing -- no console message, no failing test, no visibly
// broken page -- so this moves when that stylesheet's tokens move.
// TestHandler_ResolvesEveryCustomPropertyItUses is what makes that true rather
// than remembered.
const dashboardCSS = `
.status { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 22px 0 6px; }
.status .stat { background: var(--surface); border: 1px solid var(--border); border-left: 3px solid var(--border); border-radius: var(--radius); padding: 14px 18px; }
.status .stat.safe { border-left-color: var(--ok); }
.status .stat.warning { border-left-color: var(--warn); }
.status .stat.destructive { border-left-color: var(--danger); }
.banner { border: 1px solid var(--border); border-left: 3px solid var(--danger); background: var(--surface); border-radius: var(--radius); padding: 14px 18px; margin: 18px 0; }
.banner-title { font-weight: 600; margin-bottom: 4px; }
.banner-body { color: var(--text-dim); font-size: 13.5px; font-family: var(--mono); word-break: break-word; }
.stamp { color: var(--text-mute); font-size: 12.5px; margin-top: 6px; }
`

// render writes the whole page: the live panel this view adds, then the schema
// as internal/schemadoc renders it everywhere else.
func (s *server) render(current observation) string {
	var out strings.Builder
	title := s.opts.Title
	if title == "" {
		title = "Schema dashboard"
	}

	out.WriteString("<!doctype html>\n<html lang=\"en\">\n<head>")
	out.WriteString(`<meta charset="utf-8">`)
	out.WriteString(`<meta name="viewport" content="width=device-width, initial-scale=1">`)
	if seconds := int(s.opts.Refresh.Seconds()); seconds > 0 {
		// A meta refresh rather than a script: this page carries no JavaScript,
		// for the same reason the exported document carries none.
		fmt.Fprintf(&out, `<meta http-equiv="refresh" content="%d">`, seconds)
	}
	fmt.Fprintf(&out, "<title>%s</title>", escape(title))
	fmt.Fprintf(&out, "<style>%s%s</style>", schemadoc.Stylesheet(), dashboardCSS)
	out.WriteString("</head>\n<body><div class=\"layout\">")

	out.WriteString(`<aside class="sidebar">`)
	fmt.Fprintf(&out, `<div class="brand">%s</div>`, escape(title))
	fmt.Fprintf(&out, `<div class="brand-sub">%s</div>`, escape(shortURL(s.opts.DatabaseURL)))
	out.WriteString(sidebarOf(current))
	out.WriteString(`</aside><main class="content">`)

	fmt.Fprintf(&out, `<h1>%s</h1>`, escape(title))
	writeStatus(&out, current)
	if current.Schema != nil {
		out.WriteString(current.Schema.Content)
	}
	out.WriteString(`<div class="footer">Read-only. This view compares the declared schema with the live database and changes neither.</div>`)
	out.WriteString(`</main></div></body></html>`)
	return out.String()
}

// sidebarOf returns the schema navigation when there is a schema to navigate.
func sidebarOf(current observation) string {
	if current.Schema == nil {
		return ""
	}
	return current.Schema.Sidebar
}

// writeStatus writes the part that is only true right now.
func writeStatus(out *strings.Builder, current observation) {
	if current.Err != nil {
		// The failure is named rather than rendered as zero drift. A dashboard
		// that showed "no differences" when it could not reach the database
		// would be worse than one showing nothing at all.
		out.WriteString(`<div class="banner"><div class="banner-title">The database could not be compared</div>`)
		fmt.Fprintf(out, `<div class="banner-body">%s</div></div>`, escape(current.Err.Error()))
		writeStamp(out, current, "last attempt")
		return
	}

	out.WriteString(`<div class="status">`)
	writeStat(out, len(current.Findings), "differing categories", severityClass(current.Highest))
	writeStat(out, countOf(current.Findings, safety.Destructive), "destructive", "destructive")
	writeStat(out, countOf(current.Findings, safety.Warning), "warning", "warning")
	writeStat(out, countOf(current.Findings, safety.Safe), "safe", "safe")
	out.WriteString(`</div>`)

	if len(current.Findings) == 0 {
		out.WriteString(`<div class="card"><div class="empty">The database matches the declared schema.</div></div>`)
	} else {
		writeFindings(out, current.Findings)
	}
	writeStamp(out, current, "compared")
}

func writeStat(out *strings.Builder, value int, label, class string) {
	fmt.Fprintf(out, `<div class="stat %s"><div class="stat-n">%d</div><div class="stat-l">%s</div></div>`,
		escape(class), value, escape(label))
}

func writeFindings(out *strings.Builder, findings []safety.Finding) {
	out.WriteString(`<div class="card"><div class="card-head"><h3>Drift</h3></div>`)
	out.WriteString(`<div class="scroller"><table><thead><tr><th>Category</th><th>Objects</th><th>Severity</th></tr></thead><tbody>`)
	for _, finding := range findings {
		out.WriteString(`<tr>`)
		fmt.Fprintf(out, `<td class="name">%s</td>`, escape(finding.Category))
		fmt.Fprintf(out, `<td>%d</td>`, finding.Count)
		fmt.Fprintf(out, `<td><span class="tag">%s</span></td>`, escape(string(finding.Severity)))
		out.WriteString(`</tr>`)
	}
	out.WriteString(`</tbody></table></div></div>`)
}

// writeStamp says when, because a live view whose age is unknown is not a live
// view. A page that stopped refreshing looks identical to one that just did.
func writeStamp(out *strings.Builder, current observation, verb string) {
	fmt.Fprintf(out, `<div class="stamp">%s %s</div>`,
		escape(verb), escape(current.At.UTC().Format(time.RFC3339)))
}

func countOf(findings []safety.Finding, severity safety.Severity) int {
	total := 0
	for _, finding := range findings {
		if finding.Severity == severity {
			total++
		}
	}
	return total
}

func severityClass(severity safety.Severity) string {
	switch severity {
	case safety.Destructive:
		return "destructive"
	case safety.Warning:
		return "warning"
	default:
		return "safe"
	}
}

// shortURL is the database a reader is looking at, without the credentials that
// reach it. A dashboard that printed a password would put it in every
// screenshot of itself.
func shortURL(dbURL string) string {
	scheme, rest, found := strings.Cut(dbURL, "://")
	if !found {
		return dbURL
	}
	if _, host, hasCredentials := strings.Cut(rest, "@"); hasCredentials {
		return scheme + "://" + host
	}
	return dbURL
}
