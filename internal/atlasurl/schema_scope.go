package atlasurl

import (
	"net/url"
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// searchPathParam is the PostgreSQL-family query parameter that restricts a
// database URL to a single schema.
const searchPathParam = "search_path"

// SchemaScope returns the one schema a database URL restricts schema analysis
// to, or the empty string when the URL puts every schema of the connected
// database under review.
//
// This is the boundary `migrate lint` reviews within: the dev database snapshot
// a lint run diffs against covers exactly this scope, so an object a migration
// creates or destroys outside it was never in the before-state and its
// destruction is not a covered change.
//
// Only the PostgreSQL-family `search_path` form is recognized, and only when it
// names exactly one schema. Measured against the pinned community binary
// v1.3.0 on PostgreSQL 16, with an earlier migration creating `app."Users"` and
// `app.audit_log` and the next one dropping both:
//
//   - `postgres://…?search_path=public` reports no diagnostic, counts no schema
//     change and exits 0;
//   - the same directory through the same URL WITHOUT `search_path` reports one
//     DS102 per dropped table and exits 1;
//   - `search_path=public,app` is not a search-path list to that binary but one
//     schema NAME, and it refuses the run outright with
//     `taking database snapshot: postgres: schema "public,app" was not found`.
//
// The third case has no scoping behavior to match, so a comma-carrying value
// scopes nothing here and every object stays under review. Every other dialect
// scopes nothing for the same reason: the boundary has not been measured there,
// and reviewing more than the comparison tool does is the safe direction.
func SchemaScope(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	dialect, err := DialectFromURL(rawURL)
	if err != nil || !platform.IsPostgresFamily(dialect) {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	scope := strings.TrimSpace(parsed.Query().Get(searchPathParam))
	if scope == "" || strings.Contains(scope, ",") {
		return ""
	}
	return scope
}
