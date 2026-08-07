// Package schemaselection derives, in one place, what a database URL says
// about the schema a run works in — and what the server resolved that to.
//
// Two callers need one of these facts each, and they are NOT the same fact:
//
//   - `migrate lint` needs to know whether the operator restricted the review
//     to a schema. Only the URL knows this.
//   - the PostgreSQL realm cleanup needs to know which schema the session
//     actually lands in, so it can tell the schema it owns from a stranger's.
//     Only the server knows this.
//
// They were derived independently, which is what stokaro/ptah#1207 is about,
// and folding them into one answer is exactly what must not happen:
// current_schema() answers "public" for a URL that named "public" AND for one
// that named nothing at all. The second case means "review the whole database"
// and reviewing only "public" there breaks parity with the community binary.
//
// So the two facts stay two, reached through one type.
package schemaselection

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// searchPathParam is the PostgreSQL-family query parameter that restricts a
// database URL to a single schema.
const searchPathParam = "search_path"

// RowQuerier is the part of *sql.DB and *sql.Tx [Selection.Resolve] needs.
// Taking the narrow interface keeps this package off the connection layer,
// which is what would otherwise import it back.
type RowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Selection is what a database URL says about the schema a run works in.
type Selection struct {
	// Raw is the search_path value exactly as the URL spells it, empty when the
	// URL carries none or is not PostgreSQL-family. Diagnostics quote this
	// rather than Scope: an operator who wrote `search_path=a,b` needs to see
	// what they wrote, and Scope is empty for that value.
	Raw string

	// Scope is the one schema analysis is restricted to, or empty when the URL
	// puts every schema of the connected database under review.
	//
	// Only the PostgreSQL-family `search_path` form is recognized, and only when
	// it names exactly one schema. Measured against the pinned community binary
	// v1.3.0 on PostgreSQL 16, with an earlier migration creating `app."Users"`
	// and `app.audit_log` and the next one dropping both:
	//
	//   - `postgres://…?search_path=public` reports no diagnostic, counts no
	//     schema change and exits 0;
	//   - the same directory through the same URL WITHOUT `search_path` reports
	//     one DS102 per dropped table and exits 1;
	//   - `search_path=public,app` is not a search-path list to that binary but
	//     one schema NAME, and it refuses the run outright with
	//     `taking database snapshot: postgres: schema "public,app" was not
	//     found`.
	//
	// The third case has no scoping behavior to match, so a comma-carrying value
	// scopes nothing here and every object stays under review. Every other
	// dialect scopes nothing for the same reason: the boundary has not been
	// measured there, and reviewing more than the comparison tool does is the
	// safe direction.
	Scope string
}

// FromURL reads the selection a database URL carries. An unparseable URL, or
// one of a dialect whose scoping boundary has not been measured, selects
// nothing — which puts everything under review rather than less.
func FromURL(rawURL string) Selection {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return Selection{}
	}
	dialect, err := atlasurl.DialectFromURL(rawURL)
	if err != nil || !platform.IsPostgresFamily(dialect) {
		return Selection{}
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return Selection{}
	}
	return fromQuery(parsed.Query())
}

// FromParsedURL is [FromURL] for a URL a caller has already parsed and whose
// dialect it has already established. Re-parsing there would give the caller
// two answers to "is this PostgreSQL" and let them drift.
func FromParsedURL(parsed *url.URL) Selection {
	if parsed == nil {
		return Selection{}
	}
	return fromQuery(parsed.Query())
}

func fromQuery(query url.Values) Selection {
	raw := strings.TrimSpace(query.Get(searchPathParam))
	selection := Selection{Raw: raw}
	if raw == "" || strings.Contains(raw, ",") {
		return selection
	}
	selection.Scope = raw
	return selection
}

// Resolve asks the server which schema this session actually resolved to. It is
// the fact the realm cleanup needs: the schema the writer owns, as opposed to
// the ones it may drop.
//
// A NULL or empty answer means the search_path names only schemas that do not
// exist. That is refused rather than folded back to "public": a caller who named
// a schema and silently got a different one is the failure #1198 was, and
// answering "public" would resume dropping the schemas that one does not cover.
// Ptah is pre-general-availability, so the previous fallback is not owed
// compatibility.
//
// The message names the schema, because the operator's mistake is in the URL and
// nothing downstream can say so: without it the run reaches the replay and fails
// on a CREATE TABLE with "no schema has been selected to create in", which sends
// them to their migration.
func (s Selection) Resolve(ctx context.Context, q RowQuerier) (string, error) {
	var current sql.NullString
	if err := q.QueryRowContext(ctx, "SELECT current_schema()").Scan(&current); err != nil {
		return "", fmt.Errorf("failed to resolve PostgreSQL current schema: %w", err)
	}
	if !current.Valid || current.String == "" {
		return "", fmt.Errorf("database URL selects schema %q, which does not exist in this database", s.Raw)
	}
	return current.String, nil
}
