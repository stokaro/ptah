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
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// searchPathParam is the PostgreSQL-family query parameter that restricts a
// database URL to a single schema.
const searchPathParam = "search_path"

// PostgresNonSystemSchemas is the WHERE predicate that keeps the schemas a
// realm describes and drops the server's own, over a `pg_namespace n`.
//
// The ESCAPE clause matters: in LIKE, `_` matches any single character, so an
// unescaped 'pg\_%' would also hide a user schema named `pgapp` and describe
// less of the database than is there.
//
// It is one string rather than two because two callers ask about the same set
// — this package's [RealmSchemas] and the not-clean gate's realm probe in
// go.5x5.cz/ptah/internal/migrateclean, which needs each schema's tables as
// well. Those two questions differ; "which schemas is the realm" does not.
const PostgresNonSystemSchemas = `n.nspname <> 'information_schema'
			  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'`

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

// Realm reports whether a connection left the run at realm scope — describing
// every schema of the connected database rather than one.
//
// It is answered per dialect from the URL, NOT from the session, and the
// difference is observable. Measured on PostgreSQL 17, a URL carrying
// `options=-c search_path=extra` puts the session's `current_schema()` in
// `extra` and the pinned community binary v1.3.0 still evaluates the whole
// realm: with an empty `extra` present the not-clean gate refuses `found schema
// "extra"`. Only the `search_path` query parameter moves it to schema scope.
// Reading the session back would therefore get that URL wrong.
//
// The PostgreSQL half defers to [FromURL] above, so every caller of this
// distinction shares one answer to "did this URL restrict the run to one
// schema". A comma-carrying `search_path` selects nothing there, which lands
// here as realm scope — the direction that describes more of the database
// rather than less.
//
// MySQL-family connections answer from the connected schema instead of the URL
// because dbschema resolves the database for both URL spellings, and an empty
// answer is unreachable today: a MySQL URL with no database fails to connect
// before any of this runs, on a NULL DATABASE() scan.
//
// Two callers share this: the not-clean adoption gate (stokaro/ptah#1257) and
// the Atlas-compatible `schema inspect` surface (stokaro/ptah#1264), which had
// collapsed the two scopes into the connection's own schema and so described
// only one schema of a multi-schema database.
func Realm(dialect, rawURL, connectedSchema string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		// SQLite has one namespace, so there is no wider scope to select.
		return false
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return FromURL(rawURL).Scope == ""
	case platform.MySQL, platform.MariaDB:
		return strings.TrimSpace(connectedSchema) == ""
	default:
		// A dialect whose realm boundary nobody has measured stays at schema
		// scope, which is what every such connection already describes.
		return false
	}
}

// sqliteDefaultSchema is the only schema a SQLite connection has. It is spelled
// here rather than imported from the identifier semantics because this package
// needs the NAME a diagnostic prints, not the comparison rules that type
// carries, and importing those would pull the identifier layer under every
// caller of a URL question.
const sqliteDefaultSchema = "main"

// URLScope reports the one schema a database URL limits a run to, and whether it
// limits it at all.
//
// It answers [Realm]'s question from the URL ALONE, which is the point: the
// caller that needs it has not connected yet. The pinned Atlas community binary
// v1.3.0 refuses an HCL desired state that declares more than one schema against
// a schema-limited URL, and Ptah has to refuse it before the dev database is
// reset -- destroying that database over a document the run was never going to
// read is worse than the divergence being fixed. Measured with
// `schema inspect -u file://two-schema-blocks.hcl`:
//
//	--dev-url sqlite://dv?mode=memory              exit 1, limited to "main"
//	--dev-url postgres://…/db?search_path=public   exit 1, limited to "public"
//	--dev-url mysql://…/wf823_dev                  exit 1, limited to "wf823_dev"
//	--dev-url postgres://…/db                      exit 0, not limited
//
// PostgreSQL-family URLs defer to [FromURL], so this and [Realm] cannot disagree
// there. A dialect whose boundary nobody has measured limits nothing, which
// keeps this from refusing a run the comparison tool accepts.
func URLScope(rawURL string) (scope string, limited bool) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", false
	}
	dialect, err := atlasurl.DialectFromURL(rawURL)
	if err != nil {
		return "", false
	}
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		// SQLite has one namespace, so every SQLite URL is limited to it.
		return sqliteDefaultSchema, true
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		selected := FromURL(rawURL).Scope
		return selected, selected != ""
	case platform.MySQL, platform.MariaDB:
		named := urlDatabaseName(rawURL)
		return named, named != ""
	default:
		return "", false
	}
}

// urlDatabaseName reads the database a MySQL-family URL names, which is the same
// thing as its schema there.
//
// It cuts the string rather than going through [net/url.Parse] because the
// driver's own `tcp(host:port)` spelling is not a parseable URL host, and a
// parse failure would answer "no database" for a URL that names one -- the
// looser direction.
func urlDatabaseName(rawURL string) string {
	_, afterScheme, ok := strings.Cut(rawURL, "://")
	if !ok {
		return ""
	}
	_, path, ok := strings.Cut(afterScheme, "/")
	if !ok {
		return ""
	}
	path, _, _ = strings.Cut(path, "?")
	return strings.TrimSpace(path)
}

// RowsQuerier is the part of *sql.DB and dbschema.DatabaseConnection
// [RealmSchemas] needs.
type RowsQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// RealmSchemas returns every schema a realm-scoped run describes, sorted in
// byte order.
//
// The sort is Go's rather than the server's ORDER BY, which is
// collation-dependent: measured on PostgreSQL 17 with schemas "Zed" and "app"
// present, the pinned binary walks "Zed" first, which is byte order and not
// what this database's default collation returns.
//
// A dialect with no probe is an error rather than an empty list. Answering
// "no schemas" would let a caller describe an empty database where the server
// holds one, which is the failure stokaro/ptah#1264 is about. Only PostgreSQL
// reaches realm scope through [Realm] with a connection that can be opened at
// all, so only it has a probe.
func RealmSchemas(ctx context.Context, dialect string, q RowsQuerier) ([]string, error) {
	if !platform.IsPostgresFamily(dialect) {
		return nil, fmt.Errorf("no realm-scope schema probe for dialect %q", dialect)
	}
	rows, err := q.QueryContext(ctx, `
		SELECT n.nspname
		FROM pg_namespace n
		WHERE `+PostgresNonSystemSchemas)
	if err != nil {
		return nil, fmt.Errorf("failed to list realm schemas: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			return nil, fmt.Errorf("failed to list realm schemas: %w", scanErr)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list realm schemas: %w", err)
	}
	slices.Sort(names)
	return names, nil
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
