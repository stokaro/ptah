package atlas

import (
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// atlasRevisionsSchema is the schema the Atlas surface keeps its revision table
// in on the PostgreSQL family.
//
// Atlas puts the table in a schema of its own there rather than in the
// connection's default schema, and a drop-in that writes anywhere else cannot
// read a history Atlas wrote. Each binary then reports the other's database as
// never migrated: measured on PostgreSQL 18 against the pinned community binary
// v1.3.0, the swap failed in BOTH directions with
//
//	sql/migrate: connected database is not clean: found schema "public".
//	baseline version or allow-dirty is required
//
// because one had written `atlas_schema_revisions.atlas_schema_revisions` and
// the other `public.atlas_schema_revisions` (stokaro/ptah#1563). The table name
// already matched; only the schema did not, which is why nothing but a live
// PostgreSQL could show it.
const atlasRevisionsSchema = "atlas_schema_revisions"

// applyAtlasRevisionsSchemaDefault answers where the revision table lives when
// neither the flag nor the project file says.
//
// It is scoped to the PostgreSQL family because that is where the community
// binary scopes it, measured per dialect rather than assumed:
//
//	PostgreSQL 18   schema atlas_schema_revisions
//	MySQL 8.4       the connected database, `app`, not a database of its own
//	SQLite          the one namespace there is
//
// MySQL's schema IS its database, so naming one would move the table out of the
// database the user connected to; SQLite has no schema to name at all, and
// passing one fails outright with `unknown database "atlas_schema_revisions"`.
//
// It is applied to the RESOLVED value rather than declared as the flag's
// default, because the community binary prints no default for
// --revisions-schema and the conformance cli-surface tier compares help text
// against it. A flag default would render as `(default "…")` and diverge on a
// surface that is checked byte for byte.
func applyAtlasRevisionsSchemaDefault(resolved, databaseURL string) string {
	if strings.TrimSpace(resolved) != "" {
		return resolved
	}
	dialect, err := atlasurl.DialectFromURL(databaseURL)
	if err != nil {
		// An unreadable URL is not this function's error to report: the command
		// that connects will say so with the context this one does not have.
		// Leaving the value empty keeps the connection default, which is what
		// every dialect outside the PostgreSQL family wants anyway.
		return resolved
	}
	if !platform.IsPostgresFamily(dialect) {
		return resolved
	}
	return atlasRevisionsSchema
}
