// Package migrateclean answers the precondition the Atlas-compatible
// `migrate apply` enforces before it adopts a database for the first time:
// whether the connected schema already holds tables that no migration in the
// directory created.
//
// It sits between the compat command in cmd/atlas and the live catalog. The
// command owns the two questions this package cannot see — whether the run was
// opted out of the gate, and whether the revision table already holds rows —
// and this package owns the two it can: which tables are in scope, and what
// the refusal says.
//
// Every operand below is measured against the pinned community binary v1.3.0
// rather than designed. The fixtures are named on the declarations they pin.
package migrateclean

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
)

// Governs reports whether the not-clean gate is enforced for a dialect.
//
// The list is the set of dialects the gate was measured on, not the set the
// catalog probes below could be written for. A dialect answers "no" until
// somebody runs the pinned binary against it and records what it does, because
// the cost of guessing is asymmetric: guessing "enforce" turns every existing
// run on that dialect into a refusal the other implementation may never make,
// which is the drop-in regression the compatibility policy forbids outright.
//
// Measured 2026-08-07: PostgreSQL 17, MySQL 9.7, SQLite. MariaDB is included
// because it reads the same information_schema through the same code path as
// MySQL, and is called out as inferred in the pull request rather than passed
// off as measured.
func Governs(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite:
		return true
	default:
		return false
	}
}

// Scope is the set of tables the gate reasons about, plus everything the
// refusal has to name.
//
// Tables holds only the connected schema's own tables. A table in another
// schema is deliberately absent: measured on PostgreSQL 17, a database whose
// `public` is empty and whose `extra` schema holds a table applies at exit 0,
// so widening the scope past the connection's schema would refuse where the
// pinned binary accepts.
type Scope struct {
	// Dialect is the connection's dialect, as reported by dbschema.
	Dialect string
	// Schema names the connected schema the refusal reports. It is the schema
	// the tables were read from, so a message can never name a schema the
	// probe did not look in.
	Schema string
	// Tables lists the base tables found in Schema, sorted by name.
	Tables []string
	// RevisionTable is the unqualified table this run records revisions in,
	// when that table lives inside Schema. It is empty when the run was
	// pointed at another schema with --revisions-schema, because the gate then
	// has no bookkeeping table of its own inside the scope to exempt.
	RevisionTable string
}

// Inspect reads the base tables in the connection's own schema.
//
// revisionTable and revisionsSchema describe where the calling run records its
// revisions. They are passed in rather than assumed because the compat surface
// lets --revisions-schema move that table out of the connected schema, and the
// exemption must follow it: measured on PostgreSQL 17, `migrate apply
// --revisions-schema revs` against a `public` holding exactly one unrelated
// table still refuses and names that table, which is what proves the rule is
// "any table other than this run's revision table" rather than "more than one
// table".
//
// A dialect Governs does not cover yields a zero Scope, whose Refusal is nil.
func Inspect(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	revisionTable string,
	revisionsSchema string,
) (Scope, error) {
	if conn == nil {
		return Scope{}, fmt.Errorf("migrate apply clean check requires a database connection")
	}
	dialect := conn.Info().Dialect
	if !Governs(dialect) {
		return Scope{}, nil
	}
	scope := Scope{Dialect: dialect, Schema: strings.TrimSpace(conn.Info().Schema)}
	query, args := tableProbe(dialect, scope.Schema)
	if query == "" {
		// Governs said yes and this function has no probe for it. Reporting a
		// clean database would be a gate that passes without running, so it
		// fails loudly instead.
		return Scope{}, fmt.Errorf("migrate apply clean check has no catalog probe for dialect %q", dialect)
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return Scope{}, fmt.Errorf("migrate apply clean check: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		schema, name, scanErr := scanProbeRow(dialect, rows)
		if scanErr != nil {
			return Scope{}, fmt.Errorf("migrate apply clean check: %w", scanErr)
		}
		// The schema comes back with the rows so the refusal names the schema
		// the probe really read, not the one the connection was configured
		// with — those differ whenever the URL pins no search_path and the
		// probe falls back to the session's own schema.
		if schema != "" {
			scope.Schema = schema
		}
		scope.Tables = append(scope.Tables, name)
	}
	if err := rows.Err(); err != nil {
		return Scope{}, fmt.Errorf("migrate apply clean check: %w", err)
	}
	slices.Sort(scope.Tables)
	scope.RevisionTable = revisionTableInScope(dialect, scope.Schema, revisionTable, revisionsSchema)
	return scope, nil
}

// Refusal returns the pinned binary's refusal when the scope holds a table this
// run's revisions table does not account for, and nil when it does not.
//
// The two message shapes are measured, and they differ in more than wording.
//
// PostgreSQL 17 and MySQL 9.7 name a single table and its schema, and the table
// they name is the alphabetically first one in the schema INCLUDING the
// revisions table: with `legacy_stuff` present the refusal names
// `atlas_schema_revisions`, and with `aaa_legacy` present it names
// `aaa_legacy`. Creation order is not the operand — `zzz_first` created before
// `bbb_second` still yields `atlas_schema_revisions`.
//
// SQLite reports a count instead of a name, and the count includes the
// revisions table while excluding SQLite's own `sqlite_*` tables: one unrelated
// table reads `found multiple tables: 2`, two read `3`, and a database holding
// nothing but `sqlite_sequence` applies at exit 0.
//
// The count and the named candidate are both computed over Tables plus
// RevisionTable rather than over Tables alone, because a dry run never creates
// the revisions table on this implementation while the pinned binary creates it
// before checking. Without the addition the same database would report one
// table fewer under --dry-run than under a real apply, and the SQLite count
// would disagree with the pinned binary by exactly one.
func (s Scope) Refusal() error {
	if !Governs(s.Dialect) {
		return nil
	}
	candidates := s.candidates()
	unmanaged := 0
	for _, table := range candidates {
		if table != s.RevisionTable {
			unmanaged++
		}
	}
	if unmanaged == 0 {
		return nil
	}
	if platform.NormalizeDialect(s.Dialect) == platform.SQLite {
		return fmt.Errorf(
			"sql/migrate: connected database is not clean: found multiple tables: %d. baseline version or allow-dirty is required",
			len(candidates),
		)
	}
	return fmt.Errorf(
		"sql/migrate: connected database is not clean: found table %q in schema %q. baseline version or allow-dirty is required",
		candidates[0], s.Schema,
	)
}

// candidates returns the sorted table names the refusal reasons about: every
// table the probe saw, plus this run's revisions table when the probe did not
// already see it.
func (s Scope) candidates() []string {
	candidates := slices.Clone(s.Tables)
	if s.RevisionTable != "" && !slices.Contains(candidates, s.RevisionTable) {
		candidates = append(candidates, s.RevisionTable)
	}
	slices.Sort(candidates)
	return slices.Compact(candidates)
}

// revisionTableInScope reports the revisions table name to exempt, or "" when
// this run keeps its revisions outside the connected schema.
//
// SQLite has no schemas, so --revisions-schema cannot move anything there and
// the table is always in scope.
func revisionTableInScope(dialect, connectedSchema, revisionTable, revisionsSchema string) string {
	revisionTable = strings.TrimSpace(revisionTable)
	if revisionTable == "" {
		return ""
	}
	revisionsSchema = strings.TrimSpace(revisionsSchema)
	if revisionsSchema == "" || platform.NormalizeDialect(dialect) == platform.SQLite {
		return revisionTable
	}
	if strings.EqualFold(revisionsSchema, connectedSchema) {
		return revisionTable
	}
	return ""
}

// probeRow is the row shape scanProbeRow reads. It exists so the two-column and
// one-column probes can share one scanning path.
type probeRow interface {
	Scan(dest ...any) error
}

func scanProbeRow(dialect string, row probeRow) (schema, name string, err error) {
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		if err := row.Scan(&name); err != nil {
			return "", "", err
		}
		return "", name, nil
	}
	if err := row.Scan(&schema, &name); err != nil {
		return "", "", err
	}
	return schema, name, nil
}

// tableProbe builds the catalog query that lists the base tables inside the
// gate's scope.
//
// The scope is the connection's own schema, defaulted the way each dialect's
// session defaults it rather than to a literal, so a URL that pins no schema is
// asked about the schema its statements would really land in.
//
// An unrecognized dialect returns an empty query rather than falling through to
// another dialect's catalog; Inspect turns that into an error.
func tableProbe(dialect, schema string) (string, []any) {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		// The ESCAPE clause matters: in LIKE, `_` matches any single
		// character, so an unescaped 'sqlite_%' would also hide a user table
		// named `sqliteXthing` and under-report the database.
		return `
			SELECT name
			FROM sqlite_master
			WHERE type = 'table'
			  AND name NOT LIKE 'sqlite\_%' ESCAPE '\'
			ORDER BY name`, nil
	case platform.MySQL, platform.MariaDB:
		return `
			SELECT table_schema, table_name
			FROM information_schema.tables
			WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())
			  AND table_type = 'BASE TABLE'
			ORDER BY table_name`, []any{schema}
	case platform.Postgres:
		// relkind 'r' and 'p' are ordinary and partitioned tables. Views,
		// sequences, materialized views and indexes are excluded on purpose:
		// measured on PostgreSQL 17, a database holding only a view or only a
		// sequence applies at exit 0.
		return `
			SELECT n.nspname, c.relname
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = COALESCE(NULLIF($1, ''), current_schema())
			  AND c.relkind IN ('r', 'p')
			ORDER BY c.relname`, []any{schema}
	default:
		return "", nil
	}
}
