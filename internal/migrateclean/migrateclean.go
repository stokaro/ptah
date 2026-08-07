// Package migrateclean answers the precondition the Atlas-compatible
// `migrate apply` enforces before it adopts a database for the first time:
// whether the database already holds objects that no migration in the
// directory created.
//
// It sits between the compat command in cmd/atlas and the live catalog. The
// command owns the two questions this package cannot see — whether the run was
// opted out of the gate, and whether the revision table already holds rows —
// and this package owns the three it can: which scope the connection selected,
// what is in that scope, and what the refusal says.
//
// # Two scopes, not one
//
// The pinned community binary evaluates one of two things depending on what the
// URL pinned, and the operands are not the same:
//
//   - Schema scope, selected by a PostgreSQL-family `search_path` on the URL or
//     by a database on a MySQL-family URL. The operand is TABLES in that one
//     schema, and the refusal names a table.
//   - Realm scope, selected by a URL that pins neither. The operand is SCHEMAS,
//     and the refusal names a schema. An EMPTY extra schema is enough to refuse.
//
// stokaro/ptah#1252 implemented the first and scoped through `current_schema()`
// unconditionally, which can only ever reach schema scope; stokaro/ptah#1257 is
// the second, and it is the shorter URL spelling — the one the compatibility
// documentation itself uses.
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
	"go.5x5.cz/ptah/internal/schemaselection"
)

const (
	// postgresDefaultSchema is the one schema a realm-scope run tolerates
	// finding, and only while it holds no table.
	//
	// It is the literal name rather than the session's schema. Measured on
	// PostgreSQL 17 with `ALTER DATABASE … SET search_path TO zapp` and both
	// `public` and `zapp` empty, the pinned binary refuses `found schema
	// "zapp"` — the schema the session lands in is an offender and the one it
	// does not is not.
	postgresDefaultSchema = "public"

	// realmRevisionsSchema is where a realm-scope run keeps its revision table
	// when --revisions-schema names nothing.
	//
	// Measured on PostgreSQL 17: a plain URL against an empty database leaves
	// the table in `atlas_schema_revisions.atlas_schema_revisions`, and a
	// database already holding exactly that applies at exit 0. This
	// implementation keeps its own revision table in `public` at that scope,
	// which is a divergence recorded in stokaro/ptah#1257 rather than a reason
	// to model the gate on anything but the binary's own bookkeeping.
	realmRevisionsSchema = "atlas_schema_revisions"
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

// Scope is the set of objects the gate reasons about, plus everything the
// refusal has to name.
//
// Realm decides which half is populated, and the two halves answer different
// questions. In schema scope, Tables holds only the connected schema's own
// tables and a table in another schema is deliberately absent: measured on
// PostgreSQL 17 with `?search_path=public` on the URL, a database whose `public`
// is empty and whose `extra` schema holds a table applies at exit 0. In realm
// scope, that SAME database refuses — so the two scopes disagree about the same
// catalog and only the URL says which one is being asked.
type Scope struct {
	// Dialect is the connection's dialect, as reported by dbschema.
	Dialect string
	// Schema names the connected schema. In schema scope it is the schema the
	// tables were read from, so a message can never name a schema the probe did
	// not look in. In realm scope it is recorded for context only: a realm
	// refusal names schemas out of Schemas and never this field.
	Schema string
	// Tables lists the base tables found in Schema, sorted by name. Empty in
	// realm scope.
	Tables []string
	// RevisionTable is the unqualified table this run records revisions in. In
	// schema scope it is empty when the run was pointed at another schema with
	// --revisions-schema, because the gate then has no bookkeeping table of its
	// own inside the scope to exempt. In realm scope it is always set, and
	// RevisionsSchema says which schema holds it.
	RevisionTable string

	// Realm reports that the URL pinned no schema, so the operand is schemas
	// rather than tables. See [Scope.Refusal] for the two shapes it produces.
	Realm bool
	// Schemas lists every non-system schema in the realm with the base tables
	// it holds, sorted by name. Populated only in realm scope.
	//
	// The sort is Go's byte order rather than the server's ORDER BY, which is
	// collation-dependent: measured on PostgreSQL 17 with schemas "Zed" and
	// "app" present, the binary refuses on "Zed", which is byte order and not
	// what this database's default collation returns.
	Schemas []RealmSchema
	// RevisionsSchema names the schema this run keeps its revision table in.
	// Populated only in realm scope, where the pinned binary keeps its
	// bookkeeping in a schema of its own rather than in the connected one.
	RevisionsSchema string
}

// RealmSchema is one schema of a realm-scope catalog read.
type RealmSchema struct {
	// Name is the schema name as the catalog spells it.
	Name string
	// Tables lists the base tables the schema holds, sorted by name. A schema
	// holding only views, sequences, materialized views, types, functions or an
	// extension carries none: measured on PostgreSQL 17, each of those alone in
	// `public` applies at exit 0 through a URL that pins no search_path.
	Tables []string
}

// Inspect reads whichever catalog the connection's scope selects: the base
// tables in the connection's own schema, or every schema of the realm.
//
// It must be called BEFORE the run creates anything, and it takes no revision
// operands so that it cannot be tempted to wait for a plan. This
// implementation's own preparation creates the revision table in the connected
// schema, and at realm scope that table is a stranger to the pinned binary,
// whose bookkeeping lives in a schema of its own: reading the catalog after
// preparation made the gate refuse `found schema "public"` on a database whose
// `public` held nothing but the table this very run had just created, where the
// binary applies at exit 0. The gate is not allowed to measure its own
// footprint.
//
// Where the run keeps its revisions is a question for [Scope.ForRevisions],
// which is decision input rather than catalog state.
//
// A dialect Governs does not cover yields a zero Scope, whose Refusal is nil.
func Inspect(ctx context.Context, conn *dbschema.DatabaseConnection) (Scope, error) {
	if conn == nil {
		return Scope{}, fmt.Errorf("migrate apply clean check requires a database connection")
	}
	dialect := conn.Info().Dialect
	if !Governs(dialect) {
		return Scope{}, nil
	}
	scope := Scope{Dialect: dialect, Schema: strings.TrimSpace(conn.Info().Schema)}
	if realmScoped(dialect, conn.Info().URL, scope.Schema) {
		return inspectRealm(ctx, conn, scope)
	}
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
		// the probe really read rather than the one the connection reported.
		// Reaching this path means the URL pinned a schema, and the two agree
		// there; naming the read one anyway keeps a message from ever
		// describing a schema the probe did not look in.
		if schema != "" {
			scope.Schema = schema
		}
		scope.Tables = append(scope.Tables, name)
	}
	if err := rows.Err(); err != nil {
		return Scope{}, fmt.Errorf("migrate apply clean check: %w", err)
	}
	slices.Sort(scope.Tables)
	return scope, nil
}

// ForRevisions returns the scope with the run's bookkeeping filled in, which is
// what [Scope.Refusal] exempts.
//
// The arguments are in the order atlasmigrate.ApplyPlan.RevisionTable returns
// them, so a caller passes that call through whole. revisionsSchema is the
// run's --revisions-schema and is empty when the run keeps its revisions in the
// connection's own schema.
//
// They are decision input rather than catalog state: the pinned binary creates
// its revision table before it looks, so the table has to be accounted for even
// on a --dry-run that never creates it.
func (s Scope) ForRevisions(revisionsSchema, revisionTable string) Scope {
	revisionTable = strings.TrimSpace(revisionTable)
	revisionsSchema = strings.TrimSpace(revisionsSchema)
	if !s.Realm {
		s.RevisionTable = revisionTableInScope(s.Dialect, s.Schema, revisionTable, revisionsSchema)
		return s
	}
	s.RevisionTable = revisionTable
	s.RevisionsSchema = revisionsSchema
	if s.RevisionsSchema == "" {
		s.RevisionsSchema = realmRevisionsSchema
	}
	return s
}

// realmScoped reports whether the connection left the run at realm scope, which
// is the question that decides which catalog Inspect reads.
//
// It is answered per dialect from the URL, NOT from the session, and the
// difference is observable. Measured on PostgreSQL 17, a URL carrying
// `options=-c search_path=extra` puts the session's `current_schema()` in
// `extra` and the pinned binary still evaluates the whole realm: with an empty
// `extra` present it refuses `found schema "extra"`. Only the `search_path`
// query parameter moves it to schema scope. Reading the session back would
// therefore get that URL wrong in the forbidden direction.
//
// The PostgreSQL half defers to [schemaselection], which is where
// stokaro/ptah#1207 put the single answer to "did this URL restrict the run to
// one schema", so the gate and `migrate lint` cannot drift apart. A
// comma-carrying `search_path` selects nothing there, which lands here as realm
// scope — the direction that evaluates more rather than less.
//
// MySQL-family connections answer from the connected schema instead of the URL
// because dbschema resolves the database for both URL spellings, and an empty
// answer is unreachable today: a MySQL URL with no database fails to connect
// before any of this runs, on a NULL DATABASE() scan.
func realmScoped(dialect, rawURL, connectedSchema string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		// SQLite has one namespace, so there is no wider scope to select.
		return false
	case platform.Postgres:
		return schemaselection.FromURL(rawURL).Scope == ""
	case platform.MySQL, platform.MariaDB:
		return strings.TrimSpace(connectedSchema) == ""
	default:
		return false
	}
}

// inspectRealm reads every non-system schema of the realm with the base tables
// it holds.
func inspectRealm(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	scope Scope,
) (Scope, error) {
	query := realmProbe(scope.Dialect)
	if query == "" {
		// Governs said yes, the connection selected realm scope, and this
		// function has no probe for it. Reporting a clean database would be a
		// gate that passes without running, so it fails loudly instead. The
		// shapes this branch would need on MySQL are measured and recorded on
		// realmProbe; the connection layer refuses that URL first, so writing
		// the probe would be writing code nothing can reach.
		return Scope{}, fmt.Errorf(
			"migrate apply clean check has no realm-scope catalog probe for dialect %q", scope.Dialect,
		)
	}
	scope.Realm = true
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return Scope{}, fmt.Errorf("migrate apply clean check: %w", err)
	}
	defer func() { _ = rows.Close() }()

	tables := make(map[string][]string)
	for rows.Next() {
		var schema, table string
		if scanErr := rows.Scan(&schema, &table); scanErr != nil {
			return Scope{}, fmt.Errorf("migrate apply clean check: %w", scanErr)
		}
		// The left join yields one row per schema even when it holds no table,
		// which is the whole point at this scope: an EMPTY schema refuses.
		if table == "" {
			if _, seen := tables[schema]; !seen {
				tables[schema] = nil
			}
			continue
		}
		tables[schema] = append(tables[schema], table)
	}
	if err := rows.Err(); err != nil {
		return Scope{}, fmt.Errorf("migrate apply clean check: %w", err)
	}
	for name, held := range tables {
		slices.Sort(held)
		scope.Schemas = append(scope.Schemas, RealmSchema{Name: name, Tables: held})
	}
	slices.SortFunc(scope.Schemas, func(a, b RealmSchema) int {
		return strings.Compare(a.Name, b.Name)
	})
	return scope, nil
}

// Refusal returns the pinned binary's refusal when the scope holds something
// this run's bookkeeping does not account for, and nil when it does not.
//
// Realm scope has its own two shapes and its own operand; see realmRefusal.
// The rest of this documentation is schema scope.
//
// The two schema-scope message shapes are measured, and they differ in more
// than wording.
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
	if s.Realm {
		return s.realmRefusal()
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
		return notClean("found multiple tables: %d", len(candidates))
	}
	return notClean("found table %q in schema %q", candidates[0], s.Schema)
}

// realmRefusal answers the same question about a connection that pinned no
// schema, where the operand is schemas rather than tables.
//
// The pass is one walk over Schemas in name order, and the FIRST schema that
// fails produces the message — measured on PostgreSQL 17: a dirty bookkeeping
// schema beside `bextra` reports the bookkeeping schema, and the same state
// beside `aextra` reports `aextra` instead, so neither shape outranks the
// other. Byte order is the sort: with "Zed" and "app" present the binary
// reports "Zed".
//
// A schema fails unless one of two things is true:
//
//   - it is `public` and holds no table. Measured: an empty database applies at
//     exit 0, while an EMPTY `extra` beside it refuses. `public` is tolerated by
//     NAME and not because the session lands there — with the database default
//     search_path moved to `zapp` and both `public` and `zapp` empty, the binary
//     refuses on "zapp" and still says nothing about "public".
//   - it is this run's bookkeeping schema and holds nothing but the revision
//     table. More than that reports a COUNT rather than a name, and the count
//     includes the revision table the binary creates before it looks: measured,
//     a bookkeeping schema holding one unrelated table reads
//     `found 2 tables in schema "atlas_schema_revisions"`.
//
// The count is computed over the schema's tables plus RevisionTable for the
// same reason the schema-scope count is: a dry run on this implementation never
// creates that table, and without the addition --dry-run would report one table
// fewer than a real apply against the same database.
func (s Scope) realmRefusal() error {
	if platform.NormalizeDialect(s.Dialect) != platform.Postgres {
		// Only PostgreSQL reaches realm scope through Inspect, and only its
		// shapes are measured. A Scope built by hand for another dialect gets a
		// loud error rather than a clean bill of health.
		return fmt.Errorf("migrate apply clean check has no realm-scope rule for dialect %q", s.Dialect)
	}
	for _, schema := range s.Schemas {
		if schema.Name == s.RevisionsSchema {
			held := tablesWithRevisionTable(schema.Tables, s.RevisionTable)
			if len(held) > 1 {
				return notClean("found %d tables in schema %q", len(held), schema.Name)
			}
			continue
		}
		if schema.Name == postgresDefaultSchema && len(schema.Tables) == 0 {
			continue
		}
		return notClean("found schema %q", schema.Name)
	}
	return nil
}

// notClean wraps a measured reason in the sentence the pinned binary prints
// around every one of them.
func notClean(format string, args ...any) error {
	return fmt.Errorf(
		"sql/migrate: connected database is not clean: %s. baseline version or allow-dirty is required",
		fmt.Sprintf(format, args...),
	)
}

// candidates returns the sorted table names the refusal reasons about: every
// table the probe saw, plus this run's revisions table when the probe did not
// already see it.
func (s Scope) candidates() []string {
	return tablesWithRevisionTable(s.Tables, s.RevisionTable)
}

// tablesWithRevisionTable returns tables sorted by name with revisionTable
// added when it is not already there, which is how both scopes account for the
// table the pinned binary creates before it checks.
func tablesWithRevisionTable(tables []string, revisionTable string) []string {
	candidates := slices.Clone(tables)
	if revisionTable != "" && !slices.Contains(candidates, revisionTable) {
		candidates = append(candidates, revisionTable)
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

// realmProbe builds the catalog query that lists every schema of the realm with
// the base tables it holds.
//
// The LEFT JOIN is the point: a schema with no table still comes back, because
// at this scope an empty schema is enough to refuse. The relkind filter rides on
// the join rather than the WHERE clause for the same reason — moving it down
// would drop exactly the empty schemas the gate exists to see.
//
// Only PostgreSQL has a probe. MySQL's realm shapes were measured (`found
// multiple schemas: %d` over the non-system databases including the one the
// binary creates for its revisions, and `found multiple tables: %d` once a
// single database is also the run's --revisions-schema), but a MySQL URL with
// no database never reaches this package: dbschema refuses it while reading
// DATABASE(). Writing that probe would be writing code no test could reach, so
// this returns nothing and Inspect fails loudly instead.
func realmProbe(dialect string) string {
	if platform.NormalizeDialect(dialect) != platform.Postgres {
		return ""
	}
	// The ESCAPE clause matters for the same reason it does in the SQLite probe
	// above: an unescaped 'pg_%' would also hide a schema named `pgapp`.
	return `
		SELECT n.nspname, COALESCE(c.relname, '')
		FROM pg_namespace n
		LEFT JOIN pg_class c
		  ON c.relnamespace = n.oid
		 AND c.relkind IN ('r', 'p')
		WHERE n.nspname <> 'information_schema'
		  AND n.nspname NOT LIKE 'pg\_%' ESCAPE '\'
		ORDER BY n.nspname, c.relname`
}
