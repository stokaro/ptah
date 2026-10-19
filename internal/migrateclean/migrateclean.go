// Package migrateclean answers whether a database is clean, the question the
// pinned community binary asks in two places: before `migrate apply` adopts a
// database for the first time, and before any verb uses a dev database.
//
// It sits between its callers and the live catalog. The Atlas-compatible
// `migrate apply` owns the two questions this package cannot see -- whether
// the run was opted out of the gate, and whether the revision table already
// holds rows -- and every caller that resets a dev database asks
// [DevRefusal] before it does. This package owns the three questions it can
// answer: which scope the connection selected, what is in that scope, and what
// the refusal says.
//
// # Two scopes, not one
//
// The pinned community binary evaluates one of two scopes depending on what the
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

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/schemaselection"
)

const (
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

// Governs reports whether the not-clean gate is enforced for a dialect: whether
// its descriptor states CleanCheck.Adoption.
//
// The set is the dialects the gate was measured on, not the set whose catalog
// could be read, and each descriptor records the measurement. A dialect
// answers "no" until somebody runs the pinned binary against it and records
// what it does, because the cost of guessing is
// asymmetric: guessing "enforce" turns every existing run on that dialect into
// a refusal the other implementation may never make, which is the drop-in
// regression the compatibility policy forbids outright.
func Governs(dialect string) bool {
	return described(dialect).CleanCheck.Adoption
}

// described returns the descriptor of the dialect a spelling selects, and the
// zero descriptor, which no check governs, for one the process registry does
// not know.
func described(dialect string) descriptor.Dialect {
	found, _ := platform.Lookup(dialect)
	return found
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
	// Tables lists the base tables found in Schema, sorted by name, except on
	// SQLite, where it keeps the catalog's order; see [Scope.DevRefusal]. Empty
	// in realm scope.
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

	// Dropped lists what a reset of the dev database would drop besides what
	// the fields above judge, first to be named first. It is read only by
	// [DevRefusal] and only where the dialect's writer can say; the
	// `migrate apply` gate never reads it.
	Dropped []dbreset.Object
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
	return inspect(ctx, conn, Governs)
}

// inspect is [Inspect] for the dialects governs covers. The `migrate apply`
// gate and the dev database check cover different sets, and read them the
// same way.
func inspect(ctx context.Context, conn *dbschema.DatabaseConnection, governs func(string) bool) (Scope, error) {
	if conn == nil {
		return Scope{}, fmt.Errorf("clean check requires a database connection")
	}
	dialect := conn.Info().Dialect
	if !governs(dialect) {
		return Scope{}, nil
	}
	scope := Scope{Dialect: dialect, Schema: strings.TrimSpace(conn.Info().Schema)}
	if realmScoped(dialect, conn.Info().URL, scope.Schema) {
		return inspectRealm(ctx, conn, scope)
	}
	engine := described(dialect)
	tables, err := scopeTables(ctx, conn.SchemaWriter(), conn, conn.Info().Capabilities, dialect, engine, scope.Schema)
	if err != nil {
		return Scope{}, err
	}
	for _, table := range tables {
		// The schema comes back with the rows so the refusal names the schema
		// the probe really read rather than the one the connection reported.
		// Reaching this path means the URL pinned a schema, and the two agree
		// there; naming the read one anyway keeps a message from ever
		// describing a schema the probe did not look in.
		if table.Schema != "" {
			scope.Schema = table.Schema
		}
		scope.Tables = append(scope.Tables, table.Name)
	}
	// An engine whose descriptor states CleanCheck.CatalogOrder keeps the
	// catalog's order, which the probe reads: a dev refusal names the first
	// table in it. SQLite's is the one, and its `migrate apply` refusal counts
	// the tables and names none, so the order is not an operand there.
	if !engine.CleanCheck.CatalogOrder {
		slices.Sort(scope.Tables)
	}
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
// The decision itself lives in [schemaselection.Realm], with the measurements
// that produced it, because this gate is no longer its only caller: the
// Atlas-compatible `schema inspect` surface asks the same question of the same
// URL (stokaro/ptah#1264), and two answers to it would drift apart exactly the
// way stokaro/ptah#1207 describes.
func realmScoped(dialect, rawURL, connectedSchema string) bool {
	return schemaselection.Realm(dialect, rawURL, connectedSchema)
}

// inspectRealm reads every non-system schema of the realm with the base tables
// it holds.
func inspectRealm(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	scope Scope,
) (Scope, error) {
	schemas, err := realmSchemas(ctx, conn.SchemaWriter(), conn, conn.Info().Capabilities, scope.Dialect)
	if err != nil {
		return Scope{}, err
	}
	scope.Realm = true
	scope.Schemas = schemas
	return scope, nil
}

// realmSchemas reads every schema of the realm with the base tables it holds,
// through the scope catalog writer offers, running its read on q with the
// session's capabilities caps. The schemas are in byte order, and so are the
// tables of each.
func realmSchemas(
	ctx context.Context,
	writer catalog.SchemaWriter,
	q dbreset.Querier,
	caps capability.Capabilities,
	dialect string,
) ([]RealmSchema, error) {
	scoped := writerScopeCatalog(writer)
	if scoped == nil {
		// The caller's dialect predicate said yes, the connection selected
		// realm scope, and its writer offers no catalog to read the realm
		// through. Reporting a clean database would be a gate that passes
		// without running, so it fails loudly instead.
		return nil, fmt.Errorf("clean check has no realm-scope catalog probe for dialect %q", dialect)
	}
	rows, err := scoped.Realm(ctx, q, caps)
	if err != nil {
		return nil, fmt.Errorf("clean check: %w", err)
	}

	tables := make(map[string][]string)
	for _, row := range rows {
		// The catalog lists a schema that holds no table once, with no name,
		// which is the whole point at this scope: an EMPTY schema refuses.
		if row.Name == "" {
			if _, seen := tables[row.Schema]; !seen {
				tables[row.Schema] = nil
			}
			continue
		}
		tables[row.Schema] = append(tables[row.Schema], row.Name)
	}
	var schemas []RealmSchema
	for name, held := range tables {
		slices.Sort(held)
		schemas = append(schemas, RealmSchema{Name: name, Tables: held})
	}
	slices.SortFunc(schemas, func(a, b RealmSchema) int {
		return strings.Compare(a.Name, b.Name)
	})
	return schemas, nil
}

// scopeCatalogWriter is a writer that offers what its engine's catalog answers
// about the schemas and tables a connection reaches; see [dbreset.ScopeCatalog].
type scopeCatalogWriter interface {
	ScopeCatalog() dbreset.ScopeCatalog
}

// writerScopeCatalog returns the scope catalog writer offers, or nil where it
// is not a [scopeCatalogWriter] or offers none.
func writerScopeCatalog(writer catalog.SchemaWriter) dbreset.ScopeCatalog {
	offering, ok := writer.(scopeCatalogWriter)
	if !ok {
		return nil
	}
	return offering.ScopeCatalog()
}

// scopeTables reads the base tables in the gate's schema scope: schema, or
// the schema the session lands in when it is empty, which the session
// defaults the way the engine defaults it rather than to a literal. The read
// runs on q with the session's capabilities caps.
//
// The check reads one catalog, in this order. The scope catalog writer offers
// comes first: an engine whose URL selects a schema is read through it, since
// its query binds the schema the URL pinned. Otherwise the dialect's
// CleanCheck.Tables query is read, which binds nothing, for an engine whose
// URL selects no schema. A dialect never has both: the descriptor refuses a
// Tables query where the URL selects a schema, and each writer that offers a
// catalog serves only such dialects. A dialect with neither is an error
// rather than a clean database.
func scopeTables(
	ctx context.Context,
	writer catalog.SchemaWriter,
	q dbreset.Querier,
	caps capability.Capabilities,
	dialect string,
	engine descriptor.Dialect,
	schema string,
) ([]dbreset.ScopeTable, error) {
	if scoped := writerScopeCatalog(writer); scoped != nil {
		tables, err := scoped.Tables(ctx, q, caps, schema)
		if err != nil {
			return nil, fmt.Errorf("clean check: %w", err)
		}
		return tables, nil
	}
	return describedTables(ctx, q, dialect, engine)
}

// describedTables reads the base tables through the dialect's
// CleanCheck.Tables query.
func describedTables(
	ctx context.Context, q dbreset.Querier, dialect string, engine descriptor.Dialect,
) ([]dbreset.ScopeTable, error) {
	if engine.CleanCheck.Tables == "" {
		// governs said yes and the dialect states no probe. Reporting a clean
		// database would be a gate that passes without running, so it fails
		// loudly instead.
		return nil, fmt.Errorf("clean check has no catalog probe for dialect %q", dialect)
	}
	rows, err := q.QueryContext(ctx, engine.CleanCheck.Tables)
	if err != nil {
		return nil, fmt.Errorf("clean check: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []dbreset.ScopeTable
	for rows.Next() {
		schema, name, scanErr := scanProbeRow(engine.Schemas.Selection, rows)
		if scanErr != nil {
			return nil, fmt.Errorf("clean check: %w", scanErr)
		}
		tables = append(tables, dbreset.ScopeTable{Schema: schema, Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("clean check: %w", err)
	}
	return tables, nil
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
	if described(s.Dialect).CleanCheck.AdoptionCounts {
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
// A schema fails unless one of two conditions holds:
//
//   - it is the engine's default schema, `public`, and holds no table.
//     Measured: an empty database applies at exit 0, while an EMPTY `extra`
//     beside it refuses. `public` is tolerated by NAME and not because the
//     session lands there — with `ALTER DATABASE … SET search_path TO zapp`
//     and both `public` and `zapp` empty, the binary refuses on "zapp" and
//     still says nothing about "public".
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
//
// The rule an engine takes is its schema selection's: a database-selecting
// engine's realm is a whole server, judged by [Scope.serverRefusal], and a
// search-path engine's is a database of schemas, judged here. [Scope.Refusal]
// asks only about the dialects [Governs] covers, so PostgreSQL is the one
// search-path engine that reaches this rule.
func (s Scope) realmRefusal() error {
	engine := described(s.Dialect)
	switch engine.Schemas.Selection {
	case descriptor.SchemaSelectionDatabase:
		return s.serverRefusal()
	case descriptor.SchemaSelectionSearchPath:
	default:
		// A Scope built by hand for a dialect whose URL selects no realm gets
		// a loud error rather than a clean bill of health.
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
		if schema.Name == engine.DefaultSchema && len(schema.Tables) == 0 {
			continue
		}
		return notClean("found schema %q", schema.Name)
	}
	return nil
}

// serverRefusal answers the realm question on a whole MySQL or MariaDB
// server, where the realm's schemas are its user databases. The pinned binary
// creates its bookkeeping database before it looks, and then refuses on a
// count, measured on MySQL 8.4.11 and MariaDB 11.8.9 (stokaro/ptah#3789):
//
//   - more than one database, the bookkeeping database included, reads
//     `found multiple schemas: 2` for one other database, even an empty one,
//     and `3` for two;
//   - the bookkeeping database alone, holding a table besides the revision
//     table, reads `found multiple tables: 2`, the revision table included.
//
// Both counts include what the binary creates before it looks, so a dry run,
// which creates nothing here, reports what a real apply reports.
func (s Scope) serverRefusal() error {
	databases := make([]string, 0, len(s.Schemas)+1)
	var bookkeeping []string
	for _, schema := range s.Schemas {
		databases = append(databases, schema.Name)
		if schema.Name == s.RevisionsSchema {
			bookkeeping = schema.Tables
		}
	}
	if !slices.Contains(databases, s.RevisionsSchema) {
		databases = append(databases, s.RevisionsSchema)
	}
	if len(databases) > 1 {
		return notClean("found multiple schemas: %d", len(databases))
	}
	if held := tablesWithRevisionTable(bookkeeping, s.RevisionTable); len(held) > 1 {
		return notClean("found multiple tables: %d", len(held))
	}
	return nil
}

// wholeServer reports whether a realm-scope scope of dialect is a whole
// server, whose schemas are its user databases: the realm of an engine whose
// URL selects a database.
func wholeServer(dialect string) bool {
	return described(dialect).Schemas.Selection == descriptor.SchemaSelectionDatabase
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
// An engine with one namespace, such as SQLite, has no schema
// --revisions-schema could move anything to, so the table is always in scope
// there.
func revisionTableInScope(dialect, connectedSchema, revisionTable, revisionsSchema string) string {
	revisionTable = strings.TrimSpace(revisionTable)
	if revisionTable == "" {
		return ""
	}
	revisionsSchema = strings.TrimSpace(revisionsSchema)
	if revisionsSchema == "" || described(dialect).Schemas.Selection == descriptor.SchemaSelectionSingle {
		return revisionTable
	}
	if strings.EqualFold(revisionsSchema, connectedSchema) {
		return revisionTable
	}
	return ""
}

// probeRow is the row shape scanProbeRow reads.
type probeRow interface {
	Scan(dest ...any) error
}

// scanProbeRow reads one row of a CleanCheck.Tables query: the table's schema
// and name, or its name alone on an engine with one namespace, whose query
// selects no schema.
func scanProbeRow(selection descriptor.SchemaSelection, row probeRow) (schema, name string, err error) {
	if selection == descriptor.SchemaSelectionSingle {
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
