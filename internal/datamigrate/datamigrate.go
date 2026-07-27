// Package datamigrate composes the declarative reference/seed data pipeline:
// it parses //migrator:schema:data annotations, reads the corresponding live
// rows, diffs them, and renders the difference as a single reversible SQL
// migration body pair.
//
// It is the database-facing orchestration layer that sits above the pure
// migration/datadiff computation and below the ptah migrations data command. It
// lives under internal/ because it is a composition of existing public building
// blocks (core/goschema, dbschema, migration/datadiff) rather than a new public
// contract, and it is kept separate from the command so it can be exercised
// end to end against an in-memory database without cobra.
package datamigrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/datadiff"
	"github.com/stokaro/ptah/migration/safety"
)

// Options configures [Generate].
type Options struct {
	// RootDir is the directory of Go sources carrying //migrator:schema:data
	// annotations. It is passed verbatim to goschema.ParseDir and reused to
	// resolve each annotation's YAML row-data file, so it must be the same root
	// the annotations were authored against.
	RootDir string
	// Dialect selects the SQL dialect for literal and identifier rendering. When
	// empty the dialect reported by the connection is used, matching how the
	// command infers it from --db-url.
	Dialect string
	// AllowDestructive permits generating a migration whose up body updates or
	// deletes existing rows. When false (the default) any update or delete makes
	// Generate refuse with a summary of the destructive volume. A data migration
	// is applied through the ordinary migration path, where neither the lint nor
	// the safety gate classifies row INSERT/UPDATE/DELETE as destructive, so this
	// generate-time gate is the only guard standing between a stray diff and a
	// mass row deletion or overwrite at apply time. Insert-only migrations are
	// additive and are never gated.
	AllowDestructive bool
	// ProtectedTables lists managed tables that require AllowProd before a
	// generated migration may change them at all (insert, update, or delete),
	// mirroring the protected-target posture of migration/seeder. Matching is
	// case-insensitive. Only tables the migration would actually change are
	// considered, so protecting a table with no drift is a no-op.
	ProtectedTables []string
	// AllowProd permits changing a protected table.
	AllowProd bool
}

// Generate composes the full data-migration pipeline against conn and returns a
// single reversible SQL body pair covering every managed table.
//
// It parses the Go annotations under opts.RootDir and, for each declared
// //migrator:schema:data table, loads the desired rows, reads the live rows
// projected onto the managed column set (the union of the desired rows' columns
// plus the key columns), and computes the row-level diff. When a table's desired
// set is empty but the live table is populated, every live row becomes a full
// DELETE; the projection then widens to the table's complete non-generated column
// set so the generated down can re-insert whole rows (generated/computed columns
// are excluded because the database recomputes them on insert). Tables whose diff
// is empty contribute nothing.
//
// The generated migration is phase-separated and ordered by the schema's
// foreign-key dependency graph so that foreign keys hold at apply time: up runs
// every INSERT first with parent tables before the child tables that reference
// them, then every UPDATE, then every DELETE with child tables before their
// parents (the reverse). down is the exact reverse-inverse — it undoes the
// DELETEs (re-inserting parents first), then the UPDATEs, then the INSERTs — so
// applying the whole up followed by the whole down restores the original state.
// The dependency order comes from the parsed schema (goschema orders tables
// parents-first); managed tables without a schema-object definition, and any
// left after a circular dependency, fall back to a stable alphabetical order.
//
// When no managed table has any changes, both returned strings are empty and
// the caller writes nothing. A missing row-data file, a row missing a key
// column, or an unrenderable value surfaces as an error naming the offending
// input.
//
// Two generate-time safety gates guard the change set once it is computed but
// before any SQL is returned, so they apply equally to a dry run and to a
// written migration. Unless opts.AllowDestructive is set, a change set that
// updates or deletes any existing row is refused with a per-table summary;
// unless opts.AllowProd is set, a change set that touches any opts.ProtectedTables
// entry is refused. See [Options] for why these live here rather than on the
// apply path.
//
// Each table is read and rendered under the schema declared on its
// //migrator:schema:data annotation (the "schema" attribute, carried on
// goschema.ManagedData); an empty schema targets the connection's default
// schema. The schema qualifies both the live-row read and the generated DML.
func Generate(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options) (upSQL, downSQL string, err error) {
	if conn == nil {
		return "", "", errors.New("datamigrate: a database connection is required")
	}

	dialect := opts.Dialect
	if dialect == "" {
		dialect = conn.Info().Dialect
	}

	db, err := goschema.ParseDir(opts.RootDir)
	if err != nil {
		return "", "", fmt.Errorf("datamigrate: parse Go annotations in %q: %w", opts.RootDir, err)
	}

	managed := slices.Clone(db.ManagedData)
	slices.SortFunc(managed, func(a, b goschema.ManagedData) int {
		if c := cmp.Compare(a.Table, b.Table); c != 0 {
			return c
		}
		if c := cmp.Compare(a.StructName, b.StructName); c != 0 {
			return c
		}
		return cmp.Compare(a.File, b.File)
	})

	diffs := make([]*datadiff.DataDiff, 0, len(managed))
	var changes []tableChange
	for _, md := range managed {
		diff, err := computeTable(ctx, conn, opts.RootDir, md)
		if err != nil {
			return "", "", err
		}
		if len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletes) == 0 {
			// No drift for this table; it contributes nothing in either direction.
			continue
		}
		diffs = append(diffs, diff)
		changes = append(changes, tableChange{schema: md.Schema, table: md.Table, updates: len(diff.Updates), deletes: len(diff.Deletes)})
	}

	if len(diffs) == 0 {
		return "", "", nil
	}

	if err := checkPolicy(mergeByTable(changes), opts); err != nil {
		return "", "", err
	}

	orderByDependency(db, diffs)
	return composeByPhase(diffs, dialect)
}

// tableChange records how many rows a single managed table's diff would update
// and delete, the two operations the destructive gate cares about, along with
// the table's schema so the gates identify and report it as schema.table.
// Inserts are additive and are not tracked here.
type tableChange struct {
	schema  string
	table   string
	updates int
	deletes int
}

// qualified returns the schema.table display form for the change (just the
// table when it has no schema), matching how the renderer qualifies the name.
func (c tableChange) qualified() string {
	return qualifiedName(c.schema, c.table)
}

// mergeByTable folds multiple change entries for the same schema-qualified table
// into one, summing their volumes and preserving first-seen order. More than one
// //migrator:schema:data annotation can target the same table, which would
// otherwise make the gates report and count that table twice. Tables that share
// a bare name across different schemas stay distinct.
func mergeByTable(changes []tableChange) []tableChange {
	merged := make([]tableChange, 0, len(changes))
	index := make(map[string]int, len(changes))
	for _, change := range changes {
		key := change.qualified()
		if i, ok := index[key]; ok {
			merged[i].updates += change.updates
			merged[i].deletes += change.deletes
			continue
		}
		index[key] = len(merged)
		merged = append(merged, change)
	}
	return merged
}

// computeTable runs the read-and-diff half of the pipeline for a single managed
// table: load desired rows, read the live rows for the appropriate column
// projection, and compute the row-level diff. Rendering happens later, in
// composeByPhase, once every table's diff is known and can be ordered by
// dependency.
func computeTable(ctx context.Context, conn *dbschema.DatabaseConnection, rootDir string, md goschema.ManagedData) (*datadiff.DataDiff, error) {
	desired, err := goschema.LoadManagedRows(rootDir, md)
	if err != nil {
		return nil, err
	}

	columns, err := readColumns(conn, md, desired)
	if err != nil {
		return nil, err
	}

	live, err := dbschema.ReadTableRows(ctx, conn, md.Schema, md.Table, columns)
	if err != nil {
		return nil, err
	}

	return datadiff.Compute(md.Schema, md.Table, md.Keys, desired, live)
}

// readColumns selects which live columns to read for a managed table's diff.
//
// With desired rows present, Ptah reconciles only the managed columns (the union
// of the desired rows' columns and the keys), so that is the projection.
//
// With an empty desired set, every live row will become a DELETE, and the
// reversible down re-inserts each deleted row. Reading only the keys would make
// that rollback restore the keys alone, dropping — or, under NOT NULL, failing to
// restore — every other column. So the projection widens to the table's full
// non-generated column set (see [fullNonGeneratedColumns]); an empty desired set
// against an empty table stays a clean no-op because the read returns no rows.
func readColumns(conn *dbschema.DatabaseConnection, md goschema.ManagedData, desired []map[string]any) ([]string, error) {
	if len(desired) == 0 {
		return fullNonGeneratedColumns(conn, md.Schema, md.Table, md.Keys)
	}
	return managedColumns(desired, md.Keys), nil
}

// fullNonGeneratedColumns introspects the live schema for the managed table and
// returns the columns to read and re-insert for an empty-desired full delete.
//
// It backs the empty-desired case: when a managed table's desired row set is
// empty but the table is populated, every live row becomes a DELETE, and the
// reversible down re-inserts the row from exactly the columns returned here. The
// column selection and its safety refusals live in the pure [insertableColumns];
// this function only performs the introspection and locates the table. A table
// that cannot be found in the introspected schema is surfaced as an error.
func fullNonGeneratedColumns(conn *dbschema.DatabaseConnection, schema, table string, keys []string) ([]string, error) {
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, schemaScope(schema, conn.Info().Schema))
	if err != nil {
		return nil, fmt.Errorf("datamigrate: introspect columns of table %q: %w", qualifiedName(schema, table), err)
	}

	dbTable, ok := findManagedTable(dbSchema, schema, conn.Info().Schema, table)
	if !ok {
		return nil, fmt.Errorf(
			"datamigrate: cannot read the columns of managed table %q: it was not found in the live schema; create the table or remove the annotation",
			qualifiedName(schema, table))
	}

	return insertableColumns(conn.Info().Dialect, qualifiedName(schema, table), dbTable, keys)
}

// insertableColumns returns the sorted set of columns to read and re-insert for
// an empty-desired full delete: every column the database will accept in an
// explicit INSERT, so the reversible down restores complete rows.
//
// Two column classes are dropped because the database rejects or ignores an
// explicit value for them:
//   - generated/computed columns (GeneratedKind set) are excluded and recompute
//     from the re-inserted base columns on rollback;
//   - identity columns that reject explicit inserts (SQL Server IDENTITY,
//     PostgreSQL GENERATED ALWAYS AS IDENTITY — see [rejectsExplicitInsert])
//     cannot be restored to their original value, so rather than emit a migration
//     whose down fails at apply time (or silently re-inserts a different value),
//     the whole empty-desired case is refused, naming the offending column(s).
//
// Auto-increment/serial columns that DO accept explicit inserts (MySQL
// AUTO_INCREMENT, SQLite AUTOINCREMENT, PostgreSQL SERIAL and GENERATED BY DEFAULT
// AS IDENTITY) are kept, so re-inserting them preserves the original identity
// values. Every key column must survive both filters, since a key drives the
// DELETE predicate and the rollback INSERT; a key that is generated, absent, or
// reject-on-insert is an error.
func insertableColumns(dialect, qualified string, dbTable types.DBTable, keys []string) ([]string, error) {
	present := make(map[string]struct{}, len(dbTable.Columns))
	cols := make([]string, 0, len(dbTable.Columns))
	var rejected []string
	for _, col := range dbTable.Columns {
		if col.GeneratedKind != "" {
			continue
		}
		if rejectsExplicitInsert(dialect, col) {
			rejected = append(rejected, col.Name)
			continue
		}
		present[col.Name] = struct{}{}
		cols = append(cols, col.Name)
	}

	if len(rejected) > 0 {
		slices.Sort(rejected)
		return nil, fmt.Errorf(
			"datamigrate: cannot generate a reversible full delete for table %q: column(s) %s reject explicit inserts (identity/auto-generated) and cannot be restored on rollback; keep at least one desired row or remove the annotation",
			qualified, quoteAll(rejected))
	}

	for _, k := range keys {
		if _, ok := present[k]; !ok {
			return nil, fmt.Errorf(
				"datamigrate: key column %q of managed table %q is not a writable, non-generated column; a reversible full delete needs every key column",
				k, qualified)
		}
	}

	slices.Sort(cols)
	return cols, nil
}

// rejectsExplicitInsert reports whether the database rejects an explicit value
// for col in an INSERT, so re-inserting it on rollback would fail at apply time.
// It is true for SQL Server IDENTITY columns and PostgreSQL GENERATED ALWAYS AS
// IDENTITY columns, and deliberately false for auto-increment/serial columns that
// accept explicit inserts (MySQL AUTO_INCREMENT, SQLite AUTOINCREMENT, PostgreSQL
// SERIAL and GENERATED BY DEFAULT AS IDENTITY), so those round-trip with their
// original values preserved.
func rejectsExplicitInsert(dialect string, col types.DBColumn) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLServer:
		// The SQL Server reader sets IsAutoIncrement only for IDENTITY columns,
		// which reject explicit inserts unless SET IDENTITY_INSERT is toggled on.
		return col.IsAutoIncrement
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner:
		return strings.EqualFold(col.IdentityGeneration, "ALWAYS")
	default:
		// MySQL/MariaDB AUTO_INCREMENT, SQLite AUTOINCREMENT, and ClickHouse accept
		// explicit inserts, so re-inserting preserves the original value.
		return false
	}
}

// quoteAll renders names as a comma-separated list of double-quoted identifiers
// for an error message (for example `"a", "b"`).
func quoteAll(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, ", ")
}

// schemaScope returns the schema allow-list to introspect for a managed table:
// the table's declared schema, or the connection's default schema when the
// annotation omits it. Readers that support schema scoping narrow to it; readers
// that do not (for example SQLite) ignore it and read their configured schema,
// which is already the default schema.
func schemaScope(managedSchema, defaultSchema string) []string {
	if s := strings.TrimSpace(managedSchema); s != "" {
		return []string{s}
	}
	if s := strings.TrimSpace(defaultSchema); s != "" {
		return []string{s}
	}
	return nil
}

// findManagedTable locates the introspected table for a managed annotation. A
// declared schema requires a schema match — either the introspected schema
// literally, or a blank introspected schema when the declared schema is the
// connection's default, since readers blank the schema of default-schema tables
// (PostgreSQL public, SQLite main). An omitted declared schema matches a
// uniquely-named table, or otherwise the one in the default schema.
func findManagedTable(dbSchema *types.DBSchema, wantSchema, defaultSchema, table string) (types.DBTable, bool) {
	wantSchema = strings.TrimSpace(wantSchema)
	defaultSchema = strings.TrimSpace(defaultSchema)

	var candidates []types.DBTable
	for _, t := range dbSchema.Tables {
		if t.Name == table {
			candidates = append(candidates, t)
		}
	}

	if wantSchema != "" {
		for _, t := range candidates {
			ts := strings.TrimSpace(t.Schema)
			if ts == wantSchema || (ts == "" && wantSchema == defaultSchema) {
				return t, true
			}
		}
		return types.DBTable{}, false
	}

	if len(candidates) == 1 {
		return candidates[0], true
	}
	for _, t := range candidates {
		ts := strings.TrimSpace(t.Schema)
		if ts == "" || ts == defaultSchema {
			return t, true
		}
	}
	return types.DBTable{}, false
}

// orderByDependency reorders diffs in place so a table appears before every
// table that declares a foreign key to it, matching the schema's dependency
// graph. db.Tables is already topologically sorted parents-first, so its index
// gives the insert order (the reverse gives the delete order). Managed tables
// with no schema-object definition — and any left after a circular dependency —
// keep a stable alphabetical order after the known ones, so output stays
// deterministic.
func orderByDependency(db *goschema.Database, diffs []*datadiff.DataDiff) {
	// Index the dependency-sorted tables by their fully-qualified name, and also
	// by bare name where that name is unambiguous across the schema. The bare
	// index is a fallback for when a //migrator:schema:data annotation omits the
	// schema attribute while its //migrator:schema:table definition sets one (or
	// vice versa); without it the qualified lookup would miss and FK ordering
	// would silently degrade to alphabetical for that table. A bare name shared
	// by tables in different schemas is left out of the fallback so it can never
	// resolve to the wrong table.
	pos := make(map[string]int, len(db.Tables))
	barePos := make(map[string]int, len(db.Tables))
	bareCount := make(map[string]int, len(db.Tables))
	for i, t := range db.Tables {
		pos[t.QualifiedName()] = i
		barePos[t.Name] = i
		bareCount[t.Name]++
	}
	rank := func(d *datadiff.DataDiff) int {
		if i, ok := pos[qualifiedName(d.Schema, d.Table)]; ok {
			return i
		}
		if bareCount[d.Table] == 1 {
			if i, ok := barePos[d.Table]; ok {
				return i
			}
		}
		return math.MaxInt
	}
	slices.SortStableFunc(diffs, func(a, b *datadiff.DataDiff) int {
		if c := cmp.Compare(rank(a), rank(b)); c != 0 {
			return c
		}
		return cmp.Compare(qualifiedName(a.Schema, a.Table), qualifiedName(b.Schema, b.Table))
	})
}

// composeByPhase renders the dependency-ordered diffs into one reversible up/down
// pair, separating the work into phases so foreign keys hold at apply time. up
// runs all INSERTs parents-first, then all UPDATEs, then all DELETEs
// children-first; down is the exact reverse-inverse (undo DELETEs parents-first,
// then UPDATEs and INSERTs children-first). Each phase is rendered from a
// single-operation sub-diff via datadiff.Render, so the proven per-table
// inverse contract composes into a global reversible migration.
func composeByPhase(ordered []*datadiff.DataDiff, dialect string) (upSQL, downSQL string, err error) {
	type phased struct {
		insUp, insDown string
		updUp, updDown string
		delUp, delDown string
	}
	rendered := make([]phased, len(ordered))
	for i, d := range ordered {
		var p phased
		if p.insUp, p.insDown, err = datadiff.Render(subDiff(d, d.Inserts, nil, nil), dialect); err != nil {
			return "", "", err
		}
		if p.updUp, p.updDown, err = datadiff.Render(subDiff(d, nil, d.Updates, nil), dialect); err != nil {
			return "", "", err
		}
		if p.delUp, p.delDown, err = datadiff.Render(subDiff(d, nil, nil, d.Deletes), dialect); err != nil {
			return "", "", err
		}
		rendered[i] = p
	}

	var up []string
	for i, d := range ordered { // INSERTs: parents before children.
		up = appendBlock(up, d, "insert", rendered[i].insUp)
	}
	for i, d := range ordered { // UPDATEs.
		up = appendBlock(up, d, "update", rendered[i].updUp)
	}
	for i, d := range slices.Backward(ordered) { // DELETEs: children before parents.
		up = appendBlock(up, d, "delete", rendered[i].delUp)
	}

	var down []string
	for i, d := range ordered { // Undo DELETEs (re-insert): parents before children.
		down = appendBlock(down, d, "delete", rendered[i].delDown)
	}
	for i, d := range slices.Backward(ordered) { // Undo UPDATEs: children before parents.
		down = appendBlock(down, d, "update", rendered[i].updDown)
	}
	for i, d := range slices.Backward(ordered) { // Undo INSERTs (delete): children before parents.
		down = appendBlock(down, d, "insert", rendered[i].insDown)
	}

	return strings.Join(up, "\n"), strings.Join(down, "\n"), nil
}

// subDiff builds a single-operation view of d carrying only the given rows, so
// each phase can be rendered independently while keeping d's schema, table, and
// key columns.
func subDiff(d *datadiff.DataDiff, inserts []datadiff.Row, updates []datadiff.RowUpdate, deletes []datadiff.Row) *datadiff.DataDiff {
	return &datadiff.DataDiff{
		Schema:  d.Schema,
		Table:   d.Table,
		Keys:    d.Keys,
		Inserts: inserts,
		Updates: updates,
		Deletes: deletes,
	}
}

// appendBlock appends a phase's rendered statements for a table, prefixed with a
// comment naming the phase and table, skipping empty phases.
func appendBlock(blocks []string, d *datadiff.DataDiff, phase, rendered string) []string {
	if rendered == "" {
		return blocks
	}
	return append(blocks, tableBlock(phase+" "+qualifiedName(d.Schema, d.Table), rendered))
}

// checkPolicy enforces the generate-time safety gates over the tables the
// migration would change. Protected-table refusal takes precedence over the
// destructive gate so that a run against a protected target reports the
// protection first, regardless of whether the change also happens to be
// destructive.
func checkPolicy(changes []tableChange, opts Options) error {
	if err := checkProtected(changes, opts); err != nil {
		return err
	}
	return checkDestructive(changes, opts)
}

// checkProtected refuses to change any table named in opts.ProtectedTables
// unless opts.AllowProd is set. Only tables that the migration would actually
// change are examined. A protected entry matches a change case-insensitively by
// either its bare table name or its schema-qualified "schema.table" form, so a
// schema-qualified managed table can be protected by either spelling and a bare
// entry protects the table in whatever schema it lives.
func checkProtected(changes []tableChange, opts Options) error {
	if opts.AllowProd || len(opts.ProtectedTables) == 0 {
		return nil
	}

	protected := make(map[string]struct{}, len(opts.ProtectedTables))
	for _, table := range opts.ProtectedTables {
		if table = strings.TrimSpace(table); table != "" {
			protected[strings.ToLower(table)] = struct{}{}
		}
	}

	var matched []string
	for _, change := range changes {
		_, bareHit := protected[strings.ToLower(change.table)]
		_, qualifiedHit := protected[strings.ToLower(change.qualified())]
		if bareHit || qualifiedHit {
			matched = append(matched, change.qualified())
		}
	}
	slices.Sort(matched)
	if len(matched) > 0 {
		return fmt.Errorf("datamigrate: refusing to modify protected table(s) %s; pass --allow-prod to override", strings.Join(matched, ", "))
	}
	return nil
}

// checkDestructive refuses a change set that updates or deletes existing rows
// unless opts.AllowDestructive is set. The row-change volume is expressed as
// migration/safety findings so the gate speaks the same severity vocabulary as
// the DDL safety report and defers the destructive verdict to
// safety.HasDestructive.
func checkDestructive(changes []tableChange, opts Options) error {
	if opts.AllowDestructive {
		return nil
	}

	var updates, deletes int
	var details []string
	for _, change := range changes {
		if change.updates == 0 && change.deletes == 0 {
			continue
		}
		updates += change.updates
		deletes += change.deletes
		details = append(details, fmt.Sprintf("%q (%d update(s), %d delete(s))", change.qualified(), change.updates, change.deletes))
	}

	if !safety.HasDestructive(destructiveFindings(updates, deletes)) {
		return nil
	}
	return fmt.Errorf(
		"datamigrate: refusing to generate a destructive data migration that would change existing rows in %s; pass --allow-destructive after reviewing the change",
		strings.Join(details, ", "))
}

// destructiveFindings expresses the row-change volume as migration/safety
// findings, the same shape safety.ClassifySchemaDiff produces for DDL. A DELETE
// removes rows and an UPDATE overwrites existing row values, so both are marked
// Destructive here. This is deliberately stricter than the DDL classifier,
// which treats in-place value rewrites (for example SET EXPRESSION) as a
// Warning: row data a data migration overwrites in a live database cannot be
// recovered from the migration alone, so the stricter default is intentional.
// INSERTs are additive and contribute no finding.
func destructiveFindings(updates, deletes int) []safety.Finding {
	var findings []safety.Finding
	addFinding(&findings, "data_rows_updated", updates)
	addFinding(&findings, "data_rows_deleted", deletes)
	return findings
}

func addFinding(findings *[]safety.Finding, category string, count int) {
	if count == 0 {
		return
	}
	*findings = append(*findings, safety.Finding{Category: category, Count: count, Severity: safety.Destructive})
}

// managedColumns returns the distinct, sorted set of columns Ptah manages for a
// table: every column that appears in any desired row plus the key columns. The
// keys are always included so a key that never appears as a data column is still
// projected. It is used for the drift path where desired rows are present; the
// empty-desired path reads the table's full non-generated column set instead (see
// [readColumns]). The result is deduplicated and sorted because
// dbschema.ReadTableRows rejects duplicate columns and a stable order keeps the
// generated SQL deterministic.
func managedColumns(rows []map[string]any, keys []string) []string {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		set[k] = struct{}{}
	}
	for _, row := range rows {
		for col := range row {
			set[col] = struct{}{}
		}
	}

	cols := make([]string, 0, len(set))
	for col := range set {
		cols = append(cols, col)
	}
	slices.Sort(cols)
	return cols
}

// qualifiedName renders a table name for the block comment, prefixing the
// schema when one is declared (schema.table) and returning just the table
// otherwise. It is display-only; the actual SQL identifiers are quoted by the
// renderer.
func qualifiedName(schema, table string) string {
	// Trim so the display and gate keys treat a blank schema the same way
	// sqlident.Qualified does when rendering the actual SQL identifier.
	if schema = strings.TrimSpace(schema); schema == "" {
		return table
	}
	return schema + "." + table
}

// tableBlock prefixes a rendered script with a "-- data: <label>" comment (the
// label names the phase and table, e.g. "insert public.regions") so a reviewer
// can tell the concatenated phase blocks apart. The comment is a no-op at apply
// time and does not affect the round-trip.
func tableBlock(label, body string) string {
	return "-- data: " + label + "\n" + body
}
