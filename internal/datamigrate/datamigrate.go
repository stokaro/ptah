// Package datamigrate composes the declarative reference/seed data pipeline:
// it parses //ptah:schema:data annotations, reads the corresponding live
// rows, diffs them, and answers either with counts ([Inspect]) or with a single
// reversible SQL migration body pair ([Generate]).
//
// It is the database-facing orchestration layer that sits above the pure
// migration/datadiff computation and below the ptah migrations data and ptah
// schema drift commands. It lives under internal/ because it is a composition
// of existing public building blocks (core/goschema, dbschema,
// migration/datadiff) rather than a new public contract, and it is kept
// separate from the commands so it can be exercised end to end against an
// in-memory database without cobra.
package datamigrate

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dataorder"
	"ptah.run/internal/managedrows"
	"ptah.run/migration/datadiff"
	"ptah.run/migration/safety"
)

// Options configures [Generate] and [Inspect].
type Options struct {
	// RootDir is the directory of Go sources carrying //ptah:schema:data
	// annotations. It is passed verbatim to goschema.ParseDir and reused to
	// resolve each annotation's YAML row-data file, so it must be the same root
	// the annotations were authored against. Desired replaces the parse; the
	// row files still resolve against RootDir for an annotation whose recorded
	// source directory is relative.
	RootDir string
	// Desired supplies an already-resolved desired schema. When set it replaces
	// the RootDir parse, so a caller that merged several Go roots with schema
	// files inspects every declaration it resolved; re-parsing one root would
	// answer for one source and present it as the whole declaration. Each
	// annotation records the directory it was authored in, so the row files
	// still resolve from where their author put them.
	Desired *schemamodel.Database
	// Live is the introspected schema of the database being read. A managed
	// table it does not carry is compared against no rows at all rather than
	// read, so every declared row counts as an insert: a table nothing has
	// created yet answers no SELECT. Leave it nil to read every managed table,
	// which is what a caller that means to write a migration wants — there a
	// table the annotation names and the database lacks is an error, not a
	// column of insert counts.
	//
	// Supplying it also saves a read: the columns of each declared table are
	// decided from this schema, and a caller that leaves it nil pays one
	// introspection covering every declared schema.
	Live *catalog.Database
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
// It resolves the declarations from opts.Desired, or by parsing the Go
// annotations under opts.RootDir, and for each declared
// //ptah:schema:data table loads the desired rows, reads the live rows
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
// column, an unrenderable value, and a declared column the live table does not
// carry each surface as an error naming the offending input: a migration body
// writes every column the declaration names, so one the database cannot take is
// refused rather than rendered from whatever a read of it returns.
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
// //ptah:schema:data annotation (the "schema" attribute, carried on
// schemamodel.ManagedData); an empty schema targets the connection's default
// schema. The schema qualifies both the live-row read and the generated DML.
func Generate(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options) (upSQL, downSQL string, err error) {
	inspected, err := inspect(ctx, conn, opts, managedrows.Write)
	if err != nil {
		return "", "", err
	}
	if len(inspected.diffs) == 0 {
		return "", "", nil
	}

	if err := checkPolicy(mergeByTable(inspected.changes), opts); err != nil {
		return "", "", err
	}

	dialect := opts.Dialect
	if dialect == "" {
		dialect = conn.Info().Dialect
	}
	orderByDependency(inspected.desired, inspected.diffs)
	return composeByPhase(inspected.diffs, dialect)
}

// TableDrift is how far one managed table's live rows sit from its
// declaration, as counts.
//
// It carries no key, no column name and no value, because the callers it
// exists for report drift where a row must not be published: an operator's
// status field and a drift document a pipeline archives. The SQL that would
// close the difference is [Generate]'s answer, and asking for it is the
// deliberate second step.
type TableDrift struct {
	// Schema is the declared schema of the table, empty for the connection's
	// default schema, matching how the annotation spells it.
	Schema string `json:"schema,omitempty"`
	// Table is the table name the annotation names.
	Table string `json:"table"`
	// Inserts counts declared rows the live table does not hold.
	Inserts int `json:"inserts"`
	// Updates counts live rows whose managed columns hold a different value
	// from the declaration.
	Updates int `json:"updates"`
	// Deletes counts live rows the declaration no longer holds.
	Deletes int `json:"deletes"`
}

// Summary is the values-free answer to "have the declared rows drifted".
//
// Tables lists only the managed tables that differ, ordered by qualified name,
// so a clean database summarizes as an empty Tables and no findings. Findings
// expresses the same volume in the migration/safety vocabulary the schema-diff
// report uses, so a caller that already classifies DDL findings by severity
// classifies row drift with the same code.
type Summary struct {
	// Tables holds one entry per drifted managed table.
	Tables []TableDrift `json:"tables,omitempty"`
	// Findings totals the volume across every table, one finding per operation.
	Findings []safety.Finding `json:"findings,omitempty"`
}

// HasChanges reports whether any managed table differs from its declaration. A
// nil Summary has no changes, so a caller that did not ask for the comparison
// reads the same answer as one whose comparison came back clean.
func (s *Summary) HasChanges() bool {
	return s != nil && len(s.Tables) > 0
}

// Inspect compares every declared row set against the live database and returns
// the difference as counts, reading the same rows and running the same diff
// [Generate] renders. The two cannot disagree about whether a table drifted:
// the comparison is one function and the rendering is what is built on top.
//
// It never applies the destructive or protected-table gates. Those refuse to
// write a migration; reporting that rows drifted is what an operator asks for
// before deciding anything, and a refusal there would hide the answer behind
// the flag that permits the change.
//
// The returned Summary is never nil. A nil connection, an unreadable row file,
// and a managed table [Options.Live] carries but the database cannot read are
// errors naming the input.
func Inspect(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options) (*Summary, error) {
	inspected, err := inspect(ctx, conn, opts, managedrows.Report)
	if err != nil {
		return nil, err
	}
	return summarize(mergeByTable(inspected.changes)), nil
}

// inspection is the read-and-diff half of the pipeline, shared by [Generate]
// and [Inspect] so a rendered migration and a reported count answer for the
// same comparison.
type inspection struct {
	// desired is the schema the declarations were read from, which
	// orderByDependency reads for the foreign-key graph.
	desired *schemamodel.Database
	// diffs holds one row-level diff per drifted table, in declaration order.
	diffs []*datadiff.DataDiff
	// changes is the same drift expressed as counts, one entry per diff.
	changes []tableChange
}

func inspect(ctx context.Context, conn *dbschema.DatabaseConnection, opts Options, want managedrows.Intent) (inspection, error) {
	if conn == nil {
		return inspection{}, errors.New("datamigrate: a database connection is required")
	}

	db := opts.Desired
	if db == nil {
		parsed, err := goschema.ParseDir(opts.RootDir)
		if err != nil {
			return inspection{}, fmt.Errorf("datamigrate: parse Go annotations in %q: %w", opts.RootDir, err)
		}
		db = parsed
	}

	managed := slices.Clone(db.ManagedData)
	slices.SortFunc(managed, func(a, b schemamodel.ManagedData) int {
		if c := cmp.Compare(a.Table, b.Table); c != 0 {
			return c
		}
		if c := cmp.Compare(a.StructName, b.StructName); c != 0 {
			return c
		}
		return cmp.Compare(a.File, b.File)
	})

	columnCatalog, err := readColumnCatalog(ctx, conn, opts.Live, managed)
	if err != nil {
		return inspection{}, err
	}

	result := inspection{desired: db, diffs: make([]*datadiff.DataDiff, 0, len(managed))}
	for _, md := range managed {
		desired, err := desiredRows(opts.RootDir, md)
		if err != nil {
			return inspection{}, err
		}
		// One comparison, shared with the data stage of `ptah schema plan`, so
		// a rendered migration and a planned statement cannot disagree about
		// which live table a declaration names, which of its columns are read,
		// or which rows differ (stokaro/ptah#3276). It orders the rows inside
		// this one table -- parents first to write, children first to remove --
		// and orderByDependency below orders the tables against each other.
		diff, err := managedrows.Compare(ctx, conn, managedrows.Request{
			Desired:           db,
			Declaration:       md,
			Rows:              desired,
			Live:              columnCatalog,
			CatalogIsComplete: opts.Live != nil,
			Intent:            want,
		})
		if err != nil {
			return inspection{}, err
		}
		if len(diff.Inserts) == 0 && len(diff.Updates) == 0 && len(diff.Deletes) == 0 {
			// No drift for this table; it contributes nothing in either direction.
			continue
		}
		result.diffs = append(result.diffs, diff)
		result.changes = append(result.changes, tableChange{
			schema:  md.Schema,
			table:   md.Table,
			inserts: len(diff.Inserts),
			updates: len(diff.Updates),
			deletes: len(diff.Deletes),
		})
	}
	return result, nil
}

// summarize turns the merged per-table volumes into the reportable [Summary].
// It reads the merged list so a table two annotations both declare is one entry
// with one set of counts, the way the gates already report it.
func summarize(changes []tableChange) *Summary {
	summary := &Summary{Tables: make([]TableDrift, 0, len(changes))}
	var inserts, updates, deletes int
	for _, change := range changes {
		summary.Tables = append(summary.Tables, TableDrift{
			Schema:  change.schema,
			Table:   change.table,
			Inserts: change.inserts,
			Updates: change.updates,
			Deletes: change.deletes,
		})
		inserts += change.inserts
		updates += change.updates
		deletes += change.deletes
	}
	slices.SortFunc(summary.Tables, func(a, b TableDrift) int {
		return cmp.Compare(qualifiedName(a.Schema, a.Table), qualifiedName(b.Schema, b.Table))
	})
	summary.Findings = rowFindings(inserts, updates, deletes)
	return summary
}

// tableChange records how many rows a single managed table's diff would insert,
// update and delete, along with the table's schema so the gates identify and
// report it as schema.table. The destructive gate reads the update and delete
// counts alone; a report reads all three, because a declared row the database
// never got is drift even though writing it takes nothing away.
type tableChange struct {
	schema  string
	table   string
	inserts int
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
// //ptah:schema:data annotation can target the same table, which would
// otherwise make the gates report and count that table twice. Tables that share
// a bare name across different schemas stay distinct.
func mergeByTable(changes []tableChange) []tableChange {
	merged := make([]tableChange, 0, len(changes))
	index := make(map[string]int, len(changes))
	for _, change := range changes {
		key := change.qualified()
		if i, ok := index[key]; ok {
			merged[i].inserts += change.inserts
			merged[i].updates += change.updates
			merged[i].deletes += change.deletes
			continue
		}
		index[key] = len(merged)
		merged = append(merged, change)
	}
	return merged
}

// readColumnCatalog returns the introspected schema the column decisions are
// made against: which columns a declared table carries, and what the database will
// accept an explicit value for.
//
// A caller that supplied a live schema already read one, and reading a second
// would let the two disagree about the same table within one run. A caller that
// did not is reading in order to write a migration, and the whole declaration's
// schemas are read once here rather than once per table.
func readColumnCatalog(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	live *catalog.Database,
	managed []schemamodel.ManagedData,
) (*catalog.Database, error) {
	if live != nil {
		return live, nil
	}
	if len(managed) == 0 {
		return &catalog.Database{}, nil
	}
	scopes := make([]string, 0, len(managed))
	for _, md := range managed {
		scopes = append(scopes, schemaScope(md.Schema, conn.Info().Schema)...)
	}
	slices.Sort(scopes)
	read, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, slices.Compact(scopes))
	if err != nil {
		return nil, fmt.Errorf("datamigrate: introspect the columns of the declared tables: %w", err)
	}
	return read, nil
}

// desiredRows resolves one declaration's rows, from whichever half of it the
// caller's desired schema arrived with.
//
// A declaration that carries its rows is read as it stands. A published schema
// artifact is the caller this matters to: it travels without the working copy
// the annotation pointed into, so the file name it records resolves against
// whatever directory this process runs in, and reading it answers for a file
// nobody published — or, more often, fails and takes the whole check down. A
// declaration with no rows names a file in a checkout that is still here, and
// the file is the answer.
//
// The two forms resolve to the same values for the same declaration, which is
// [schemamodel.ResolveManagedRows]'s contract, so an artifact-sourced run and a
// root-sourced run of one declaration report the same drift.
func desiredRows(rootDir string, md schemamodel.ManagedData) ([]map[string]any, error) {
	if md.Rows != nil {
		return schemamodel.ResolveManagedRows(md)
	}
	return schemamodel.LoadManagedRows(rootDir, md)
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

// orderByDependency reorders diffs in place so a table appears before every
// table that declares a foreign key to it, matching the schema's dependency
// graph. The rank comes from dataorder, which the plan stage reads too, because
// a row's order against the rows it references is one rule and not two. Managed
// tables with no schema-object definition — and any left after a circular
// dependency — keep a stable alphabetical order after the known ones, so output
// stays deterministic.
func orderByDependency(db *schemamodel.Database, diffs []*datadiff.DataDiff) {
	ranker := dataorder.New(db)
	slices.SortStableFunc(diffs, func(a, b *datadiff.DataDiff) int {
		if c := cmp.Compare(ranker.Rank(a.Schema, a.Table), ranker.Rank(b.Schema, b.Table)); c != 0 {
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
		Schema:      d.Schema,
		Table:       d.Table,
		Keys:        d.Keys,
		ColumnTypes: d.ColumnTypes,
		Inserts:     inserts,
		Updates:     updates,
		Deletes:     deletes,
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

// rowFindings expresses the row-change volume as migration/safety findings, the
// same shape safety.ClassifySchemaDiff produces for DDL.
//
// A DELETE removes rows and an UPDATE overwrites existing row values, so both
// are marked Destructive. This is deliberately stricter than the DDL
// classifier, which treats in-place value rewrites (for example SET EXPRESSION)
// as a Warning: row data a write overwrites in a live database cannot be
// recovered from the migration alone, so the stricter classification is
// intentional. An INSERT writes a declared row the database does not hold and
// takes nothing away, so it is Safe — a threshold set to destructive passes a
// database that is only missing reference rows, and a threshold that fails on
// any drift still sees it.
//
// The findings are ordered insert, update, delete rather than by severity. A
// caller that needs a different order sorts; a caller that appends these to
// another list needs the order to be the same on every run, which this is.
func rowFindings(inserts, updates, deletes int) []safety.Finding {
	var findings []safety.Finding
	addFinding(&findings, "data_rows_inserted", inserts, safety.Safe)
	addFinding(&findings, "data_rows_updated", updates, managedrows.ReportUpdateSeverity)
	addFinding(&findings, "data_rows_deleted", deletes, managedrows.ReportUpdateSeverity)
	return findings
}

// destructiveFindings is the volume the generate-time gate decides on: the
// operations that overwrite or remove a live row. It reads [rowFindings] rather
// than building its own list, so the gate and the report cannot disagree about
// which operation is destructive.
func destructiveFindings(updates, deletes int) []safety.Finding {
	return rowFindings(0, updates, deletes)
}

func addFinding(findings *[]safety.Finding, category string, count int, severity safety.Severity) {
	if count == 0 {
		return
	}
	*findings = append(*findings, safety.Finding{Category: category, Count: count, Severity: severity})
}

func qualifiedName(schema, table string) string {
	return schemamodel.QualifyTableName(schema, table)
}

// tableBlock prefixes a rendered script with a "-- data: <label>" comment (the
// label names the phase and table, e.g. "insert public.regions") so a reviewer
// can tell the concatenated phase blocks apart. The comment is a no-op at apply
// time and does not affect the round-trip.
func tableBlock(label, body string) string {
	return "-- data: " + label + "\n" + body
}
