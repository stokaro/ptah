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
	"slices"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
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
// //migrator:schema:data table (iterated in Table order for deterministic
// output), loads the desired rows, reads the live rows projected onto the
// managed column set (the union of the desired rows' columns plus the key
// columns), computes the row-level diff, and renders it. The per-table up
// scripts are concatenated in Table order and the down scripts in reverse Table
// order, so that applying the whole up followed by the whole down restores the
// original state (a net-state round-trip). Tables whose diff is empty contribute
// nothing.
//
// Table order is alphabetical, not foreign-key-topological, so a migration that
// deletes a parent table's rows before a child's, or inserts a child's before a
// parent's, can hit a foreign-key violation at apply time. Each migration runs
// in a transaction, so such a violation aborts cleanly with nothing applied;
// FK-related managed tables may need manual reordering. Dependency-aware
// ordering is a tracked follow-up.
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

	var ups, downs []string
	var changes []tableChange
	for _, md := range managed {
		rendered, err := renderTable(ctx, conn, dialect, opts.RootDir, md)
		if err != nil {
			return "", "", err
		}
		if rendered.up == "" {
			// An empty diff renders as empty up and down together, so there is
			// nothing to reverse for this table.
			continue
		}
		ups = append(ups, tableBlock(qualifiedName(md.Schema, md.Table), rendered.up))
		downs = append(downs, tableBlock(qualifiedName(md.Schema, md.Table), rendered.down))
		changes = append(changes, tableChange{table: md.Table, updates: len(rendered.diff.Updates), deletes: len(rendered.diff.Deletes)})
	}

	if len(ups) == 0 {
		return "", "", nil
	}

	if err := checkPolicy(mergeByTable(changes), opts); err != nil {
		return "", "", err
	}

	slices.Reverse(downs)
	return strings.Join(ups, "\n"), strings.Join(downs, "\n"), nil
}

// tableChange records how many rows a single managed table's diff would update
// and delete, the two operations the destructive gate cares about. Inserts are
// additive and are not tracked here.
type tableChange struct {
	table   string
	updates int
	deletes int
}

// mergeByTable folds multiple change entries for the same table into one,
// summing their volumes and preserving first-seen order. More than one
// //migrator:schema:data annotation can target the same table, which would
// otherwise make the gates report and count that table twice.
func mergeByTable(changes []tableChange) []tableChange {
	merged := make([]tableChange, 0, len(changes))
	index := make(map[string]int, len(changes))
	for _, change := range changes {
		if i, ok := index[change.table]; ok {
			merged[i].updates += change.updates
			merged[i].deletes += change.deletes
			continue
		}
		index[change.table] = len(merged)
		merged = append(merged, change)
	}
	return merged
}

// tableRender is a single managed table's rendered migration together with the
// diff it came from, so the caller can both concatenate the scripts and assess
// the change volume. An empty up means the table had no drift.
type tableRender struct {
	up   string
	down string
	diff *datadiff.DataDiff
}

// renderTable runs the pipeline for a single managed table: load desired rows,
// read the live rows for the managed columns, diff, and render. On error it
// returns the zero tableRender.
func renderTable(ctx context.Context, conn *dbschema.DatabaseConnection, dialect, rootDir string, md goschema.ManagedData) (tableRender, error) {
	desired, err := goschema.LoadManagedRows(rootDir, md)
	if err != nil {
		return tableRender{}, err
	}

	live, err := dbschema.ReadTableRows(ctx, conn, md.Schema, md.Table, managedColumns(desired, md.Keys))
	if err != nil {
		return tableRender{}, err
	}

	// With no desired rows the managed column set is just the keys, so every
	// live row becomes a DELETE whose down re-inserts only the key columns —
	// a rollback that drops (or, under NOT NULL, cannot restore) every other
	// column. Rather than emit a knowingly-irreversible migration, refuse the
	// empty-desired-but-populated-table case. Reconstructing the full row on
	// rollback needs the table's whole column set and is a tracked follow-up;
	// an empty desired set against an empty table is still a clean no-op.
	if len(desired) == 0 && len(live) > 0 {
		return tableRender{}, fmt.Errorf(
			"datamigrate: managed table %q has an empty desired row set but %d live row(s); a reversible full delete cannot be generated from the key columns alone — provide the desired rows or remove the annotation",
			md.Table, len(live))
	}

	diff, err := datadiff.Compute(md.Schema, md.Table, md.Keys, desired, live)
	if err != nil {
		return tableRender{}, err
	}
	up, down, err := datadiff.Render(diff, dialect)
	if err != nil {
		return tableRender{}, err
	}
	return tableRender{up: up, down: down, diff: diff}, nil
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
// change are examined, matched case-insensitively.
func checkProtected(changes []tableChange, opts Options) error {
	if opts.AllowProd || len(opts.ProtectedTables) == 0 {
		return nil
	}

	protected := make(map[string]string, len(opts.ProtectedTables))
	for _, table := range opts.ProtectedTables {
		if table = strings.TrimSpace(table); table != "" {
			protected[strings.ToLower(table)] = table
		}
	}

	var matched []string
	for _, change := range changes {
		if original, ok := protected[strings.ToLower(change.table)]; ok {
			matched = append(matched, original)
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
		details = append(details, fmt.Sprintf("%q (%d update(s), %d delete(s))", change.table, change.updates, change.deletes))
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
// keys are always included so a table with no desired rows still reads its live
// rows (which then all become deletes) and so a key that never appears as a
// data column is still projected. The result is deduplicated and sorted because
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
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// tableBlock prefixes a rendered per-table script with a comment naming the
// table so a reviewer can tell the concatenated blocks apart. The comment is a
// no-op at apply time and does not affect the round-trip.
func tableBlock(table, body string) string {
	return "-- data: " + table + "\n" + body
}
