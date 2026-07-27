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
// The table is read with an empty schema argument: a datadiff.DataDiff carries
// no schema, so both the read and the rendered DML target the connection's
// default schema. Schema-qualified managed tables are a known follow-up.
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
	for _, md := range managed {
		up, down, err := renderTable(ctx, conn, dialect, opts.RootDir, md)
		if err != nil {
			return "", "", err
		}
		if up == "" {
			// An empty diff renders as empty up and down together, so there is
			// nothing to reverse for this table.
			continue
		}
		ups = append(ups, tableBlock(md.Table, up))
		downs = append(downs, tableBlock(md.Table, down))
	}

	if len(ups) == 0 {
		return "", "", nil
	}

	slices.Reverse(downs)
	return strings.Join(ups, "\n"), strings.Join(downs, "\n"), nil
}

// renderTable runs the pipeline for a single managed table: load desired rows,
// read the live rows for the managed columns, diff, and render.
func renderTable(ctx context.Context, conn *dbschema.DatabaseConnection, dialect, rootDir string, md goschema.ManagedData) (up, down string, err error) {
	desired, err := goschema.LoadManagedRows(rootDir, md)
	if err != nil {
		return "", "", err
	}

	live, err := dbschema.ReadTableRows(ctx, conn, "", md.Table, managedColumns(desired, md.Keys))
	if err != nil {
		return "", "", err
	}

	// With no desired rows the managed column set is just the keys, so every
	// live row becomes a DELETE whose down re-inserts only the key columns —
	// a rollback that drops (or, under NOT NULL, cannot restore) every other
	// column. Rather than emit a knowingly-irreversible migration, refuse the
	// empty-desired-but-populated-table case. Reconstructing the full row on
	// rollback needs the table's whole column set and is a tracked follow-up;
	// an empty desired set against an empty table is still a clean no-op.
	if len(desired) == 0 && len(live) > 0 {
		return "", "", fmt.Errorf(
			"datamigrate: managed table %q has an empty desired row set but %d live row(s); a reversible full delete cannot be generated from the key columns alone — provide the desired rows or remove the annotation",
			md.Table, len(live))
	}

	diff, err := datadiff.Compute(md.Table, md.Keys, desired, live)
	if err != nil {
		return "", "", err
	}
	return datadiff.Render(diff, dialect)
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

// tableBlock prefixes a rendered per-table script with a comment naming the
// table so a reviewer can tell the concatenated blocks apart. The comment is a
// no-op at apply time and does not affect the round-trip.
func tableBlock(table, body string) string {
	return "-- data: " + table + "\n" + body
}
