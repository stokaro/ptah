package managedrows

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dataorder"
	"ptah.run/migration/datadiff"
	"ptah.run/migration/safety"
)

// Intent is what the caller will do with the diff, which is what decides how
// much of each row is read and whether a declaration the database cannot
// satisfy is a difference to report or a refusal.
//
// A count and a plan answer a question about the database, and have to answer
// it for every state the database can be in. A migration body writes to the
// database, so a declaration it cannot render is refused before any SQL exists.
type Intent int

const (
	// Report reads enough of each row to tell an insert from an update from a
	// delete, and narrows the comparison to the columns the live table carries.
	Report Intent = iota
	// Write reads every column a reversible up and down will write, and refuses
	// a declaration the live table cannot take.
	Write
	// Plan compares against the table the plan is about to produce. Its schema
	// stage runs before its data stage, so a column the live table has not
	// gained yet is a column the data statements will write to, and the value
	// declared for it is a difference rather than drift to narrow away.
	Plan
)

// PlanUpdateSeverity and ReportUpdateSeverity are what the two callers charge
// for an UPDATE that overwrites a live row. The comparison is one answer; this
// is the policy each caller selects over it.
//
// The plan rates a statement, and overwriting managed columns is a warning
// there because the author asked for the new value. The drift report and the
// `ptah migrations data` gate rate what the database is about to lose: a live
// value nobody declared is gone once the statement runs, and it is the same
// loss --allow-destructive gates. The difference is deliberate and is written
// down on the reference-data page.
const (
	PlanUpdateSeverity   = safety.Warning
	ReportUpdateSeverity = safety.Destructive
)

// Request is one declared row set to compare against the live database.
type Request struct {
	// Desired is the schema the declaration was read from. It answers which of
	// the table's columns reference that same table, and what type each column
	// declares, so rows inside one table are ordered parents-first and each
	// value renders under its declared type. A schema that does not define the
	// table leaves the rows in key order and renders each value from its Go
	// value alone.
	Desired *schemamodel.Database
	// Declaration is the row set's //ptah:schema:data annotation.
	Declaration schemamodel.ManagedData
	// Rows are the declared rows, already resolved to the Go values the
	// comparison works in. The caller resolves them, because the file a caller
	// may read is bounded differently on each path, and every caller resolves
	// through [schemamodel.ResolveManagedRows] so the values are not.
	Rows []map[string]any
	// Live is the introspected catalog the table and column decisions are made
	// against. A nil catalog decides nothing, and the read goes out as the
	// declaration wrote it for the database to answer.
	Live *catalog.Database
	// CatalogIsComplete says that Live answers for the database.
	//
	// A caller that introspected the database and handed that catalog in has a
	// second opinion: a table it does not carry, or one missing a key column,
	// holds nothing to compare, so every declared row is an insert and nothing
	// is read. The key columns are what a row is matched on, so a read without
	// one of them could not tell an insert from an update.
	//
	// A caller that introspected only to decide the columns has no second
	// opinion, and the database answers for itself: an unreadable table is an
	// error naming it rather than a column of insert counts.
	CatalogIsComplete bool
	// Intent is what the caller will do with the diff.
	Intent Intent
	// Read, when set, receives what the comparison read from the database.
	//
	// A caller that has to recognize later whether those rows moved reads them
	// again, and it has to read the same projection or it would call every row
	// changed and every plan stale. Handing back the columns this call chose is
	// what keeps that second read from being a second decision about them.
	Read *Read
}

// Read is what one comparison read: the live columns, and whether the table
// was read at all. A table the catalog says holds nothing to compare -- one the
// plan is about to create, or one missing a key column -- is not read, and
// Performed stays false.
type Read struct {
	Columns   []string
	Performed bool
}

// Compare answers what stands between a declaration and the rows the database
// holds.
//
// It finds the live table under the connection's identifier rules, decides
// which of its columns this intent may read, reads them, and computes the row
// diff. The inserts and deletes come back ordered for the rows inside this one
// table: parents first to write, children first to remove, which is the order
// both callers' phase composition expects.
//
// A declaration with no key column, a table the database refuses to read, and a
// declared column a migration body could not write are errors naming the table.
func Compare(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	req Request,
) (*datadiff.DataDiff, error) {
	if conn == nil {
		return nil, errors.New("comparing declared rows requires a database connection")
	}
	declaration := req.Declaration
	qualified := schemamodel.QualifyTableName(declaration.Schema, declaration.Table)
	if len(declaration.Keys) == 0 {
		return nil, fmt.Errorf("managed data for table %s declares no key column", qualified)
	}
	names := conn.Info().IdentifierSemantics
	liveTable := LiveTable(req.Live, declaration.Schema, declaration.Table, names)

	// A caller whose catalog answers for the database compares against no live
	// rows wherever that catalog has nothing to read: every declared row is an
	// insert then, and a read would fail rather than answer.
	desired := req.Rows
	var live []map[string]any
	if !req.CatalogIsComplete || liveTableCarriesTheKeys(liveTable, declaration.Keys, names) {
		columns, narrowed, err := readBack(
			conn.Info().Dialect, names, liveTable, declaration, desired, req.Intent, qualified,
		)
		if err != nil {
			return nil, err
		}
		desired = narrowed
		live, err = dbschema.ReadTableRows(ctx, conn, declaration.Schema, declaration.Table, columns)
		if err != nil {
			return nil, fmt.Errorf("read declared rows of %s: %w", qualified, err)
		}
		if req.Read != nil {
			*req.Read = Read{Columns: slices.Clone(columns), Performed: true}
		}
	}

	diff, err := datadiff.Compute(declaration.Schema, declaration.Table, declaration.Keys, desired, live)
	if err != nil {
		return nil, fmt.Errorf("compare declared rows of %s: %w", qualified, err)
	}
	// The callers order one table against the tables it references. A row can
	// reference a row of its own table -- a category tree, an org chart -- and
	// that order is inside one table, so it is decided here: parents first to
	// write, children first to remove (stokaro/ptah#3266).
	references, columnTypes := declarationShape(req.Desired, declaration)
	diff.ColumnTypes = columnTypes
	diff.Inserts = dataorder.Rows(diff.Inserts, declaration.Keys, references)
	diff.Deletes = dataorder.Rows(diff.Deletes, declaration.Keys, references)
	slices.Reverse(diff.Deletes)
	return diff, nil
}

// liveTableCarriesTheKeys answers whether an introspected table is one the
// comparison can read: it exists, and it carries every column the declaration
// matches a row on. A read without one of the keys could not tell an insert
// from an update, so a table missing one holds nothing to compare.
//
// Key presence is asked under names, the connection's rules, for the reason the
// table lookup is: Oracle folds the bare name the renderer wrote, so a declared
// `code` is CODE in its catalog, and compared exactly the table reads as one
// that lost its key.
func liveTableCarriesTheKeys(liveTable *catalog.Table, keys []string, names identifier.Semantics) bool {
	if liveTable == nil {
		return false
	}
	present := liveColumnKeys(*liveTable, names)
	for _, key := range keys {
		if _, ok := present[names.ColumnIdentityKey(key)]; !ok {
			return false
		}
	}
	return true
}

// readBack selects which live columns to read, and returns the declared rows
// the diff compares them against.
//
// With declared rows present, only the managed columns are reconciled: the
// union of the rows' columns and the keys. A column the live table does not
// carry is where the two intents part, because they are about to do different
// things with it -- see [ProjectOntoLive] and [refuseUndeclaredColumns].
//
// With an empty declared set, every live row becomes a DELETE. Counting them
// needs the keys and nothing else. Rendering the reversible down that
// re-inserts each deleted row needs every column it will write, so that
// projection widens to the table's full non-generated column set (see
// [insertableColumns]); an empty declared set against an empty table stays a
// clean no-op because the read returns no rows either way.
func readBack(
	dialect string,
	names identifier.Semantics,
	liveTable *catalog.Table,
	declaration schemamodel.ManagedData,
	desired []map[string]any,
	intent Intent,
	qualified string,
) ([]string, []map[string]any, error) {
	if len(desired) == 0 {
		if intent != Write {
			return Columns(nil, declaration.Keys), desired, nil
		}
		if liveTable == nil {
			return nil, nil, fmt.Errorf(
				"cannot read the columns of managed table %q: it was not found in the live schema; create the table or remove the annotation",
				qualified)
		}
		columns, err := insertableColumns(dialect, names, qualified, *liveTable, declaration.Keys)
		return columns, desired, err
	}

	columns := Columns(desired, declaration.Keys)
	if liveTable == nil {
		return columns, desired, nil
	}
	narrowed := ProjectOntoLive(columns, liveTable, names)
	switch intent {
	case Write:
		return columns, desired, refuseUndeclaredColumns(qualified, columns, narrowed)
	case Plan:
		// The read stays narrow, because the column is not there to read yet;
		// the comparison keeps it, because the schema stage adds it before the
		// data statements run. Narrowing the declared rows here instead leaves
		// the value out of every statement, and a widened declaration converges
		// on a row whose new column the plan never wrote.
		return narrowed, desired, nil
	}
	// A column the declaration names and the table has not gained yet is
	// structural drift, and the structural comparison reports it. The count
	// covers the columns both sides carry: the live row holds no value for the
	// missing one, so no live value is at stake, and counting each row as an
	// overwrite would misstate what applying costs. The declared rows narrow
	// with the columns, so the comparison runs over one column set.
	if len(narrowed) == len(columns) {
		return narrowed, desired, nil
	}
	return narrowed, projectRows(desired, narrowed), nil
}

// declarationShape reads the table a declaration belongs to: the columns that
// reference that same table, and the type each column declares.
func declarationShape(
	desired *schemamodel.Database,
	declaration schemamodel.ManagedData,
) (references []string, columnTypes map[string]string) {
	if desired == nil {
		return nil, nil
	}
	for _, table := range desired.Tables {
		if table.StructName == declaration.StructName || table.Name == declaration.Table {
			return dataorder.SelfReferences(desired, table), dataorder.ColumnTypes(desired, table)
		}
	}
	return nil, nil
}
