package atlasschema

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dataorder"
	"ptah.run/internal/managedrows"
	"ptah.run/migration/datadiff"
	"ptah.run/migration/safety"
)

// dataPhase is which of the three passes a statement belongs to. The order of
// the constants is the order the passes run in, and dataPhases is that order
// written once so a reader and the loop cannot disagree.
type dataPhase int

const (
	phaseInsert dataPhase = iota
	phaseUpdate
	phaseDelete
)

var dataPhases = []dataPhase{phaseInsert, phaseUpdate, phaseDelete}

// dataStatement is one statement reconciling declared rows, carrying the
// severity this package assigns it rather than the one a SQL analyzer would,
// and the pass it runs in.
//
// The analyzer reads a DELETE of a reference row as safe, because it does not
// remove a table or tighten a constraint. That is true about the schema and
// false about the rows, and a plan whose deletions read as safe is a plan an
// approval policy waves through.
type dataStatement struct {
	sql      string
	phase    dataPhase
	severity safety.Severity
	reason   string
}

// managedDataStatements reconciles every declared row set against the database.
//
// It runs whether or not the schema diff has changes: a release that only edits
// a reference row changes no DDL, and a planner that skipped the data stage
// then would report an empty plan for a change the author made.
//
// The statements come out in the order the foreign keys allow, not in the order
// the tables happen to be named: every INSERT and UPDATE parents-first, then
// every DELETE children-first. Grouping by table instead puts a child's rows in
// front of the parent's whenever the child sorts first — "countries" before
// "regions" — and the constraint the same plan just created refuses them
// (stokaro/ptah#3252). The rank is dataorder's, which is what the migration
// body reads, so a plan and a migration cannot disagree about which row goes in
// first.
func managedDataStatements(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	current *catalog.Database,
) ([]dataStatement, error) {
	if conn == nil || desired == nil || len(desired.ManagedData) == 0 {
		return nil, nil
	}
	ranker := dataorder.New(desired)
	declarations := slices.Clone(desired.ManagedData)
	// One order for the statements a plan records, so two runs over one
	// declaration produce one plan. Tables the schema does not define rank
	// together at the end and keep their names' order.
	sort.SliceStable(declarations, func(i, j int) bool {
		left, right := declarations[i], declarations[j]
		leftRank := ranker.Rank(left.Schema, left.Table)
		rightRank := ranker.Rank(right.Schema, right.Table)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return managedDataTableName(left) < managedDataTableName(right)
	})
	phases := make([][]dataStatement, len(dataPhases))
	for _, declaration := range declarations {
		declared, err := managedDataDiff(ctx, conn, declaration, current, managedDataSelfReferences(desired, declaration))
		if err != nil {
			return nil, err
		}
		for _, statement := range declared {
			phases[statement.phase] = append(phases[statement.phase], statement)
		}
	}
	// A deletion runs against the constraint from the other side: the row that
	// references has to go before the row it references, so this phase alone
	// reads the declaration order backwards.
	slices.Reverse(phases[phaseDelete])
	statements := make([]dataStatement, 0, len(declarations))
	for _, phase := range dataPhases {
		statements = append(statements, phases[phase]...)
	}
	return statements, nil
}

// managedDataSelfReferences names the declaration's columns that reference its
// own table, read from the table the declaration belongs to.
//
// A declaration carries a struct name and a table name; the fields that declare
// the foreign keys belong to the table, so the table has to be found before the
// columns can be. A declaration whose table the desired state does not define
// has no references to read, and the rows keep their key order.
func managedDataSelfReferences(desired *schemamodel.Database, declaration schemamodel.ManagedData) []string {
	for _, table := range desired.Tables {
		if table.StructName == declaration.StructName || table.Name == declaration.Table {
			return dataorder.SelfReferences(desired, table)
		}
	}
	return nil
}

func managedDataDiff(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	declaration schemamodel.ManagedData,
	current *catalog.Database,
	selfReferences []string,
) ([]dataStatement, error) {
	qualified := managedDataTableName(declaration)
	if declaration.Rows == nil {
		return nil, fmt.Errorf(
			"managed data for table %s was never read; a desired state that declares rows has to carry them",
			qualified,
		)
	}
	if len(declaration.Keys) == 0 {
		return nil, fmt.Errorf("managed data for table %s declares no key column", qualified)
	}
	desiredRows, err := managedDesiredRows(declaration)
	if err != nil {
		return nil, err
	}
	// A table this plan is about to create holds nothing, and reading it would
	// fail rather than answer. Every declared row is an insert then.
	liveRows := []map[string]any(nil)
	if liveTable := managedrows.LiveTable(current, declaration.Schema, declaration.Table); liveTable != nil {
		// Only the columns the table already has. The plan may be about to add
		// one the declaration names, and asking the server for it before the
		// DDL runs stops the whole reconciliation with 42703
		// (stokaro/ptah#3260); the value still reaches the plan, as the INSERT
		// or UPDATE of a column the live row does not carry.
		columns := managedrows.ProjectOntoLive(managedrows.Columns(desiredRows, declaration.Keys), liveTable)
		liveRows, err = dbschema.ReadTableRows(ctx, conn, declaration.Schema, declaration.Table, columns)
		if err != nil {
			return nil, fmt.Errorf("read managed rows of %s: %w", qualified, err)
		}
	}
	diff, err := datadiff.Compute(declaration.Schema, declaration.Table, declaration.Keys, desiredRows, liveRows)
	if err != nil {
		return nil, fmt.Errorf("compare managed rows of %s: %w", qualified, err)
	}
	// The table order above puts a table after the tables it references. A row
	// can reference a row of its own table -- a category tree, an org chart --
	// and that order is inside one table, so it is decided here: parents first
	// to write, children first to remove (stokaro/ptah#3266).
	diff.Inserts = dataorder.Rows(diff.Inserts, declaration.Keys, selfReferences)
	diff.Deletes = dataorder.Rows(diff.Deletes, declaration.Keys, selfReferences)
	slices.Reverse(diff.Deletes)
	// The statements come from the renderer as a list. Its script form cannot
	// be cut back into statements at line breaks: a declared value may carry a
	// newline, which is legal inside a literal and is the byte the script joins
	// statements with (stokaro/ptah#3278).
	up, _, err := datadiff.RenderStatements(diff, conn.Info().Dialect)
	if err != nil {
		return nil, fmt.Errorf("render managed rows of %s: %w", qualified, err)
	}
	return classifyDataStatements(up, qualified), nil
}

// classifyDataStatements assigns the severity a reference-row change actually
// carries.
//
// An INSERT adds what the declaration asks for. An UPDATE overwrites a value
// the database holds, and a DELETE removes a row entirely: both are losses a
// policy must be able to refuse, and neither is destructive to the schema, so
// nothing in the DDL analyzer would have said so.
//
// Each element of up is one whole statement, so its leading keyword is the
// statement's own and not that of a fragment cut out of a literal.
func classifyDataStatements(up []string, qualified string) []dataStatement {
	statements := make([]dataStatement, 0, len(up))
	for _, statement := range up {
		trimmed := strings.TrimSpace(statement)
		declared := dataStatement{
			sql:      trimmed,
			phase:    phaseInsert,
			severity: safety.Safe,
			reason:   fmt.Sprintf("inserts declared rows into %s", qualified),
		}
		switch {
		case strings.HasPrefix(strings.ToUpper(trimmed), "DELETE"):
			declared.phase = phaseDelete
			declared.severity = safety.Destructive
			declared.reason = fmt.Sprintf("removes a row from %s that the declaration no longer holds", qualified)
		case strings.HasPrefix(strings.ToUpper(trimmed), "UPDATE"):
			declared.phase = phaseUpdate
			declared.severity = safety.Warning
			declared.reason = fmt.Sprintf("overwrites managed columns of a row in %s", qualified)
		}
		statements = append(statements, declared)
	}
	return statements
}

// managedDesiredRows converts declared values into the Go values the diff
// compares and the renderer writes.
//
// The tag decides: a value declared as `007` is the integer 7 in an integer
// column, and one declared as "007" is the three characters. Carrying the tag
// this far is what keeps those apart.
func managedDesiredRows(declaration schemamodel.ManagedData) ([]map[string]any, error) {
	rows := make([]map[string]any, 0, len(declaration.Rows))
	for index, declared := range declaration.Rows {
		row := make(map[string]any, len(declared))
		for column, value := range declared {
			converted, err := managedValue(value)
			if err != nil {
				return nil, fmt.Errorf(
					"managed data for table %s, row %d, column %q: %w",
					managedDataTableName(declaration), index+1, column, err,
				)
			}
			row[column] = converted
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func managedValue(value schemamodel.ManagedValue) (any, error) {
	if value.Null {
		return nil, nil
	}
	switch value.Tag {
	case "str", "timestamp", "":
		return value.Text, nil
	case "int":
		parsed, err := strconv.ParseInt(value.Text, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is tagged as an integer and is not one", value.Text)
		}
		return parsed, nil
	case "float":
		parsed, err := strconv.ParseFloat(value.Text, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is tagged as a float and is not one", value.Text)
		}
		return parsed, nil
	case "bool":
		parsed, err := strconv.ParseBool(value.Text)
		if err != nil {
			return nil, fmt.Errorf("%q is tagged as a boolean and is not one", value.Text)
		}
		return parsed, nil
	default:
		return nil, fmt.Errorf("value carries the unsupported YAML tag %q", value.Tag)
	}
}

// managedDataColumns is what is read back from the database: the key columns
// and the managed ones, and nothing else.
//
// A column no declaration names is not read and never written. Reconciling a
// reference table is not permission to rewrite the columns beside it.
func managedDataTableName(declaration schemamodel.ManagedData) string {
	if declaration.Schema == "" {
		return declaration.Table
	}
	return declaration.Schema + "." + declaration.Table
}
