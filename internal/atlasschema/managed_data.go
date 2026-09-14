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
	"ptah.run/migration/datadiff"
	"ptah.run/migration/safety"
)

// dataStatement is one statement reconciling declared rows, carrying the
// severity this package assigns it rather than the one a SQL analyzer would.
//
// The analyzer reads a DELETE of a reference row as safe, because it does not
// remove a table or tighten a constraint. That is true about the schema and
// false about the rows, and a plan whose deletions read as safe is a plan an
// approval policy waves through.
type dataStatement struct {
	sql      string
	severity safety.Severity
	reason   string
}

// managedDataStatements reconciles every declared row set against the database.
//
// It runs whether or not the schema diff has changes: a release that only edits
// a reference row changes no DDL, and a planner that skipped the data stage
// then would report an empty plan for a change the author made.
func managedDataStatements(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
	current *catalog.Database,
) ([]dataStatement, error) {
	if conn == nil || desired == nil || len(desired.ManagedData) == 0 {
		return nil, nil
	}
	declarations := slices.Clone(desired.ManagedData)
	// One order for the statements a plan records, so two runs over one
	// declaration produce one plan.
	sort.SliceStable(declarations, func(i, j int) bool {
		if declarations[i].Schema != declarations[j].Schema {
			return declarations[i].Schema < declarations[j].Schema
		}
		return declarations[i].Table < declarations[j].Table
	})
	statements := make([]dataStatement, 0, len(declarations))
	for _, declaration := range declarations {
		declared, err := managedDataDiff(ctx, conn, declaration, current)
		if err != nil {
			return nil, err
		}
		statements = append(statements, declared...)
	}
	return statements, nil
}

func managedDataDiff(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	declaration schemamodel.ManagedData,
	current *catalog.Database,
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
	if managedDataTableExists(current, declaration) {
		columns := managedDataColumns(declaration, desiredRows)
		liveRows, err = dbschema.ReadTableRows(ctx, conn, declaration.Schema, declaration.Table, columns)
		if err != nil {
			return nil, fmt.Errorf("read managed rows of %s: %w", qualified, err)
		}
	}
	diff, err := datadiff.Compute(declaration.Schema, declaration.Table, declaration.Keys, desiredRows, liveRows)
	if err != nil {
		return nil, fmt.Errorf("compare managed rows of %s: %w", qualified, err)
	}
	up, _, err := datadiff.Render(diff, conn.Info().Dialect)
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
func classifyDataStatements(up, qualified string) []dataStatement {
	statements := make([]dataStatement, 0)
	for statement := range strings.SplitSeq(strings.TrimSpace(up), "\n") {
		trimmed := strings.TrimSpace(statement)
		if trimmed == "" {
			continue
		}
		declared := dataStatement{
			sql:      trimmed,
			severity: safety.Safe,
			reason:   fmt.Sprintf("inserts declared rows into %s", qualified),
		}
		switch {
		case strings.HasPrefix(strings.ToUpper(trimmed), "DELETE"):
			declared.severity = safety.Destructive
			declared.reason = fmt.Sprintf("removes a row from %s that the declaration no longer holds", qualified)
		case strings.HasPrefix(strings.ToUpper(trimmed), "UPDATE"):
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
func managedDataColumns(declaration schemamodel.ManagedData, rows []map[string]any) []string {
	columns := make(map[string]struct{}, len(declaration.Keys))
	for _, key := range declaration.Keys {
		columns[key] = struct{}{}
	}
	for _, row := range rows {
		for column := range row {
			columns[column] = struct{}{}
		}
	}
	names := make([]string, 0, len(columns))
	for column := range columns {
		names = append(names, column)
	}
	sort.Strings(names)
	return names
}

func managedDataTableExists(current *catalog.Database, declaration schemamodel.ManagedData) bool {
	if current == nil {
		return false
	}
	for _, table := range current.Tables {
		if table.Name != declaration.Table {
			continue
		}
		if declaration.Schema != "" && table.Schema != declaration.Schema {
			continue
		}
		return true
	}
	return false
}

func managedDataTableName(declaration schemamodel.ManagedData) string {
	if declaration.Schema == "" {
		return declaration.Table
	}
	return declaration.Schema + "." + declaration.Table
}
