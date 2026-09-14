package generator

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/datadiff"
)

// BootstrapDataMarker introduces the statements a checkpoint carries for a
// reference table.
//
// A checkpoint that bootstraps data says so in its own bytes: an operator
// reading the file, and a reader deciding whether a fresh database will hold
// the rows its later migrations read, both need to see it without running
// anything.
const BootstrapDataMarker = "-- ptah:checkpoint-data"

// renderBootstrapData renders the rows of the named reference tables as they
// stand in the shadow database after the replay.
//
// The rows come from the shadow database rather than from a schema declaration,
// and that is the whole point: the replay has taken the database to the
// checkpoint's own version, so the rows there are the rows this history
// produces at this version. A generator that read today's declarations would
// bootstrap a database that no migration after the checkpoint was written
// against.
//
// Statement order is deterministic. [dbschema.ReadTableRows] applies no ORDER
// BY, and a checkpoint whose bytes depend on what the database felt like
// returning is one whose checksum nobody can reproduce.
func renderBootstrapData(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema *schemamodel.Database,
	tables []string,
) (string, error) {
	if len(tables) == 0 {
		return "", nil
	}
	var rendered strings.Builder
	for _, name := range tables {
		statements, err := renderBootstrapTable(ctx, conn, schema, name)
		if err != nil {
			return "", err
		}
		rendered.WriteString(statements)
	}
	return rendered.String(), nil
}

func renderBootstrapTable(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	schema *schemamodel.Database,
	name string,
) (string, error) {
	tableSchema, table := splitTableName(name)
	columns, keys, err := bootstrapColumns(schema, tableSchema, table)
	if err != nil {
		return "", err
	}
	rows, err := dbschema.ReadTableRows(ctx, conn, tableSchema, table, columns)
	if err != nil {
		return "", fmt.Errorf("checkpoint generation failed: read bootstrap rows for %s: %w", name, err)
	}
	sortBootstrapRows(rows, columns)
	up, _, err := datadiff.Render(&datadiff.DataDiff{
		Schema:  tableSchema,
		Table:   table,
		Keys:    keys,
		Inserts: rows,
	}, conn.Info().Dialect)
	if err != nil {
		return "", fmt.Errorf("checkpoint generation failed: render bootstrap rows for %s: %w", name, err)
	}
	header := fmt.Sprintf("%s %s rows=%d\n", BootstrapDataMarker, name, len(rows))
	if up == "" {
		return header, nil
	}
	return header + up, nil
}

// bootstrapColumns names what is read and what identifies a row.
//
// The columns are every column the table has at the checkpoint's version, and
// the keys are its primary key. A table with no primary key is refused rather
// than keyed on every column: a reference table whose rows have no identity is
// one this mechanism cannot promise to reproduce.
func bootstrapColumns(schema *schemamodel.Database, tableSchema, table string) (columns, keys []string, err error) {
	qualified := table
	if tableSchema != "" {
		qualified = tableSchema + "." + table
	}
	if schema == nil {
		return nil, nil, fmt.Errorf("checkpoint generation failed: no shadow schema to read %s from", qualified)
	}
	structName := ""
	found := false
	for _, candidate := range schema.Tables {
		if candidate.Name != table {
			continue
		}
		if tableSchema != "" && candidate.Schema != tableSchema {
			continue
		}
		structName = candidate.StructName
		found = true
		break
	}
	if !found {
		return nil, nil, fmt.Errorf(
			"checkpoint generation failed: bootstrap table %s does not exist at this version",
			qualified,
		)
	}
	// Fields carry the struct they were declared on rather than the table name,
	// which is how a schema read back from a database links them.
	for _, field := range schema.Fields {
		if field.StructName != structName {
			continue
		}
		columns = append(columns, field.Name)
		if field.Primary {
			keys = append(keys, field.Name)
		}
	}
	if len(columns) == 0 {
		return nil, nil, fmt.Errorf(
			"checkpoint generation failed: bootstrap table %s has no columns at this version",
			qualified,
		)
	}
	if len(keys) == 0 {
		return nil, nil, fmt.Errorf(
			"checkpoint generation failed: bootstrap table %s has no primary key, so its rows have no identity to reproduce",
			qualified,
		)
	}
	slices.Sort(columns)
	slices.Sort(keys)
	return columns, keys, nil
}

// sortBootstrapRows puts the rows in one order every run reproduces, by
// comparing their values column by column in sorted column order.
func sortBootstrapRows(rows []map[string]any, columns []string) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, column := range columns {
			left := fmt.Sprintf("%v", rows[i][column])
			right := fmt.Sprintf("%v", rows[j][column])
			if left != right {
				return left < right
			}
		}
		return false
	})
}

// splitTableName reads a "schema.table" or "table" spelling. Only the first dot
// separates, so a table name is never split into a schema it does not have.
func splitTableName(name string) (schema, table string) {
	trimmed := strings.TrimSpace(name)
	if before, after, found := strings.Cut(trimmed, "."); found {
		return before, after
	}
	return "", trimmed
}
