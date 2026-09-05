package migrationlintreport

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/schemalineage"
	"go.5x5.cz/ptah/migration/lint"
)

// readBaselineColumns records the schema state one migration version starts
// from, in the form the linter can look columns up in.
//
// It is read from the dev database mid-replay, after every earlier migration has
// been applied and before this one is. That is the only place the retired half
// of a rename still exists: `ALTER TABLE users RENAME COLUMN id TO oid` says
// nothing about what `id` was, and after the statement runs there is no `id`
// left to ask about.
func readBaselineColumns(ctx context.Context,
	conn *dbschema.DatabaseConnection,
	version int64,
	schemas []string,
) ([]lint.BaselineColumn, error) {
	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, schemas)
	if err != nil {
		return nil, fmt.Errorf("read dev database schema: %w", err)
	}
	var columns []lint.BaselineColumn
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			columns = append(columns, lint.BaselineColumn{
				Version:    version,
				Schema:     table.Schema,
				Table:      table.Name,
				Name:       column.Name,
				DataType:   compatColumnDataType(column),
				NotNull:    strings.EqualFold(strings.TrimSpace(column.IsNullable), "NO"),
				HasDefault: column.ColumnDefault != nil && strings.TrimSpace(*column.ColumnDefault) != "",
			})
		}
	}
	return columns, nil
}

// compatColumnDataType renders the type name the compatibility surface prints
// for one introspected column, or empty when that spelling has not been
// measured.
//
// The catalog's own type name is what the diagnostic carries for the types
// below -- `int` reads back as `integer`, `varchar(20)` as `character
// varying(20)` -- each measured against the pinned community binary v1.3.0 on
// PostgreSQL 16 (stokaro/ptah#1074). Types whose diagnostic spelling is NOT the
// catalog's return empty rather than a plausible guess: `timestamptz` reads back
// from the catalog as `timestamp without time zone`'s neighbor `timestamp with
// time zone` while the diagnostic says `timestamptz`, and an array column reads
// back as the bare category `ARRAY`. An empty spelling still reports the
// diagnostic, under Ptah's own labeled wording, which is the renderer's existing
// answer to a type it cannot spell the other tool's way.
func compatColumnDataType(column catalog.Column) string {
	dataType := strings.ToLower(strings.Join(strings.Fields(column.DataType), " "))
	switch dataType {
	case "character", "character varying":
		if column.CharacterMaxLength == nil {
			return dataType
		}
		return dataType + "(" + strconv.Itoa(*column.CharacterMaxLength) + ")"
	case "numeric":
		return numericDataType(column)
	case "bigint", "boolean", "bytea", "date", "double precision",
		"integer", "json", "jsonb", "real", "smallint", "text", "uuid":
		return dataType
	}
	return ""
}

// numericDataType spells a numeric column the way the diagnostic does: bare when
// the type constrains nothing, `numeric(p)` when it constrains precision alone,
// and `numeric(p,s)` when it constrains both. Measured: `numeric(10)` and
// `numeric(10,0)` both print `numeric(10)`, so a zero scale is not printed.
func numericDataType(column catalog.Column) string {
	if column.NumericPrecision == nil {
		return "numeric"
	}
	precision := "numeric(" + strconv.Itoa(*column.NumericPrecision)
	if column.NumericScale == nil || *column.NumericScale == 0 {
		return precision + ")"
	}
	return precision + "," + strconv.Itoa(*column.NumericScale) + ")"
}

// readBaselineDependents records what reads each column in the state one
// migration version starts from.
//
// It is the same read as [readBaselineColumns] answered a second way: the
// catalog gives the view and routine bodies, and schemalineage resolves which
// columns they read. A drop can then say what it breaks instead of only that it
// deletes data, which is what #1270's criterion 9 asked for.
//
// What the analysis could not resolve is not reported here. An undecided view
// contributes no dependent, so the rule stays silent about it rather than
// naming a reader it did not establish -- the rule reports a fact, and the
// analysis's own undecided list is where the gaps are stated.
func readBaselineDependents(ctx context.Context,
	conn *dbschema.DatabaseConnection,
	version int64,
	schemas []string,
	dialect string,
) ([]lint.BaselineDependent, error) {
	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, schemas)
	if err != nil {
		return nil, fmt.Errorf("read dev database schema: %w", err)
	}
	desired := dbschematogo.ConvertDBSchemaToGoSchema(schema, dialect)
	tableSchemas := tableSchemasByName(schema)

	dependents := make([]lint.BaselineDependent, 0)
	for _, edge := range schemalineage.Derive(desired).Edges {
		dependents = append(dependents, lint.BaselineDependent{
			Version: version, Schema: tableSchemas[edge.FromTable],
			Table: edge.FromTable, Column: edge.FromColumn,
			Dependent: edge.ToView, Kind: viewKinds[edge.Materialized],
		})
	}
	for _, read := range schemalineage.DeriveRoutines(desired, dialect).Reads {
		dependents = append(dependents, lint.BaselineDependent{
			Version: version, Schema: tableSchemas[read.Table],
			Table: read.Table, Column: read.Column,
			Dependent: read.ByRoutine, Kind: routineKind(read.Kind),
		})
	}
	return dependents, nil
}

// tableSchemasByName maps each table to the schema the server spells it in, so
// a dependent carries the same qualification a baseline column does.
func tableSchemasByName(schema *catalog.Database) map[string]string {
	schemas := make(map[string]string, len(schema.Tables))
	for _, table := range schema.Tables {
		schemas[table.Name] = table.Schema
	}
	return schemas
}

// viewKinds names a view in the words an operator uses, keyed by whether it is
// materialized.
var viewKinds = map[bool]string{false: "view", true: "materialized view"}

// routineKind names a routine, defaulting to the family a declaration without
// one belongs to.
func routineKind(kind string) string {
	if kind == "" {
		return "function"
	}
	return kind
}
