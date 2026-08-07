package migrationlintreport

import (
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
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
func readBaselineColumns(
	conn *dbschema.DatabaseConnection,
	version int64,
	schemas []string,
) ([]lint.BaselineColumn, error) {
	schema, err := dbschema.ReadSchemaWithSchemas(conn, schemas)
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
func compatColumnDataType(column dbschematypes.DBColumn) string {
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
func numericDataType(column dbschematypes.DBColumn) string {
	if column.NumericPrecision == nil {
		return "numeric"
	}
	precision := "numeric(" + strconv.Itoa(*column.NumericPrecision)
	if column.NumericScale == nil || *column.NumericScale == 0 {
		return precision + ")"
	}
	return precision + "," + strconv.Itoa(*column.NumericScale) + ")"
}
