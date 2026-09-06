package migrationlintreport

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/schemalineage"
	"ptah.run/migration/lint"
)

// baselineState is the schema state one migration version starts from, in
// the three forms the linter looks things up in.
type baselineState struct {
	columns    []lint.BaselineColumn
	indexes    []lint.BaselineIndex
	dependents []lint.BaselineDependent
}

// readBaselineState records the schema state one migration version starts
// from, in the form the linter can look columns, indexes and dependents up in.
//
// It is read from the dev database mid-replay, after every earlier migration has
// been applied and before this one is. That is the only place the retired half
// of a rename still exists: `ALTER TABLE users RENAME COLUMN id TO oid` says
// nothing about what `id` was, and after the statement runs there is no `id`
// left to ask about. One catalog read answers all three forms: the columns and
// the indexes are read off it directly, and the dependents are resolved from
// the view and routine bodies it carries.
func readBaselineState(ctx context.Context,
	conn *dbschema.DatabaseConnection,
	version int64,
	schemas []string,
	dialect string,
) (baselineState, error) {
	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, schemas)
	if err != nil {
		return baselineState{}, fmt.Errorf("read dev database schema: %w", err)
	}
	return baselineState{
		columns:    baselineColumnsOf(schema, version),
		indexes:    baselineIndexesOf(schema, version),
		dependents: baselineDependentsOf(schema, version, dialect),
	}, nil
}

// baselineColumnsOf lists every column of the read schema as the state
// version starts from.
func baselineColumnsOf(schema *catalog.Database, version int64) []lint.BaselineColumn {
	var columns []lint.BaselineColumn
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			columns = append(columns, lint.BaselineColumn{
				Version:        version,
				Schema:         table.Schema,
				Table:          table.Name,
				Name:           column.Name,
				DataType:       compatColumnDataType(column),
				ColumnType:     baselineTypeSpelling(column),
				Charset:        column.Charset,
				TableCharset:   table.Charset,
				Collation:      column.Collate,
				TableCollation: table.Collate,
				NotNull:        strings.EqualFold(strings.TrimSpace(column.IsNullable), "NO"),
				HasDefault:     column.ColumnDefault != nil && strings.TrimSpace(*column.ColumnDefault) != "",
			})
		}
	}
	return columns
}

// baselineIndexesOf lists every index of the read schema as the state
// version starts from, key parts included.
//
// A reader that describes key parts reports them in Parts, with the
// expression or prefix a part carries; one that reports only the legacy
// column list is read through that list, whole columns every one. A part the
// reader could not name is carried as the incompleteness it is, so no rule
// reads a partial key as the whole one.
func baselineIndexesOf(schema *catalog.Database, version int64) []lint.BaselineIndex {
	var indexes []lint.BaselineIndex
	for _, index := range schema.Indexes {
		indexes = append(indexes, lint.BaselineIndex{
			Version:    version,
			Schema:     index.Schema,
			Table:      index.TableName,
			Name:       index.Name,
			Parts:      baselineIndexParts(index),
			Unique:     index.IsUnique || index.IsPrimary,
			Primary:    index.IsPrimary,
			Partial:    strings.TrimSpace(index.Condition) != "",
			Incomplete: index.KeyPartsIncomplete,
		})
	}
	return indexes
}

func baselineIndexParts(index catalog.Index) []lint.BaselineIndexPart {
	if len(index.Parts) == 0 {
		parts := make([]lint.BaselineIndexPart, 0, len(index.Columns))
		for _, column := range index.Columns {
			parts = append(parts, lint.BaselineIndexPart{Column: column})
		}
		return parts
	}
	parts := make([]lint.BaselineIndexPart, 0, len(index.Parts))
	for _, part := range index.Parts {
		prefix, _ := strconv.Atoi(strings.TrimSpace(part.Prefix))
		column := part.Name
		if part.Expr != "" {
			column = ""
		}
		parts = append(parts, lint.BaselineIndexPart{Column: column, Prefix: prefix})
	}
	return parts
}

// baselineTypeSpelling is the column's type as the server spells it, for the
// rules that compare a column's type before and after a statement.
//
// MySQL and MariaDB report it whole in COLUMN_TYPE. PostgreSQL spreads it over
// information_schema: the width, precision, and scale sit in fields of their
// own, and only an array or a domain column carries format_type's spelling.
// The composition here writes what format_type would: `character varying(20)`,
// `numeric(10,2)`, `timestamp(3) without time zone`, `bit varying(8)`, and the
// catalog's own spelling for everything else.
func baselineTypeSpelling(column catalog.Column) string {
	if column.ColumnType != "" {
		return column.ColumnType
	}
	if column.FormattedType != "" {
		return column.FormattedType
	}
	dataType := strings.ToLower(strings.Join(strings.Fields(column.DataType), " "))
	switch dataType {
	case "character varying", "character", "bit", "bit varying":
		if column.CharacterMaxLength != nil {
			return dataType + "(" + strconv.Itoa(*column.CharacterMaxLength) + ")"
		}
	case "numeric":
		if column.NumericPrecision != nil {
			scale := 0
			if column.NumericScale != nil {
				scale = *column.NumericScale
			}
			return dataType + "(" + strconv.Itoa(*column.NumericPrecision) + "," + strconv.Itoa(scale) + ")"
		}
	case "timestamp without time zone", "timestamp with time zone", "time without time zone", "time with time zone":
		if column.DatetimePrecision != nil {
			base, zone, _ := strings.Cut(dataType, " ")
			return base + "(" + strconv.Itoa(*column.DatetimePrecision) + ") " + zone
		}
	case "interval":
		if column.DatetimePrecision != nil {
			return dataType + "(" + strconv.Itoa(*column.DatetimePrecision) + ")"
		}
	case "user-defined":
		return column.UDTName
	}
	return dataType
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

// baselineDependentsOf records what reads each column in the state one
// migration version starts from.
//
// It is the same read as [baselineColumnsOf] answered a second way: the
// catalog gives the view and routine bodies, and schemalineage resolves which
// columns they read. A drop can then say what it breaks instead of only that it
// deletes data, which is what #1270's criterion 9 asked for.
//
// What the analysis could not resolve is not reported here. An undecided view
// contributes no dependent, so the rule stays silent about it rather than
// naming a reader it did not establish -- the rule reports a fact, and the
// analysis's own undecided list is where the gaps are stated.
func baselineDependentsOf(schema *catalog.Database, version int64, dialect string) []lint.BaselineDependent {
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
	return dependents
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
