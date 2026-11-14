package migrationlintreport

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/lintext"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/schemalineage"
)

// baselineRuntime converts a replayed catalog and reads the routine bodies it
// holds, which is what resolving the baseline's dependents takes.
type baselineRuntime interface {
	dbschematogo.Runtime
	schemalineage.Bodies
}

// baselineState is the schema state one migration version starts from, in
// the forms the linter looks things up in.
type baselineState struct {
	columns     []lintext.BaselineColumn
	indexes     []lintext.BaselineIndex
	dependents  []lintext.BaselineDependent
	hypertables []lintext.BaselineHypertable
	objects     []lintext.BaselineObject
	// read names the version and the kinds of object the read lists in full.
	read lintext.BaselineRead
}

// readBaselineState records the schema state one migration version starts
// from, in the form the linter can look columns, indexes and dependents up in.
//
// It is read from the dev database mid-replay, after every earlier migration has
// been applied and before this one is. That is the only place the retired half
// of a rename still exists: `ALTER TABLE users RENAME COLUMN id TO oid` says
// nothing about what `id` was, and after the statement runs there is no `id`
// left to ask about. One catalog read answers every form: the columns, the
// indexes and the TimescaleDB hypertables are read off it directly, and the
// dependents are resolved from the view and routine bodies it carries.
func readBaselineState(ctx context.Context,
	conn *dbschema.DatabaseConnection,
	version int64,
	schemas []string,
	runtime baselineRuntime,
) (baselineState, error) {
	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, schemas)
	if err != nil {
		return baselineState{}, fmt.Errorf("read dev database schema: %w", err)
	}
	dependents, err := baselineDependentsOf(ctx, schema, version, conn.Info().Dialect, runtime)
	if err != nil {
		return baselineState{}, fmt.Errorf("convert dev database baseline: %w", err)
	}
	info := conn.Info()
	listed := listedObjectKinds(schema, info.Dialect)
	return baselineState{
		columns:     baselineColumnsOf(schema, version),
		indexes:     baselineIndexesOf(schema, version),
		dependents:  dependents,
		hypertables: baselineHypertablesOf(schema, version),
		objects:     baselineObjectsOf(schema, version, info.Schema, listed),
		read:        lintext.BaselineRead{Version: version, Listed: listed},
	}, nil
}

// listedObjectKinds are the kinds of object the read lists in full: those the
// dev database's dialect names in [descriptor.Lint.ListedObjects], less the
// types where this read did not describe every domain, composite type and
// range type. A kind the dialect names that the linter does not know is left
// out, which keeps every drop of it judged as a drop of something there.
func listedObjectKinds(schema *catalog.Database, dialect string) []lintext.BaselineObjectKind {
	described, _ := platform.Lookup(dialect)
	var listed []lintext.BaselineObjectKind
	for _, kind := range lintext.BaselineObjectKinds() {
		if !slices.Contains(described.Lint.ListedObjects, string(kind)) {
			continue
		}
		if kind == lintext.BaselineType && !typesDescribed(schema) {
			continue
		}
		listed = append(listed, kind)
	}
	return listed
}

// typesDescribed reports a read that described every user-defined type a DROP
// TYPE can name: the enums, which a read takes from the core catalog, and the
// domains, composite types and range types the type owner models, which a
// server without catalog dependencies does not let the read reach.
func typesDescribed(schema *catalog.Database) bool {
	for _, kind := range []coverage.Kind{coverage.Domain, coverage.Composite, coverage.Range} {
		if !schema.NotDescribed.Describes(kind) {
			return false
		}
	}
	for _, kind := range pgtype.Kinds() {
		if schema.FeatureCoverage.Lookup(kind, objectidentity.ID{Kind: objectidentity.Kind(kind)}).State != schemaext.Complete {
			return false
		}
	}
	return true
}

// baselineObjectsOf lists the objects of the read schema, of the kinds listed,
// as the state version starts from.
//
// An object of own, the schema the connection resolves unqualified names
// against, is written with no schema, the way the readers write the tables
// beside it. The MySQL reader names the database on a view and on no table,
// and a state with a schema on one row and none on the others reads as a state
// of several schemas, which matches no qualified reference to the tables.
func baselineObjectsOf(schema *catalog.Database, version int64, own string, listed []lintext.BaselineObjectKind) []lintext.BaselineObject {
	var objects []lintext.BaselineObject
	for _, kind := range listed {
		for _, object := range objectsOfKind(schema, kind) {
			if object.Schema == own {
				object.Schema = ""
			}
			object.Version, object.Kind = version, kind
			objects = append(objects, object)
		}
	}
	return objects
}

// objectsOfKind lists every object of one kind the read schema holds, with no
// version and no kind set.
//
// Two kinds are not read off one list. A continuous aggregate is dropped with
// DROP MATERIALIZED VIEW, and the reader keeps it out of both view lists and
// records it as a TimescaleDB object. A sequence that a serial or identity
// column owns is described on that column ([catalog.Column.OwnedSequence])
// and not in [catalog.Database.Sequences]; DROP SEQUENCE ... CASCADE drops
// it and the column's default with it. Without either source, the state shows
// the object absent, and its IF EXISTS drop reads as a statement that changes
// nothing.
func objectsOfKind(schema *catalog.Database, kind lintext.BaselineObjectKind) []lintext.BaselineObject {
	var objects []lintext.BaselineObject
	switch kind {
	case lintext.BaselineType:
		for _, enum := range schema.Enums {
			objects = append(objects, lintext.BaselineObject{Schema: enum.Schema, Name: enum.Name, Enum: true, Values: slices.Clone(enum.Values)})
		}
		objects = append(objects, featureObjectsOf(schema, pgtype.Kinds()...)...)
	case lintext.BaselineView:
		for _, view := range schema.Views {
			objects = append(objects, lintext.BaselineObject{Schema: view.Schema, Name: view.Name})
		}
	case lintext.BaselineMaterializedView:
		for _, view := range schema.MatViews {
			objects = append(objects, lintext.BaselineObject{Schema: view.Schema, Name: view.Name})
		}
		objects = append(objects, featureObjectsOf(schema, tsschema.ContinuousAggregateKind)...)
	case lintext.BaselineTrigger:
		for _, trigger := range schema.Triggers {
			objects = append(objects, lintext.BaselineObject{Schema: trigger.Schema, Table: trigger.Table, Name: trigger.Name})
		}
	case lintext.BaselineSequence:
		for _, sequence := range schema.Sequences {
			objects = append(objects, lintext.BaselineObject{Schema: sequence.Schema, Name: sequence.Name})
		}
		objects = append(objects, columnSequencesOf(schema)...)
	}
	return objects
}

// featureObjectsOf lists the extension objects of the read schema whose kind
// is one of kinds.
func featureObjectsOf(schema *catalog.Database, kinds ...schemaext.Kind) []lintext.BaselineObject {
	var objects []lintext.BaselineObject
	for _, ref := range schema.FeatureObjects.Refs() {
		if slices.Contains(kinds, schemaext.Kind(ref.Kind)) {
			objects = append(objects, lintext.BaselineObject{Schema: ref.Schema.Authored(), Name: ref.Name.Source})
		}
	}
	return objects
}

// columnSequencesOf lists the sequences the columns of the read schema own.
// Such a sequence is in its column's schema.
func columnSequencesOf(schema *catalog.Database) []lintext.BaselineObject {
	var objects []lintext.BaselineObject
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			if column.OwnedSequence != "" {
				objects = append(objects, lintext.BaselineObject{Schema: table.Schema, Name: column.OwnedSequence})
			}
		}
	}
	return objects
}

// baselineHypertablesOf lists the TimescaleDB hypertables of the read schema as
// the state version starts from: the tables carrying hypertable settings, and
// the ones the read could only record as hypertables whose dimension it did
// not see. The reader asks the extension's catalog only where the extension is
// installed, so a database without it yields none.
func baselineHypertablesOf(schema *catalog.Database, version int64) []lintext.BaselineHypertable {
	var hypertables []lintext.BaselineHypertable
	for _, table := range schema.Tables {
		if slices.Contains(table.Facets.Kinds(), tsschema.HypertableKind) {
			hypertables = append(hypertables, lintext.BaselineHypertable{Version: version, Schema: table.Schema, Table: table.Name})
		}
	}
	for _, record := range schema.FeatureCoverage.SubjectRecords() {
		if record.Kind == tsschema.HypertableKind && record.Subject.Kind == objectidentity.KindTable {
			hypertables = append(hypertables, lintext.BaselineHypertable{Version: version,
				Schema: tsschema.AuthoredSchema(record.Subject), Table: record.Subject.Name.Source})
		}
	}
	return hypertables
}

// baselineColumnsOf lists every column of the read schema as the state
// version starts from.
func baselineColumnsOf(schema *catalog.Database, version int64) []lintext.BaselineColumn {
	var columns []lintext.BaselineColumn
	for _, table := range schema.Tables {
		for _, column := range table.Columns {
			// The character set is the MySQL owner's observation of the
			// column; a column whose type carries none has none.
			settings, _, _ := mysqlschema.Settings(column.Facets)
			columns = append(columns, lintext.BaselineColumn{
				Version:          version,
				Schema:           table.Schema,
				Table:            table.Name,
				Name:             column.Name,
				DataType:         compatColumnDataType(column),
				ColumnType:       baselineTypeSpelling(column),
				Charset:          settings.Charset,
				TableCharset:     table.Charset,
				Collation:        column.Collate,
				TableCollation:   table.Collate,
				TablePartitioned: table.Partitioned,
				NotNull:          strings.EqualFold(strings.TrimSpace(column.IsNullable), "NO"),
				HasDefault:       column.ColumnDefault != nil && strings.TrimSpace(*column.ColumnDefault) != "",
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
func baselineIndexesOf(schema *catalog.Database, version int64) []lintext.BaselineIndex {
	var indexes []lintext.BaselineIndex
	for _, index := range schema.Indexes {
		indexes = append(indexes, lintext.BaselineIndex{
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

func baselineIndexParts(index catalog.Index) []lintext.BaselineIndexPart {
	if len(index.Parts) == 0 {
		parts := make([]lintext.BaselineIndexPart, 0, len(index.Columns))
		for _, column := range index.Columns {
			parts = append(parts, lintext.BaselineIndexPart{Column: column})
		}
		return parts
	}
	parts := make([]lintext.BaselineIndexPart, 0, len(index.Parts))
	for _, part := range index.Parts {
		prefix, _ := strconv.Atoi(strings.TrimSpace(part.Prefix))
		column := part.Name
		if part.Expr != "" {
			column = ""
		}
		parts = append(parts, lintext.BaselineIndexPart{Column: column, Prefix: prefix})
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
func baselineDependentsOf(ctx context.Context, schema *catalog.Database, version int64, dialect string, runtime baselineRuntime) ([]lintext.BaselineDependent, error) {
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, schema, dialect, runtime)
	if err != nil {
		return nil, err
	}
	tableSchemas := tableSchemasByName(schema)

	dependents := make([]lintext.BaselineDependent, 0)
	for _, edge := range schemalineage.Derive(desired).Edges {
		dependents = append(dependents, lintext.BaselineDependent{
			Version: version, Schema: tableSchemas[edge.FromTable],
			Table: edge.FromTable, Column: edge.FromColumn,
			Dependent: edge.ToView, Kind: viewKinds[edge.Materialized],
		})
	}
	routines, err := schemalineage.DeriveRoutines(ctx, runtime, desired, dialect)
	if err != nil {
		return nil, err
	}
	for _, read := range routines.Reads {
		dependents = append(dependents, lintext.BaselineDependent{
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
