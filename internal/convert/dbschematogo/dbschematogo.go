// Package dbschematogo converts an introspected database schema
// (dbschema/types.DBSchema) into the goschema entity model, so live databases
// can flow through the same diff and planning pipeline as annotated Go
// sources.
package dbschematogo

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

// ConvertDBSchemaToGoSchema converts a database schema to goschema format
// This is needed for down migrations where we use the current DB state as the target
func ConvertDBSchemaToGoSchema(dbSchema *dbschematypes.DBSchema) *goschema.Database {
	database := newDatabase()
	convertSchemas(database, dbSchema.Schemas)
	convertEnums(database, dbSchema.Enums)

	// Index single-column FOREIGN KEY constraints by table.column so the
	// reconstructed fields can carry the foreign reference and its referential
	// actions. This is what lets a down migration restore the prior ON DELETE /
	// ON UPDATE action of a field-level FK (issue #189): the down path treats
	// the introspected (pre-change) database as the target, so the old action
	// must survive the round-trip into goschema.
	fkByColumn := indexForeignKeysByColumn(dbSchema)
	tablePrimaryKeys := primaryKeysByTable(dbSchema)
	tablePKColumns := primaryKeyColumnSets(tablePrimaryKeys)
	tableStructNames := convertTablesAndFields(database, dbSchema, fkByColumn, tablePrimaryKeys, tablePKColumns)

	database.Indexes = convertIndexes(dbSchema, tableStructNames)
	database.Constraints = convertConstraints(dbSchema, tableStructNames)
	clearColumnUniqueForNamedConstraints(database)
	convertExtensions(database, dbSchema.Extensions)
	convertRLSPolicies(database, dbSchema.RLSPolicies, tableStructNames)
	convertFunctions(database, dbSchema.Functions)
	convertSequences(database, dbSchema.Sequences)
	convertUserTypes(database, dbSchema)
	convertViews(database, dbSchema.Views)
	convertMaterializedViews(database, dbSchema.MatViews)
	convertTriggers(database, dbSchema.Triggers)
	convertHypertables(database, dbSchema.Hypertables)
	convertContinuousAggregates(database, dbSchema.ContinuousAggregates)
	convertSynonyms(database, dbSchema.Synonyms)
	convertExtendedProperties(database, dbSchema.ExtendedProperties)
	convertRoles(database, dbSchema.Roles)
	database.Grants = convertGrants(dbSchema.Grants)
	convertRLSEnabledTables(database, dbSchema.Tables, tableStructNames)
	// What the read did not look at is part of what the read said. Dropping it
	// here would turn the reader's silence back into desired absence one
	// conversion after it was recorded (stokaro/ptah#1276).
	database.NotDescribed = dbSchema.NotDescribed

	return database
}

func convertSchemas(database *goschema.Database, schemas []dbschematypes.DBSchemaInfo) {
	for _, schema := range schemas {
		database.Schemas = append(database.Schemas, goschema.Schema{
			Name:    schema.Name,
			Comment: schema.Comment,
			Charset: schema.Charset,
			Collate: schema.Collate,
		})
	}
}

func newDatabase() *goschema.Database {
	return &goschema.Database{
		Schemas:           make([]goschema.Schema, 0),
		Tables:            make([]goschema.Table, 0),
		Fields:            make([]goschema.Field, 0),
		Indexes:           make([]goschema.Index, 0),
		Constraints:       make([]goschema.Constraint, 0),
		Enums:             make([]goschema.Enum, 0),
		Extensions:        make([]goschema.Extension, 0),
		Functions:         make([]goschema.Function, 0),
		Sequences:         make([]goschema.Sequence, 0),
		Domains:           make([]goschema.Domain, 0),
		CompositeTypes:    make([]goschema.CompositeType, 0),
		Ranges:            make([]goschema.Range, 0),
		Views:             make([]goschema.View, 0),
		MaterializedViews: make([]goschema.MaterializedView, 0),
		Triggers:          make([]goschema.Trigger, 0),
		RLSPolicies:       make([]goschema.RLSPolicy, 0),
		RLSEnabledTables:  make([]goschema.RLSEnabledTable, 0),
		Roles:             make([]goschema.Role, 0),
		Grants:            make([]goschema.Grant, 0),
		Dependencies:      make(map[string][]string),
	}
}

// convertEnums carries the schema the reader recorded, exactly as the domain,
// composite and range conversions below do.
//
// Dropping it made every enum in the result belong to whatever schema the
// consumer defaulted to. On a read covering more than one schema that is a
// claim about a type the database does not hold: `extra.mood` was described as
// `public.mood`, applying the description built the type in `public`, and the
// column in `extra` that uses it was typed against it (stokaro/ptah#1276).
func convertEnums(database *goschema.Database, dbEnums []dbschematypes.DBEnum) {
	for _, dbEnum := range dbEnums {
		database.Enums = append(database.Enums, goschema.Enum{
			Name:   dbEnum.Name,
			Schema: dbEnum.Schema,
			Values: dbEnum.Values,
		})
	}
}

func convertTablesAndFields(
	database *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	fkByColumn map[tableMemberKey]foreignKeyInfo,
	tablePrimaryKeys map[string]tablePrimaryKey,
	tablePKColumns map[string]map[string]bool,
) map[string]string {
	tableStructNames := make(map[string]string, len(dbSchema.Tables))
	tableNameCounts := tableNameCounts(dbSchema.Tables)
	for _, dbTable := range dbSchema.Tables {
		structName := dbTableStructName(dbTable, tableNameCounts)
		tableStructNames[dbTable.QualifiedName()] = structName
		primaryKey := tablePrimaryKeys[dbTable.QualifiedName()]

		table := goschema.Table{
			StructName: structName,
			Name:       dbTable.Name,
			Schema:     dbTable.Schema,
			Comment:    dbTable.Comment,
			PrimaryKey: primaryKey.columns,
			// The payload rides with the key it belongs to. Nothing else can
			// carry it: convertConstraint refuses a PRIMARY KEY outright so the
			// key renders once, and the column flag has no slot for it.
			PrimaryKeyInclude: primaryKey.include,
			Strict:            dbTable.Strict,
			WithoutRowID:      dbTable.WithoutRowID,
			// A virtual table's module declaration is what recreates it.
			// Dropping it here is what made `ptah db read` describe an FTS5
			// index as an ordinary table. See stokaro/ptah#1028.
			VirtualModule:    dbTable.VirtualModule,
			VirtualArguments: dbTable.VirtualArguments,
			// Cloned so the description and the declaration built from it do
			// not share a pointer; a caller mutating one must not reach the
			// other (stokaro/ptah#1027).
			RowTTL:    dbTable.RowTTL.Clone(),
			Overrides: clickHouseTableOverrides(dbTable),
		}
		database.Tables = append(database.Tables, table)

		// Convert columns to fields
		for _, dbColumn := range dbTable.Columns {
			field := goschema.Field{
				StructName:         structName,
				FieldName:          generateFieldName(dbColumn.Name),
				Name:               dbColumn.Name,
				Type:               goSchemaFieldType(dbColumn),
				Comment:            dbColumn.Comment,
				TypeIsDeclaredText: dbColumn.TypeIsDeclaredText,
				Nullable:           dbColumn.IsNullable == "YES",
				Primary:            dbColumn.IsPrimaryKey && !tablePKColumns[dbTable.QualifiedName()][dbColumn.Name],
				AutoInc:            dbColumn.IsAutoIncrement,
				Unique:             dbColumn.IsUnique,
				Charset:            dbColumn.Charset,
				Collate:            dbColumn.Collate,
				GeneratedKind:      dbColumn.GeneratedKind,
				IdentityGeneration: dbColumn.IdentityGeneration,
			}
			if dbColumn.GeneratedExpression != nil {
				field.GeneratedExpression = *dbColumn.GeneratedExpression
			}

			if dbColumn.ColumnDefault != nil && postgresSerialType(dbColumn) == "" {
				setFieldDefaultFromDB(&field, *dbColumn.ColumnDefault)
			}

			// Carry the field-level foreign key (reference + referential actions)
			// so down migrations can reconstruct it with the prior action.
			if fk, ok := fkByColumn[tableMemberKey{table: dbTable.QualifiedName(), member: dbColumn.Name}]; ok {
				field.Foreign = fk.foreign
				field.ForeignKeyName = fk.name
				field.OnDelete = fk.onDelete
				field.Deferrable = fk.deferrable
				field.Initially = fk.initially
				field.OnUpdate = fk.onUpdate
			}

			database.Fields = append(database.Fields, field)
		}
	}
	return tableStructNames
}

func tableNameCounts(tables []dbschematypes.DBTable) map[string]int {
	counts := make(map[string]int, len(tables))
	for _, table := range tables {
		counts[table.Name]++
	}
	return counts
}

func dbTableStructName(table dbschematypes.DBTable, tableNameCounts map[string]int) string {
	if tableNameCounts[table.Name] > 1 && strings.TrimSpace(table.Schema) != "" {
		return generateStructName(table.Schema + "_" + table.Name)
	}
	return generateStructName(table.Name)
}

func convertIndexes(dbSchema *dbschematypes.DBSchema, tableStructNames map[string]string) []goschema.Index {
	constraintBackedIndexes := constraintBackedIndexesByTable(dbSchema)
	indexes := make([]goschema.Index, 0, len(dbSchema.Indexes))
	for _, dbIndex := range dbSchema.Indexes {
		// Skip primary key indexes as they're handled by primary key fields
		if dbIndex.IsPrimary {
			continue
		}
		if _, ok := constraintBackedIndexes[tableMemberKey{table: dbIndex.QualifiedTableName(), member: dbIndex.Name}]; ok {
			continue
		}

		index := goschema.Index{
			StructName:    structNameForTable(tableStructNames, dbIndex.QualifiedTableName(), dbIndex.TableName),
			Name:          dbIndex.Name,
			TableName:     dbIndex.QualifiedTableName(),
			Fields:        dbIndex.Columns,
			Parts:         convertIndexParts(dbIndex.Parts),
			Unique:        dbIndex.IsUnique,
			Condition:     dbIndex.Condition,
			Comment:       dbIndex.Comment,
			NullsDistinct: cloneBoolPtr(dbIndex.NullsDistinct),
			Type:          indexType(dbIndex),
			Granularity:   dbIndex.Granularity,

			IncludeColumns: slices.Clone(dbIndex.IncludeColumns),
			StorageParams:  maps.Clone(dbIndex.StorageParams),
			// Carried rather than recomputed: only the reader has the catalog,
			// and an operator class the index's own DDL leaves implicit is
			// reachable no other way.
			RequiresExtensions: slices.Clone(dbIndex.RequiresExtensions),
		}
		indexes = append(indexes, index)
	}
	return indexes
}

// indexType picks the value goschema.Index.Type carries for an introspected
// index. goschema keeps one field for two concepts the database layer keeps
// apart: the PostgreSQL access method (btree/gin/gist/brin/hash) and the
// ClickHouse data-skipping-index type (minmax/bloom_filter/...). No reader
// sets both, so the choice is unambiguous.
func indexType(index dbschematypes.DBIndex) string {
	if index.Method != "" {
		return index.Method
	}
	return index.Type
}

func convertIndexParts(parts []dbschematypes.DBIndexPart) []goschema.IndexPart {
	if len(parts) == 0 {
		return nil
	}
	converted := make([]goschema.IndexPart, len(parts))
	for position, part := range parts {
		converted[position] = goschema.IndexPart{
			Name:       part.Name,
			Expr:       part.Expr,
			Operator:   part.Operator,
			Prefix:     part.Prefix,
			Desc:       part.Desc,
			NullsOrder: part.NullsOrder,
		}
	}
	return converted
}

func convertExtensions(database *goschema.Database, dbExtensions []dbschematypes.DBExtension) {
	for _, dbExtension := range dbExtensions {
		extension := goschema.Extension{
			Name:        dbExtension.Name,
			Schema:      dbExtension.Schema,
			IfNotExists: true, // Default to true for down migrations for safety
			Version:     dbExtension.Version,
			// Carried rather than recomputed: only the reader has the catalog,
			// and the Atlas-compatible renderer needs it to tell an extension
			// nothing depends on from one a column type still needs.
			Provides: dbExtension.Provides,
		}

		// Set comment if available
		if dbExtension.Comment != nil {
			extension.Comment = *dbExtension.Comment
		}

		database.Extensions = append(database.Extensions, extension)
	}
}

func convertRLSPolicies(
	database *goschema.Database,
	dbPolicies []dbschematypes.DBRLSPolicy,
	tableStructNames map[string]string,
) {
	for _, dbPolicy := range dbPolicies {
		policy := goschema.RLSPolicy{
			StructName:          structNameForTable(tableStructNames, dbPolicy.Table, dbPolicy.Table),
			Name:                dbPolicy.Name,
			Table:               dbPolicy.Table,
			PolicyFor:           dbPolicy.PolicyFor,
			ToRoles:             dbPolicy.ToRoles,
			UsingExpression:     dbPolicy.UsingExpression,
			WithCheckExpression: dbPolicy.WithCheckExpression,
			Comment:             dbPolicy.Comment,
		}
		database.RLSPolicies = append(database.RLSPolicies, policy)
	}
}

// convertFunctions carries the schema the reader recorded, in Name, which is
// where goschema.Function keeps it -- the same place views and materialized
// views keep theirs, and the same place the HCL parser already writes it from a
// `function` block's `schema` attribute.
//
// Dropping it left the name unqualified, so the Atlas-compatible render wrote a
// `function` block with no schema attribute at all and an apply recreated the
// function in whatever schema the connection defaulted to. On a read covering
// more than one schema, `extra.f_extra` came back as `public.f_extra`
// (stokaro/ptah#1276).
func convertFunctions(database *goschema.Database, dbFunctions []dbschematypes.DBFunction) {
	for _, dbFunction := range dbFunctions {
		function := goschema.Function{
			StructName: "", // Functions are not associated with specific structs in DB schema
			Name:       dbFunction.QualifiedName(),
			// The kind travels with the routine. Dropping it here would turn
			// every read-back procedure into a function, which is what the
			// reader used to do by filtering them out (stokaro/ptah#1722).
			Kind:       dbFunction.Kind,
			Parameters: dbFunction.Parameters,
			Returns:    dbFunction.Returns,
			Language:   dbFunction.Language,
			Security:   dbFunction.Security,
			Volatility: dbFunction.Volatility,
			Body:       dbFunction.Body,
			Comment:    dbFunction.Comment,
		}
		database.Functions = append(database.Functions, function)
	}
}

func convertUserTypes(database *goschema.Database, dbSchema *dbschematypes.DBSchema) {
	for _, domain := range dbSchema.Domains {
		converted := goschema.Domain{
			Name:     domain.Name,
			Schema:   domain.Schema,
			BaseType: domain.BaseType,
			NotNull:  domain.NotNull,
			Check:    domain.Check,
		}
		setDomainDefaultFromDB(&converted, domain.Default)
		database.Domains = append(database.Domains, converted)
	}
	for _, composite := range dbSchema.Composites {
		fields := make([]goschema.CompositeTypeField, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			fields = append(fields, goschema.CompositeTypeField{Name: field.Name, Type: field.Type})
		}
		database.CompositeTypes = append(database.CompositeTypes, goschema.CompositeType{
			Name:   composite.Name,
			Schema: composite.Schema,
			Fields: fields,
		})
	}
	for _, rangeType := range dbSchema.Ranges {
		database.Ranges = append(database.Ranges, goschema.Range{
			Name:    rangeType.Name,
			Schema:  rangeType.Schema,
			Subtype: rangeType.Subtype,
		})
	}
}

func convertSequences(database *goschema.Database, dbSequences []dbschematypes.DBSequence) {
	for _, dbSequence := range dbSequences {
		database.Sequences = append(database.Sequences, goschema.Sequence{
			Name:      dbSequence.Name,
			Schema:    dbSequence.Schema,
			AsType:    dbSequence.DataType,
			Start:     dbSequence.Start,
			Increment: dbSequence.Increment,
			MinValue:  dbSequence.MinValue,
			MaxValue:  dbSequence.MaxValue,
			Cache:     dbSequence.Cache,
			Cycle:     dbSequence.Cycle,
			OwnedBy:   dbSequence.OwnedBy,
			Comment:   dbSequence.Comment,
		})
	}
}

// convertHypertables carries the TimescaleDB hypertables a read found into the
// IR, so a description says which tables are partitioned.
//
// Only the primary dimension is carried, because only it is declarable. A
// table with more than one is described with the first, and the note the reader
// emits says how many it did not describe.
func convertHypertables(database *goschema.Database, hypertables []dbschematypes.DBHypertable) {
	for _, hypertable := range hypertables {
		database.Hypertables = append(database.Hypertables, goschema.Hypertable{
			Table:         hypertable.QualifiedName(),
			Column:        hypertable.PrimaryDimension,
			ChunkInterval: hypertable.ChunkInterval,
		})
	}
}

// convertContinuousAggregates carries the TimescaleDB continuous aggregates a
// read found into the IR.
//
// The body is the catalog's `view_definition` rather than pg_get_viewdef's,
// which is what makes this conversion usable at all: pg_get_viewdef answers the
// rewritten definition, which selects from the materialization hypertable in a
// schema the extension owns, and a down migration built from it would create an
// aggregate over an internal relation.
func convertContinuousAggregates(
	database *goschema.Database,
	aggregates []dbschematypes.DBContinuousAggregate,
) {
	for _, aggregate := range aggregates {
		// The catalog always reports a value, so the converted declaration
		// carries a definite one: this description is what a DOWN migration
		// recreates the aggregate from, and leaving the option unset there
		// would take the server default rather than the value that was there.
		materializedOnly := aggregate.MaterializedOnly
		database.ContinuousAggregates = append(database.ContinuousAggregates, goschema.ContinuousAggregate{
			Name:             aggregate.Name,
			Schema:           aggregate.Schema,
			Body:             aggregate.Definition,
			MaterializedOnly: &materializedOnly,
		})
	}
}

// convertSynonyms carries the SQL Server synonyms a read found into the IR.
//
// Without it `ptah schema inspect` described none of them, in any format, even
// though the reader finds every one and the HCL surface has had a `synonym`
// block since stokaro/ptah#1031. The loss was between the read and the
// document, so nothing that renders from a hand-built schema could see it
// (stokaro/ptah#2001).
//
// The target is rebuilt from the PARSED parts rather than copied. `Target` is
// base_object_name exactly as the catalog records it, brackets included, and
// [go.5x5.cz/ptah/core/goschema.Synonym.Target] is the spelling that will be
// emitted: one to four dot-separated parts, unquoted. Copying the catalog's
// form would put `[other].[dbo].[gauge]` in a document and render it again as
// a name with brackets inside it.
func convertSynonyms(database *goschema.Database, synonyms []dbschematypes.DBSynonym) {
	for _, synonym := range synonyms {
		database.Synonyms = append(database.Synonyms, goschema.Synonym{
			Name:    synonym.Name,
			Schema:  synonym.Schema,
			Target:  synonymTarget(synonym),
			Comment: synonym.Comment,
		})
	}
}

// synonymTarget joins the parsed target parts back into the spelling a
// declaration uses. Absent leading parts are empty, so joining what is present
// gives the one-to-four part name without inventing a level.
func synonymTarget(synonym dbschematypes.DBSynonym) string {
	parts := make([]string, 0, 4)
	for _, part := range []string{
		synonym.TargetServer,
		synonym.TargetDatabase,
		synonym.TargetSchema,
		synonym.TargetObject,
	} {
		if strings.TrimSpace(part) == "" {
			continue
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		// A catalog row Ptah could not parse still names something, and the
		// raw form is better than nothing: it is what the server holds.
		return synonym.Target
	}
	return strings.Join(parts, ".")
}

// convertExtendedProperties carries the SQL Server extended properties a read
// found into the IR, except the ones no declaration could restore.
//
// A property whose value the server stores under a base type Ptah cannot write
// back must NOT become a declaration. The renderer emits an N” literal, so
// putting an int or a date into the document would change its type on the next
// apply, and CONVERT(NVARCHAR, …) on a date answers `Jan  2 2026` -- a
// locale-dependent rendering rather than the value. The comparator already
// declines those in both directions; describing one would undo that by turning
// the description into a declaration that asks for the string.
//
// The read still reports it, so it is not invisible: [dbschematypes.DBExtendedProperty]
// carries the row and the flag, and nothing is planned to remove it.
func convertExtendedProperties(
	database *goschema.Database,
	properties []dbschematypes.DBExtendedProperty,
) {
	for _, property := range properties {
		if property.ValueNotRepresentable {
			continue
		}
		database.ExtendedProperties = append(database.ExtendedProperties, goschema.ExtendedProperty{
			Name:   property.Name,
			Schema: property.Schema,
			Table:  property.Table,
			Column: property.Column,
			Value:  property.Value,
		})
	}
}

func convertViews(database *goschema.Database, dbViews []dbschematypes.DBView) {
	for _, dbView := range dbViews {
		database.Views = append(database.Views, goschema.View{
			Name:       dbView.QualifiedName(),
			Body:       dbView.Body,
			WithCheck:  strings.EqualFold(dbView.CheckOption, "LOCAL") || strings.EqualFold(dbView.CheckOption, "CASCADED"),
			Comment:    dbView.Comment,
			Attributes: dbView.Attributes,
		})
	}
}

func convertMaterializedViews(database *goschema.Database, dbViews []dbschematypes.DBMatView) {
	for _, dbView := range dbViews {
		materializedView := goschema.MaterializedView{
			Name:    dbView.QualifiedName(),
			Body:    dbView.Body,
			Comment: dbView.Comment,
			Refresh: dbView.Refresh.Clone(),
		}
		database.MaterializedViews = append(database.MaterializedViews, materializedView)
	}
}

func convertTriggers(database *goschema.Database, dbTriggers []dbschematypes.DBTrigger) {
	for _, dbTrigger := range dbTriggers {
		trigger := goschema.Trigger{
			Name:    dbTrigger.Name,
			Table:   dbTrigger.QualifiedTable(),
			Timing:  dbTrigger.Timing,
			Event:   dbTrigger.Event,
			ForEach: dbTrigger.ForEach,
			Body:    dbTrigger.Body,
			Comment: dbTrigger.Comment,
		}
		trigger.Canonicalize()
		database.Triggers = append(database.Triggers, trigger)
	}
}

func convertRoles(database *goschema.Database, dbRoles []dbschematypes.DBRole) {
	for _, dbRole := range dbRoles {
		role := goschema.Role{
			StructName:  "", // Roles are not associated with specific structs in DB schema
			Name:        dbRole.Name,
			Login:       dbRole.Login,
			Password:    "", // Not available in current DBRole for security
			Superuser:   dbRole.Superuser,
			CreateDB:    dbRole.CreateDB,
			CreateRole:  dbRole.CreateRole,
			Inherit:     dbRole.Inherit,
			Replication: dbRole.Replication,
			Comment:     dbRole.Comment,
		}
		database.Roles = append(database.Roles, role)
	}
}

func convertRLSEnabledTables(
	database *goschema.Database,
	dbTables []dbschematypes.DBTable,
	tableStructNames map[string]string,
) {
	for _, dbTable := range dbTables {
		if dbTable.RLSEnabled {
			rlsEnabledTable := goschema.RLSEnabledTable{
				StructName: structNameForTable(tableStructNames, dbTable.QualifiedName(), dbTable.Name),
				// Qualified, like every other table reference this file
				// produces -- convertRLSPolicies carries the reader's already
				// qualified name, convertViews and convertMaterializedViews use
				// QualifiedName, convertTriggers uses QualifiedTable.
				//
				// The bare name that used to be here resolves against the search
				// path, so a description of a table outside the connection's
				// default schema enabled row security on whatever `users` the
				// path found first -- leaving that table with no policy, which
				// returns no rows to anyone but its owner, and leaving the real
				// table with a policy that was never enforced
				// (stokaro/ptah#2201).
				Table:   dbTable.QualifiedName(),
				Comment: "", // Comment not available in DBTable for RLS enablement
			}
			database.RLSEnabledTables = append(database.RLSEnabledTables, rlsEnabledTable)
		}
	}
}

// tablePrimaryKey is a primary key the description writes as a declaration of
// its own rather than as a flag on one column.
type tablePrimaryKey struct {
	columns []string
	include []string
}

func primaryKeyColumnSets(primaryKeysByTable map[string]tablePrimaryKey) map[string]map[string]bool {
	result := make(map[string]map[string]bool, len(primaryKeysByTable))
	for tableName, key := range primaryKeysByTable {
		columnSet := make(map[string]bool, len(key.columns))
		for _, column := range key.columns {
			columnSet[column] = true
		}
		result[tableName] = columnSet
	}
	return result
}

// primaryKeysByTable selects the primary keys that need a declaration of their
// own, keyed by qualified table name.
//
// A single-column key with nothing else to say is left to the column's own
// `Primary` flag, which reproduces it exactly; writing a table-level key for it
// would put a redundant second spelling into every description.
//
// An INCLUDE payload is the "something else to say". The column flag has
// nowhere to hang it, so a covering key needs the table-level form however few
// columns it has -- `PRIMARY KEY (a) INCLUDE (payload)` is as covering as
// `PRIMARY KEY (a, b) INCLUDE (payload)`, and the column-count test alone
// dropped the first of them before it reached this map at all
// (stokaro/ptah#2199).
func primaryKeysByTable(dbSchema *dbschematypes.DBSchema) map[string]tablePrimaryKey {
	result := make(map[string]tablePrimaryKey)
	for _, constraint := range dbSchema.Constraints {
		if !strings.EqualFold(constraint.Type, "PRIMARY KEY") {
			continue
		}
		columns := constraint.ColumnNamesOrDefault()
		if len(columns) == 0 {
			continue
		}
		if len(columns) == 1 && len(constraint.IncludeColumns) == 0 {
			continue
		}
		result[constraint.QualifiedTableName()] = tablePrimaryKey{
			columns: columns,
			include: slices.Clone(constraint.IncludeColumns),
		}
	}
	return result
}

// generatedUniqueConstraintName reports whether a single-column UNIQUE carries
// the name its server would have made up, in which case the compact column
// spelling reproduces it exactly and nothing is lost by using it.
//
// Two forms, and neither needs the dialect to recognize: PostgreSQL names such
// a constraint `<table>_<column>_key`, and MySQL and MariaDB name it after the
// column. Anything else is a name somebody chose, and choosing one is the only
// reason to write a table-level constraint for a single column.
func generatedUniqueConstraintName(constraint dbschematypes.DBConstraint, columns []string) bool {
	if len(columns) != 1 {
		return false
	}
	name := strings.TrimSpace(constraint.Name)
	if name == "" {
		return true
	}
	return name == columns[0] || name == constraint.TableName+"_"+columns[0]+"_key"
}

// clearColumnUniqueForNamedConstraints stops a column carrying `unique = true`
// for a constraint the description now names on its own.
//
// Both spellings mean one constraint, so writing both would put two of them in
// the document and plan a duplicate on apply.
func clearColumnUniqueForNamedConstraints(database *goschema.Database) {
	named := make(map[tableMemberKey]struct{}, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(constraint.Columns) != 1 {
			continue
		}
		named[tableMemberKey{table: constraint.StructName, member: constraint.Columns[0]}] = struct{}{}
	}
	for i := range database.Fields {
		field := &database.Fields[i]
		if _, isNamed := named[tableMemberKey{table: field.StructName, member: field.Name}]; isNamed {
			field.Unique = false
		}
	}
}

func convertConstraints(dbSchema *dbschematypes.DBSchema, tableStructNames map[string]string) []goschema.Constraint {
	constraints := make([]goschema.Constraint, 0, len(dbSchema.Constraints))
	for _, dbConstraint := range dbSchema.Constraints {
		constraint, ok := convertConstraint(dbConstraint, tableStructNames)
		if ok {
			constraints = append(constraints, constraint)
		}
	}
	return constraints
}

func convertConstraint(dbConstraint dbschematypes.DBConstraint, tableStructNames map[string]string) (goschema.Constraint, bool) {
	constraintType := strings.ToUpper(dbConstraint.Type)
	columns := dbConstraint.ColumnNamesOrDefault()
	switch constraintType {
	case "PRIMARY KEY":
		return goschema.Constraint{}, false
	case "FOREIGN KEY":
		if len(columns) <= 1 {
			return goschema.Constraint{}, false
		}
	case "UNIQUE":
		// A single-column UNIQUE is normally carried by the column's own
		// `unique = true`, which is the compact spelling and the one a person
		// writing a schema uses. That spelling has no room for a NAME, so a
		// constraint somebody named was described without it and came back
		// under the server's generated one -- `customers_email_key` where the
		// author had written `customers_email_uq`. A constraint name is an
		// interface: it appears in every violation error the application sees
		// (stokaro/ptah#2102).
		if len(columns) <= 1 && generatedUniqueConstraintName(dbConstraint, columns) {
			return goschema.Constraint{}, false
		}
	case "CHECK":
		if dbConstraint.CheckClause == nil || strings.TrimSpace(*dbConstraint.CheckClause) == "" {
			return goschema.Constraint{}, false
		}
		if isPostgresSyntheticNotNullCheck(dbConstraint) {
			return goschema.Constraint{}, false
		}
	case "EXCLUDE":
		if dbConstraint.UsingMethod == nil || dbConstraint.ExcludeElements == nil {
			return goschema.Constraint{}, false
		}
	default:
		return goschema.Constraint{}, false
	}

	return goschema.Constraint{
		StructName:      structNameForTable(tableStructNames, dbConstraint.QualifiedTableName(), dbConstraint.TableName),
		Name:            dbConstraint.Name,
		Type:            constraintType,
		Table:           dbConstraint.QualifiedTableName(),
		UsingMethod:     derefString(dbConstraint.UsingMethod),
		ExcludeElements: derefString(dbConstraint.ExcludeElements),
		WhereCondition:  derefString(dbConstraint.WhereCondition),
		CheckExpression: derefString(dbConstraint.CheckClause),
		Columns:         columns,
		IncludeColumns:  append([]string(nil), dbConstraint.IncludeColumns...),
		NullsDistinct:   cloneBoolPtr(dbConstraint.NullsDistinct),
		ForeignTable:    dbConstraint.QualifiedForeignTableName(),
		ForeignColumn:   firstString(dbConstraint.ForeignColumnsOrDefault()),
		ForeignColumns:  dbConstraint.ForeignColumnsOrDefault(),
		OnDelete:        derefString(dbConstraint.DeleteRule),
		OnUpdate:        derefString(dbConstraint.UpdateRule),
		Deferrable:      dbConstraint.Deferrable,
		Initially:       dbConstraint.Initially,
		// The index backing this constraint is dropped above so the constraint
		// renders once; what that index needed does not go with it.
		RequiresExtensions: slices.Clone(dbConstraint.RequiresExtensions),
	}, true
}

func constraintBackedIndexesByTable(dbSchema *dbschematypes.DBSchema) map[tableMemberKey]struct{} {
	result := make(map[tableMemberKey]struct{}, len(dbSchema.Constraints))
	for _, constraint := range dbSchema.Constraints {
		switch strings.ToUpper(constraint.Type) {
		case "PRIMARY KEY", "UNIQUE", "EXCLUDE":
			result[tableMemberKey{table: constraint.QualifiedTableName(), member: constraint.Name}] = struct{}{}
		}
	}
	return result
}

func isPostgresSyntheticNotNullCheck(constraint dbschematypes.DBConstraint) bool {
	if constraint.CheckClause == nil || !strings.HasSuffix(constraint.Name, "_not_null") {
		return false
	}
	checkClause := strings.TrimSpace(strings.ToUpper(*constraint.CheckClause))
	if !strings.HasSuffix(checkClause, " IS NOT NULL") {
		return false
	}
	return strings.Count(checkClause, " IS NOT NULL") == 1
}

func structNameForTable(tableStructNames map[string]string, qualifiedTableName, fallbackTableName string) string {
	if structName, ok := tableStructNames[qualifiedTableName]; ok {
		return structName
	}
	return generateStructName(fallbackTableName)
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func goSchemaFieldType(dbColumn dbschematypes.DBColumn) string {
	if serialType := postgresSerialType(dbColumn); serialType != "" {
		return serialType
	}
	// The server's own spelling wins wherever the reader had to ask for it,
	// which today means PostgreSQL array and domain columns. DataType for an
	// array is the bare category "ARRAY" -- a word no engine accepts as a type,
	// so a schema read back out of a database rendered DDL that could not be
	// executed (stokaro/ptah#1138).
	//
	// It is read from FormattedType rather than from ColumnType deliberately.
	// ColumnType is also what the Atlas-compatible JSON inspect output prints,
	// and measured on the pinned community binary v1.3.0 that output is
	// `"type": "ARRAY"` for an array column -- the same value Ptah prints there
	// today. Routing the fix through ColumnType would have made that surface
	// disagree with the binary in order to fix a surface the binary does not
	// have.
	//
	// It stays AHEAD of the USER-DEFINED branch below, and that order is the
	// whole content of one half of #1138. A domain whose base type is itself
	// user-defined is reported by information_schema with data_type
	// "USER-DEFINED" and udt_name naming the BASE, while domain_name names the
	// domain -- so with the branches the other way round the domain was
	// flattened to its base and the CHECK it carries was silently dropped.
	// Measured on PostgreSQL 17, one cluster, two domains that differ only in
	// what they are built on:
	//
	//	CREATE DOMAIN point3d AS cube CHECK (cube_dim(VALUE) = 3);
	//	CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
	//
	//	column      data_type      udt_name   domain_name   format_type
	//	c_point3d   USER-DEFINED   cube       point3d       point3d
	//	c_domain    integer        int4       positive_int  positive_int
	//
	// Before this, c_point3d inspected as `cube` and c_domain as
	// `positive_int`: applying that document back built the column as a bare
	// cube, so the domain and its constraint were gone from the database with
	// nothing reported. The pinned community binary v1.3.0 renders
	// `sql("point3d")` for the same column, so this is also the compatible
	// answer. The same split is visible on stock extension domains -- `lo`
	// (over oid) survived while `earth` (over cube) did not.
	if dbColumn.FormattedType != "" {
		return dbColumn.FormattedType
	}
	if strings.EqualFold(dbColumn.DataType, "USER-DEFINED") && dbColumn.UDTName != "" {
		return dbColumn.UDTName
	}
	if dbColumn.ColumnType != "" {
		return dbColumn.ColumnType
	}
	if sizedType := sizedColumnType(dbColumn); sizedType != "" {
		return sizedType
	}
	return dbColumn.DataType
}

// postgresSerialType reports the SERIAL shorthand a column can be written back
// as, or "" when it cannot.
//
// A domain column can never be written back as SERIAL. PostgreSQL's SERIAL
// shorthand only ever builds a column of an integer type, so spelling a column
// of domain `positive` as SERIAL rebuilds it as a plain integer and drops the
// domain's CHECK with it. The domain wins, and the sequence default it was
// drawing from is then carried as an ordinary default rather than folded into
// the shorthand. Measured on PostgreSQL 17.10 against `id positive DEFAULT
// nextval('s')` with the sequence OWNED BY that column: the pinned binary
// v1.3.0 reports `type = sql("positive")` with the nextval default beside it,
// and Ptah reported `type = serial` with no default at all. See
// stokaro/ptah#1242.
func postgresSerialType(dbColumn dbschematypes.DBColumn) string {
	if dbColumn.DomainName != "" {
		return ""
	}
	if !dbColumn.IsAutoIncrement || dbColumn.ColumnDefault == nil ||
		!strings.Contains(strings.ToLower(*dbColumn.ColumnDefault), "nextval(") {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(dbColumn.DataType)) {
	case "smallint":
		return "SMALLSERIAL"
	case "integer":
		return "SERIAL"
	case "bigint":
		return "BIGSERIAL"
	default:
		return ""
	}
}

// sizedColumnType renders the width a read carried in a field of its own.
//
// Every family PostgreSQL keeps a width for belongs here, and the two bit ones
// were missing. `ptah schema inspect` wrote a `bit(4)` column as `bit`, and
// replaying that document into a fresh database produced `bit(1)` -- measured
// on PostgreSQL 17.11, three bits of every value gone. A `bit varying(8)` came
// back unlimited, and applying the document to the SOURCE database removed the
// declared width from the live column (stokaro/ptah#2034).
func sizedColumnType(dbColumn dbschematypes.DBColumn) string {
	dataType := strings.ToLower(strings.TrimSpace(dbColumn.DataType))
	switch dataType {
	case "character varying", "varchar":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("VARCHAR(%d)", *dbColumn.CharacterMaxLength)
		}
	case "character", "char":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("CHAR(%d)", *dbColumn.CharacterMaxLength)
		}
	case "bit":
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("BIT(%d)", *dbColumn.CharacterMaxLength)
		}
	case "bit varying", "varbit":
		// Lower case, unlike the arms around it. Those are modeled HCL type
		// names that the renderer lower-cases on the way out; this one is not
		// writable bare -- two identifiers separated by a space is not one HCL
		// expression -- so it reaches the document through sql() carrying
		// whatever case it has here, and that binary's type names are case
		// sensitive.
		if dbColumn.CharacterMaxLength != nil {
			return fmt.Sprintf("bit varying(%d)", *dbColumn.CharacterMaxLength)
		}
	case "numeric", "decimal":
		if dbColumn.NumericPrecision != nil && dbColumn.NumericScale != nil {
			return fmt.Sprintf("NUMERIC(%d,%d)", *dbColumn.NumericPrecision, *dbColumn.NumericScale)
		}
		if dbColumn.NumericPrecision != nil {
			return fmt.Sprintf("NUMERIC(%d)", *dbColumn.NumericPrecision)
		}
	}
	return ""
}

func setFieldDefaultFromDB(field *goschema.Field, defaultSQL string) {
	if dbDefaultLooksLikeExpression(defaultSQL) {
		field.DefaultExpr = defaultSQL
		return
	}
	field.Default = defaultSQL
}

// setDomainDefaultFromDB routes a domain's catalog default the way a column's
// is routed, which is the whole of the fix for stokaro/ptah#2037.
//
// [goschema.Domain] keeps a literal and an expression apart because the
// renderer does: an expression becomes `sql(...)` and a literal becomes a
// quoted string. Assigning the catalog's answer to the literal field wrote a
// quoted default, which reads back as a 26-character string, so `apply` planned
// a SET DEFAULT of that quoted text and the domain's default became the TEXT of
// the old expression. Measured on PostgreSQL 17.11, a column of that type then
// defaulted to that text, and each further inspect-and-apply cycle wrapped it
// again: 26, 49, 76, 111 characters.
//
// PostgreSQL reports every domain default as an expression -- a declared
// DEFAULT 'x' comes back with a cast -- so in practice this routes all of them
// to the expression side. The literal branch is kept because the field exists
// and a caller building a description by hand may use it.
func setDomainDefaultFromDB(domain *goschema.Domain, defaultSQL string) {
	if strings.TrimSpace(defaultSQL) == "" {
		return
	}
	if dbDefaultLooksLikeExpression(defaultSQL) {
		domain.DefaultExpr = defaultSQL
		return
	}
	domain.Default = defaultSQL
}
func dbDefaultLooksLikeExpression(defaultSQL string) bool {
	value := strings.TrimSpace(defaultSQL)
	if value == "" {
		return false
	}
	if strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		return false
	}
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return false
	}
	return true
}

// convertGrants describes live grant rows as declarations.
//
// A row marked [dbschematypes.DBGrant.IsPartialRevoke] is skipped, because it
// is not a grant: it SUBTRACTS a privilege from a broader grant, and only
// ClickHouse produces one. Describing it as a [goschema.Grant] would state the
// exact opposite of what the row says — a document telling an operator the role
// HOLDS a privilege the server records it as having lost — and applying that
// document would grant it for real.
//
// Skipping still leaves the broader grant the exception applies to, so the
// description over-states the role's privileges rather than inverting them.
// That is the safer of the two errors available: over-stating makes a
// comparison find the grant present and plan nothing, so the exception on the
// server survives, while dropping the broader grant as well would make the
// comparison plan a GRANT that wipes the exception out. Ptah's grant model has
// no shape for "this privilege except there", which is why
// [go.5x5.cz/ptah/internal/clickhouserbac.ValidateLive] refuses to compare a
// managed role carrying one at all rather than leaving this function to
// approximate it.
func convertGrants(dbGrants []dbschematypes.DBGrant) []goschema.Grant {
	grants := make([]goschema.Grant, 0, len(dbGrants))
	for _, dbGrant := range dbGrants {
		if dbGrant.IsPartialRevoke {
			continue
		}
		grant := goschema.Grant{
			Role:       dbGrant.Role,
			Privileges: []string{dbGrant.Privilege},
			WithOption: dbGrant.WithOption,
			GrantedBy:  dbGrant.GrantedBy,
		}
		if strings.EqualFold(dbGrant.ObjectType, "SCHEMA") {
			grant.OnSchema = dbGrant.ObjectName
		} else {
			grant.OnTable = dbGrant.QualifiedTarget()
		}
		grant.Canonicalize()
		grants = append(grants, grant)
	}
	return grants
}

// foreignKeyInfo holds the field-level pieces reconstructed from a database
// FOREIGN KEY constraint.
type foreignKeyInfo struct {
	name     string // constraint name
	foreign  string // "table(column)" reference
	onDelete string // ON DELETE action (NO ACTION normalized away later)
	onUpdate string // ON UPDATE action
	// deferrable and initially carry the deferral the catalog reported, so a
	// single-column foreign key read back off a live server keeps the property
	// the schema declared (stokaro/ptah#1624).
	deferrable bool
	initially  string
}

type tableMemberKey struct {
	table  string
	member string
}

// indexForeignKeysByColumn maps table.column -> reconstructed FK info for every
// single-column FOREIGN KEY constraint in the database schema. Multi-column FKs
// are not field-level and are skipped (they are represented as table-level
// constraints, which this converter does not yet round-trip).
func indexForeignKeysByColumn(dbSchema *dbschematypes.DBSchema) map[tableMemberKey]foreignKeyInfo {
	result := make(map[tableMemberKey]foreignKeyInfo)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" || c.ColumnName == "" || c.ForeignTable == nil || len(c.ColumnNamesOrDefault()) != 1 {
			continue
		}
		foreignTable := c.QualifiedForeignTableName()
		foreignColumn := ""
		if foreignColumns := c.ForeignColumnsOrDefault(); len(foreignColumns) == 1 {
			foreignColumn = foreignColumns[0]
		}
		foreign := foreignTable
		if foreignColumn != "" {
			foreign = foreignTable + "(" + foreignColumn + ")"
		}
		result[tableMemberKey{table: c.QualifiedTableName(), member: c.ColumnName}] = foreignKeyInfo{
			name:       c.Name,
			foreign:    foreign,
			onDelete:   derefString(c.DeleteRule),
			onUpdate:   derefString(c.UpdateRule),
			deferrable: c.Deferrable,
			initially:  c.Initially,
		}
	}
	return result
}

// derefString returns the pointed-to string or "" when nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// generateStructName converts a table name to a Go struct name
func generateStructName(tableName string) string {
	// Simple conversion: remove underscores and capitalize
	parts := strings.Split(tableName, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}

// generateFieldName converts a column name to a Go field name
func generateFieldName(columnName string) string {
	// Simple conversion: remove underscores and capitalize
	parts := strings.Split(columnName, "_")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + part[1:]
		}
	}
	return strings.Join(parts, "")
}

// clickHouseTableOverrides carries the ClickHouse engine facts a description
// needs that have no field of their own on goschema.Table, and returns nil when
// there are none.
//
// Only the sorting key is here so far. It reaches the renderer as the
// `order_by` platform override, which is the same key a declaration writes, so
// a read description and a hand-written one produce the same statement
// (stokaro/ptah#1603).
func clickHouseTableOverrides(dbTable dbschematypes.DBTable) map[string]map[string]string {
	if dbTable.ClickHouseSortingKey == "" {
		return nil
	}
	return map[string]map[string]string{
		"clickhouse": {"order_by": dbTable.ClickHouseSortingKey},
	}
}
