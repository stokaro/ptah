// Package dbschematogo converts an introspected database schema
// (catalog.Database) into the goschema entity model, so live databases
// can flow through the same diff and planning pipeline as annotated Go
// sources.
package dbschematogo

import (
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/catalogfield"
	"go.5x5.cz/ptah/internal/indexbacking"
)

// ConvertDBSchemaToGoSchema converts a database schema to goschema format
// This is needed for down migrations where we use the current DB state as the target
//
// dialect names the server the catalog was read from, and decides which
// constraint kinds that server enforces with an index the reader also reports;
// [go.5x5.cz/ptah/internal/indexbacking] holds that answer for this converter
// and for migration/schemadiff alike. It may be empty, because
// [catalog.Database] carries no dialect and the stable
// atlascompat.DBSchemaToGoSchema takes none. An empty dialect answers only what
// every server does, which is what this path has always produced -- stated at
// the shared declaration rather than implied by an arm nobody wrote.
func ConvertDBSchemaToGoSchema(dbSchema *catalog.Database, dialect string) *schemamodel.Database {
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

	// One decision, consulted by both pools below. A unique constraint and its
	// backing index describe one object, so exactly one of them may be emitted.
	indexDescribed := indexDescribedUniques(dbSchema)
	database.Indexes = convertIndexes(dbSchema, tableStructNames, indexDescribed, dialect)
	database.Constraints = convertConstraints(dbSchema, tableStructNames, indexDescribed)
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

func convertSchemas(database *schemamodel.Database, schemas []catalog.Schema) {
	for _, schema := range schemas {
		database.Schemas = append(database.Schemas, schemamodel.Schema{
			Name:    schema.Name,
			Comment: schema.Comment,
			Charset: schema.Charset,
			Collate: schema.Collate,
		})
	}
}

func newDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas:           make([]schemamodel.Schema, 0),
		Tables:            make([]schemamodel.Table, 0),
		Fields:            make([]schemamodel.Field, 0),
		Indexes:           make([]schemamodel.Index, 0),
		Constraints:       make([]schemamodel.Constraint, 0),
		Enums:             make([]schemamodel.Enum, 0),
		Extensions:        make([]schemamodel.Extension, 0),
		Functions:         make([]schemamodel.Function, 0),
		Sequences:         make([]schemamodel.Sequence, 0),
		Domains:           make([]schemamodel.Domain, 0),
		CompositeTypes:    make([]schemamodel.CompositeType, 0),
		Ranges:            make([]schemamodel.Range, 0),
		Views:             make([]schemamodel.View, 0),
		MaterializedViews: make([]schemamodel.MaterializedView, 0),
		Triggers:          make([]schemamodel.Trigger, 0),
		RLSPolicies:       make([]schemamodel.RLSPolicy, 0),
		RLSEnabledTables:  make([]schemamodel.RLSEnabledTable, 0),
		Roles:             make([]schemamodel.Role, 0),
		Grants:            make([]schemamodel.Grant, 0),
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
func convertEnums(database *schemamodel.Database, dbEnums []catalog.Enum) {
	for _, dbEnum := range dbEnums {
		database.Enums = append(database.Enums, schemamodel.Enum{
			Name:   dbEnum.Name,
			Schema: dbEnum.Schema,
			Values: dbEnum.Values,
		})
	}
}

func convertTablesAndFields(
	database *schemamodel.Database,
	dbSchema *catalog.Database,
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

		table := schemamodel.Table{
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
			RowTTL:            dbTable.RowTTL.Clone(),
			RowDeletionPolicy: dbTable.RowDeletionPolicy.Clone(),
			Overrides:         clickHouseTableOverrides(dbTable),
		}
		database.Tables = append(database.Tables, table)

		// Convert columns to fields
		for _, dbColumn := range dbTable.Columns {
			opts := catalogfield.Options{
				CoveredByTablePrimaryKey: tablePKColumns[dbTable.QualifiedName()][dbColumn.Name],
			}
			// Carry the field-level foreign key (reference + referential actions)
			// so down migrations can reconstruct it with the prior action.
			if fk, ok := fkByColumn[tableMemberKey{table: dbTable.QualifiedName(), member: dbColumn.Name}]; ok {
				opts.ForeignKey = &catalogfield.ForeignKey{
					Name:       fk.name,
					Reference:  fk.foreign,
					OnDelete:   fk.onDelete,
					OnUpdate:   fk.onUpdate,
					Deferrable: fk.deferrable,
					Initially:  fk.initially,
				}
			}

			// The column itself is described by catalogfield, which the schema
			// comparison reaches too. What is added here is what that package
			// deliberately does not know: the Go source a field was parsed
			// from (stokaro/ptah#2315).
			field := catalogfield.Field(dbColumn, opts)
			field.StructName = structName
			field.FieldName = generateFieldName(dbColumn.Name)

			database.Fields = append(database.Fields, field)
		}
	}
	return tableStructNames
}

func tableNameCounts(tables []catalog.Table) map[string]int {
	counts := make(map[string]int, len(tables))
	for _, table := range tables {
		counts[table.Name]++
	}
	return counts
}

func dbTableStructName(table catalog.Table, tableNameCounts map[string]int) string {
	if tableNameCounts[table.Name] > 1 && strings.TrimSpace(table.Schema) != "" {
		return generateStructName(table.Schema + "_" + table.Name)
	}
	return generateStructName(table.Name)
}

func convertIndexes(
	dbSchema *catalog.Database,
	tableStructNames map[string]string,
	indexDescribed map[tableMemberKey]struct{},
	dialect string,
) []schemamodel.Index {
	constraintBackedIndexes := constraintBackedIndexesByTable(dbSchema, indexDescribed, dialect)
	indexes := make([]schemamodel.Index, 0, len(dbSchema.Indexes))
	for _, dbIndex := range dbSchema.Indexes {
		// An index nothing can declare is not described as one. A primary
		// key's index belongs to the key, and SQLite's sqlite_autoindex_* rows
		// name an internal structure whose name the server refuses in a
		// CREATE INDEX -- so describing one produced a schema that could not be
		// replayed into the database it came from (stokaro/ptah#2894).
		//
		// The predicate is shared with migration/schemadiff rather than
		// restated: the comparator recognized this shape and this converter did
		// not, which is the duplication ADR 0015 D2 removes.
		if indexbacking.Unaddressable(dbIndex, dialect) {
			continue
		}
		if _, ok := constraintBackedIndexes[tableMemberKey{table: dbIndex.QualifiedTableName(), member: dbIndex.Name}]; ok {
			continue
		}

		index := schemamodel.Index{
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

// indexType picks the value schemamodel.Index.Type carries for an introspected
// index. goschema keeps one field for two concepts the database layer keeps
// apart: the PostgreSQL access method (btree/gin/gist/brin/hash) and the
// ClickHouse data-skipping-index type (minmax/bloom_filter/...). No reader
// sets both, so the choice is unambiguous.
func indexType(index catalog.Index) string {
	if index.Method != "" {
		return index.Method
	}
	return index.Type
}

func convertIndexParts(parts []catalog.IndexPart) []schemamodel.IndexPart {
	if len(parts) == 0 {
		return nil
	}
	converted := make([]schemamodel.IndexPart, len(parts))
	for position, part := range parts {
		converted[position] = schemamodel.IndexPart{
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

func convertExtensions(database *schemamodel.Database, dbExtensions []catalog.Extension) {
	for _, dbExtension := range dbExtensions {
		extension := schemamodel.Extension{
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
	database *schemamodel.Database,
	dbPolicies []catalog.RLSPolicy,
	tableStructNames map[string]string,
) {
	for _, dbPolicy := range dbPolicies {
		policy := schemamodel.RLSPolicy{
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
// where schemamodel.Function keeps it -- the same place views and materialized
// views keep theirs, and the same place the HCL parser already writes it from a
// `function` block's `schema` attribute.
//
// Dropping it left the name unqualified, so the Atlas-compatible render wrote a
// `function` block with no schema attribute at all and an apply recreated the
// function in whatever schema the connection defaulted to. On a read covering
// more than one schema, `extra.f_extra` came back as `public.f_extra`
// (stokaro/ptah#1276).
func convertFunctions(database *schemamodel.Database, dbFunctions []catalog.Function) {
	for _, dbFunction := range dbFunctions {
		function := schemamodel.Function{
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
			Settings:   dbFunction.Settings,
			Body:       dbFunction.Body,
			Comment:    dbFunction.Comment,
		}
		database.Functions = append(database.Functions, function)
	}
}

func convertUserTypes(database *schemamodel.Database, dbSchema *catalog.Database) {
	for _, domain := range dbSchema.Domains {
		converted := schemamodel.Domain{
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
		fields := make([]schemamodel.CompositeField, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			fields = append(fields, schemamodel.CompositeField{Name: field.Name, Type: field.Type})
		}
		database.CompositeTypes = append(database.CompositeTypes, schemamodel.CompositeType{
			Name:   composite.Name,
			Schema: composite.Schema,
			Fields: fields,
		})
	}
	for _, rangeType := range dbSchema.Ranges {
		database.Ranges = append(database.Ranges, schemamodel.Range{
			Name:    rangeType.Name,
			Schema:  rangeType.Schema,
			Subtype: rangeType.Subtype,
			// The four attributes beside the subtype are what make one range
			// type a different type from another. The reader asks pg_range for
			// all of them and the renderer emits an option for each; listing
			// only the subtype here described `CREATE TYPE r AS RANGE (SUBTYPE
			// = timestamptz, SUBTYPE_DIFF = f)` as a range with no diff
			// function, and replaying that built a type whose GiST indexes lose
			// their penalty function and whose discrete values stop
			// canonicalizing (stokaro/ptah#2200).
			SubtypeOpClass: rangeType.SubtypeOpClass,
			Collation:      rangeType.Collation,
			Canonical:      rangeType.Canonical,
			SubtypeDiff:    rangeType.SubtypeDiff,
		})
	}
}

func convertSequences(database *schemamodel.Database, dbSequences []catalog.Sequence) {
	for _, dbSequence := range dbSequences {
		database.Sequences = append(database.Sequences, schemamodel.Sequence{
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
func convertHypertables(database *schemamodel.Database, hypertables []catalog.Hypertable) {
	for _, hypertable := range hypertables {
		database.Hypertables = append(database.Hypertables, schemamodel.Hypertable{
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
	database *schemamodel.Database,
	aggregates []catalog.ContinuousAggregate,
) {
	for _, aggregate := range aggregates {
		// The catalog always reports a value, so the converted declaration
		// carries a definite one: this description is what a DOWN migration
		// recreates the aggregate from, and leaving the option unset there
		// would take the server default rather than the value that was there.
		materializedOnly := aggregate.MaterializedOnly
		database.ContinuousAggregates = append(database.ContinuousAggregates, schemamodel.ContinuousAggregate{
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
// [go.5x5.cz/ptah/core/schemamodel.Synonym.Target] is the spelling that will be
// emitted: one to four dot-separated parts, unquoted. Copying the catalog's
// form would put `[other].[dbo].[gauge]` in a document and render it again as
// a name with brackets inside it.
func convertSynonyms(database *schemamodel.Database, synonyms []catalog.Synonym) {
	for _, synonym := range synonyms {
		database.Synonyms = append(database.Synonyms, schemamodel.Synonym{
			Name:    synonym.Name,
			Schema:  synonym.Schema,
			Target:  synonym.DeclaredTarget(),
			Comment: synonym.Comment,
		})
	}
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
// The read still reports it, so it is not invisible: [catalog.ExtendedProperty]
// carries the row and the flag, and nothing is planned to remove it.
func convertExtendedProperties(
	database *schemamodel.Database,
	properties []catalog.ExtendedProperty,
) {
	for _, property := range properties {
		if property.ValueNotRepresentable {
			continue
		}
		database.ExtendedProperties = append(database.ExtendedProperties, schemamodel.ExtendedProperty{
			Name:   property.Name,
			Schema: property.Schema,
			Table:  property.Table,
			Column: property.Column,
			Value:  property.Value,
		})
	}
}

func convertViews(database *schemamodel.Database, dbViews []catalog.View) {
	for _, dbView := range dbViews {
		database.Views = append(database.Views, schemamodel.View{
			Name:       dbView.QualifiedName(),
			Body:       dbView.Body,
			WithCheck:  sqlutil.CheckOptionRequestsCheck(dbView.CheckOption),
			Comment:    dbView.Comment,
			Attributes: dbView.Attributes,
		})
	}
}

func convertMaterializedViews(database *schemamodel.Database, dbViews []catalog.MaterializedView) {
	for _, dbView := range dbViews {
		materializedView := schemamodel.MaterializedView{
			Name:    dbView.QualifiedName(),
			Body:    dbView.Body,
			Comment: dbView.Comment,
			Refresh: dbView.Refresh.Clone(),
		}
		database.MaterializedViews = append(database.MaterializedViews, materializedView)
	}
}

func convertTriggers(database *schemamodel.Database, dbTriggers []catalog.Trigger) {
	for _, dbTrigger := range dbTriggers {
		trigger := schemamodel.Trigger{
			Name:    dbTrigger.Name,
			Table:   dbTrigger.QualifiedTable(),
			Timing:  dbTrigger.Timing,
			Event:   dbTrigger.Event,
			ForEach: dbTrigger.ForEach,
			Body:    dbTrigger.Body,
			Comment: dbTrigger.Comment,
		}
		trigger.Canonicalize()
		// A trigger running a function Ptah did NOT generate for it keeps that
		// function by name rather than by a copy of its source. Describing the
		// source instead made one audit function shared by ten tables into ten
		// functions under ptah_trigger_* names, leaving the original defined and
		// called by nothing -- so changing the audit logic stopped being one
		// edit (stokaro/ptah#2210).
		//
		// The generated name is the discriminator, and it is the same one the
		// reverse conversion uses to fold a body back in.
		//
		// The body is KEPT beside the reference. The Atlas HCL surface has no
		// way to name a function a trigger runs and refuses a trigger without a
		// body -- measured, `schema inspect` answers
		// `trigger requires table and body for HCL schema export` and omits it,
		// after which applying the document plans a DROP of the trigger it just
		// described. The native SQL description uses the reference and the HCL
		// one keeps falling back to the body, which is the surface's existing
		// limit rather than a new one.
		if dbTrigger.ExecuteFunction != "" && dbTrigger.ExecuteFunction != trigger.FunctionName() {
			trigger.ExecuteFunction = dbTrigger.ExecuteFunction
		}
		database.Triggers = append(database.Triggers, trigger)
	}
}

func convertRoles(database *schemamodel.Database, dbRoles []catalog.Role) {
	for _, dbRole := range dbRoles {
		role := schemamodel.Role{
			StructName:  "", // Roles are not associated with specific structs in DB schema
			Name:        dbRole.Name,
			Login:       dbRole.Login,
			Password:    "", // Not available in current Role for security
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
	database *schemamodel.Database,
	dbTables []catalog.Table,
	tableStructNames map[string]string,
) {
	for _, dbTable := range dbTables {
		if dbTable.RLSEnabled {
			rlsEnabledTable := schemamodel.RLSEnabledTable{
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
				Comment: "", // Comment not available in Table for RLS enablement
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
func primaryKeysByTable(dbSchema *catalog.Database) map[string]tablePrimaryKey {
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
func generatedUniqueConstraintName(constraint catalog.Constraint, columns []string) bool {
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
func clearColumnUniqueForNamedConstraints(database *schemamodel.Database) {
	named := make(map[tableMemberKey]struct{}, len(database.Constraints))
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(constraint.Columns) != 1 {
			continue
		}
		named[tableMemberKey{table: constraint.StructName, member: constraint.Columns[0]}] = struct{}{}
	}
	// A named unique INDEX covering the column describes the same object, and
	// the column's inline UNIQUE would be a second one. The two pools have to
	// be read together here for the same reason ownership is decided in one
	// place: an object moving between them must not change what the column
	// says. Measured on CockroachDB -- with the covering index owning the
	// object, clearing on constraints alone emitted `email text UNIQUE` beside
	// it and the replay grew an `a_email_key` the source never had
	// (stokaro/ptah#2589).
	for _, index := range database.Indexes {
		if !index.Unique || len(index.Fields) != 1 {
			continue
		}
		named[tableMemberKey{table: index.StructName, member: index.Fields[0]}] = struct{}{}
	}
	for i := range database.Fields {
		field := &database.Fields[i]
		if _, isNamed := named[tableMemberKey{table: field.StructName, member: field.Name}]; isNamed {
			field.Unique = false
		}
	}
}

func convertConstraints(
	dbSchema *catalog.Database,
	tableStructNames map[string]string,
	indexDescribed map[tableMemberKey]struct{},
) []schemamodel.Constraint {
	constraints := make([]schemamodel.Constraint, 0, len(dbSchema.Constraints))
	for _, dbConstraint := range dbSchema.Constraints {
		key := tableMemberKey{table: dbConstraint.QualifiedTableName(), member: dbConstraint.Name}
		if _, ok := indexDescribed[key]; ok {
			continue
		}
		constraint, ok := convertConstraint(dbConstraint, tableStructNames)
		if ok {
			constraints = append(constraints, constraint)
		}
	}
	return constraints
}

func convertConstraint(dbConstraint catalog.Constraint, tableStructNames map[string]string) (schemamodel.Constraint, bool) {
	constraintType := strings.ToUpper(dbConstraint.Type)
	columns := dbConstraint.ColumnNamesOrDefault()
	switch constraintType {
	case "PRIMARY KEY":
		return schemamodel.Constraint{}, false
	case "FOREIGN KEY":
		if len(columns) <= 1 {
			return schemamodel.Constraint{}, false
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
			return schemamodel.Constraint{}, false
		}
	case "CHECK":
		if dbConstraint.CheckClause == nil || strings.TrimSpace(*dbConstraint.CheckClause) == "" {
			return schemamodel.Constraint{}, false
		}
		if isPostgresSyntheticNotNullCheck(dbConstraint) {
			return schemamodel.Constraint{}, false
		}
	case "EXCLUDE":
		if dbConstraint.UsingMethod == nil || dbConstraint.ExcludeElements == nil {
			return schemamodel.Constraint{}, false
		}
	default:
		return schemamodel.Constraint{}, false
	}

	return schemamodel.Constraint{
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

func constraintBackedIndexesByTable(
	dbSchema *catalog.Database,
	indexDescribed map[tableMemberKey]struct{},
	dialect string,
) map[tableMemberKey]struct{} {
	result := make(map[tableMemberKey]struct{}, len(dbSchema.Constraints))
	for _, constraint := range dbSchema.Constraints {
		key := tableMemberKey{table: constraint.QualifiedTableName(), member: constraint.Name}
		if _, ok := indexDescribed[key]; ok {
			// The index is the description this object keeps, so it is not
			// suppressed and the constraint is dropped instead.
			continue
		}
		// NamedAfterConstraint rather than ServerBacks: this pool matches on the
		// constraint's name, and the name is only evidence about the object for
		// the kinds named here. A foreign key's backing index shares the name on
		// MySQL, MariaDB and Spanner and still needs the column match the
		// comparator does, which this converter has no place to do.
		if indexbacking.NamedAfterConstraint(dialect, indexbacking.KindOf(constraint.Type)) {
			result[key] = struct{}{}
		}
	}
	return result
}

// indexDescribedUniques names the UNIQUE constraints whose same-named index is
// the fuller description of the same object.
//
// A unique constraint and its backing index are one object, and exactly one of
// them may be described: emitting both produces one name and two objects, which
// is worse than the loss it would fix. Which description is kept is decided
// here, once, and both pools consult the answer. Each side filtering the other
// independently is how the payload came to be dropped -- the same rule
// stokaro/ptah#1245 established for the comparator, applied to the description
// path (stokaro/ptah#2589).
//
// The discriminator is a payload the constraint view does not carry. CockroachDB
// reports a bare covering unique index in pg_constraint as well as in pg_index,
// and its pg_get_constraintdef prints no INCLUDE, so describing that object by
// its constraint loses the payload silently. PostgreSQL does not report such an
// index in pg_constraint at all, and MySQL and MariaDB have no payload to lose,
// so on those servers this set is empty and nothing moves -- which is what keeps
// the fix from reaching a server that never had the defect.
func indexDescribedUniques(dbSchema *catalog.Database) map[tableMemberKey]struct{} {
	covering := make(map[tableMemberKey]struct{})
	for _, index := range dbSchema.Indexes {
		if !index.IsUnique || index.IsPrimary || len(index.IncludeColumns) == 0 {
			continue
		}
		covering[tableMemberKey{table: index.QualifiedTableName(), member: index.Name}] = struct{}{}
	}
	if len(covering) == 0 {
		return nil
	}
	owned := make(map[tableMemberKey]struct{}, len(covering))
	for _, constraint := range dbSchema.Constraints {
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(constraint.IncludeColumns) != 0 {
			continue
		}
		key := tableMemberKey{table: constraint.QualifiedTableName(), member: constraint.Name}
		if _, ok := covering[key]; ok {
			owned[key] = struct{}{}
		}
	}
	return owned
}

func isPostgresSyntheticNotNullCheck(constraint catalog.Constraint) bool {
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

// setDomainDefaultFromDB routes a domain's catalog default the way a column's
// is routed, which is the whole of the fix for stokaro/ptah#2037.
//
// [schemamodel.Domain] keeps a literal and an expression apart because the
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
func setDomainDefaultFromDB(domain *schemamodel.Domain, defaultSQL string) {
	if strings.TrimSpace(defaultSQL) == "" {
		return
	}
	if sqlutil.DefaultLooksLikeExpression(defaultSQL) {
		domain.DefaultExpr = defaultSQL
		return
	}
	domain.Default = defaultSQL
}

// convertGrants describes live grant rows as declarations.
//
// A row marked [catalog.Grant.IsPartialRevoke] is skipped, because it
// is not a grant: it SUBTRACTS a privilege from a broader grant, and only
// ClickHouse produces one. Describing it as a [schemamodel.Grant] would state the
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
func convertGrants(dbGrants []catalog.Grant) []schemamodel.Grant {
	grants := make([]schemamodel.Grant, 0, len(dbGrants))
	for _, dbGrant := range dbGrants {
		if dbGrant.IsPartialRevoke {
			continue
		}
		grant := schemamodel.Grant{
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
func indexForeignKeysByColumn(dbSchema *catalog.Database) map[tableMemberKey]foreignKeyInfo {
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
// needs that have no field of their own on schemamodel.Table, and returns nil when
// there are none.
//
// Only the sorting key is here so far. It reaches the renderer as the
// `order_by` platform override, which is the same key a declaration writes, so
// a read description and a hand-written one produce the same statement
// (stokaro/ptah#1603).
func clickHouseTableOverrides(dbTable catalog.Table) map[string]map[string]string {
	// Every clause the engine spec resolves, under the key the renderer reads.
	// An unknown override key becomes a node option under its upper-cased name,
	// which is what resolveTableEngineSpec looks up.
	//
	// Carrying only the sorting key left every other clause to the renderer's
	// defaults: a ReplacingMergeTree came back a MergeTree, and the partition
	// key, the sampling key, the TTL and the settings came back absent
	// (stokaro/ptah#2198).
	overrides := make(map[string]string, 7)
	for key, value := range map[string]string{
		"engine":       dbTable.ClickHouseEngine,
		"order_by":     dbTable.ClickHouseOrderBy,
		"partition_by": dbTable.ClickHousePartitionKey,
		"primary_key":  dbTable.ClickHousePrimaryKey,
		"sample_by":    dbTable.ClickHouseSamplingKey,
		"ttl":          dbTable.ClickHouseTTL,
		"settings":     dbTable.ClickHouseSettings,
	} {
		if value != "" {
			overrides[key] = value
		}
	}
	if len(overrides) == 0 {
		return nil
	}
	return map[string]map[string]string{"clickhouse": overrides}
}
