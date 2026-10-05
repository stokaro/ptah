// Package dbschematogo converts an introspected database schema
// (catalog.Database) into the goschema entity model, so live databases
// can flow through the same diff and planning pipeline as annotated Go
// sources.
package dbschematogo

import (
	"maps"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/catalogfield"
	"ptah.run/internal/indexbacking"
	"ptah.run/internal/mysqlindex"
	"ptah.run/internal/pgname"
	"ptah.run/internal/uniquename"
	"ptah.run/internal/ydbpool"
	"ptah.run/internal/ydbsecret"
)

// ConvertDBSchemaToGoSchema converts a database schema to goschema format
// This is needed for down migrations where we use the current DB state as the target
//
// dialect names the server the catalog was read from, and decides which
// constraint kinds that server enforces with an index the reader also reports;
// [ptah.run/internal/indexbacking] holds that answer for this converter
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
	columnKeys := indexForeignKeysByColumn(dbSchema)
	tablePrimaryKeys := primaryKeysByTable(dbSchema, dialect)
	tablePKColumns := primaryKeyColumnSets(tablePrimaryKeys)
	tableStructNames := convertTablesAndFields(
		database, dbSchema, columnKeys.byColumn, tablePrimaryKeys, tablePKColumns, dialect,
	)

	// One decision, consulted by both pools below. A unique constraint and its
	// backing index describe one object, so exactly one of them may be emitted.
	indexDescribed := indexDescribedUniques(dbSchema, dialect)
	database.Indexes = convertIndexes(dbSchema, tableStructNames, indexDescribed, dialect)
	database.Constraints = convertConstraints(dbSchema, tableStructNames, indexDescribed, columnKeys.besideColumn)
	clearColumnUniqueForNamedConstraints(database, dbSchema, tableStructNames)
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
	convertTopics(database, dbSchema.Topics)
	convertResourcePools(database, dbSchema.ResourcePools, dbSchema.ResourcePoolClassifiers)
	convertReplications(database, dbSchema.AsyncReplications, dbSchema.Transfers)
	convertCoordinationNodes(database, dbSchema.CoordinationNodes)
	convertSecrets(database, dbSchema.Secrets)
	convertExternalObjects(database, dbSchema)
	convertExtendedProperties(database, dbSchema.ExtendedProperties)
	convertRoles(database, dbSchema.Roles, membershipsFor(dbSchema.RoleMemberships, dialect))
	database.DatabasePath = dbSchema.DatabasePath
	database.Grants = convertGrants(dbSchema.Grants, replayedColumnSequences(dbSchema.Tables))
	database.RevokedGrants = revokedPublicExecute(dbSchema.Grants)
	database.DefaultPrivileges = convertDefaultPrivileges(dbSchema.DefaultPrivileges)
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
		RevokedGrants:     make([]schemamodel.Grant, 0),
		DefaultPrivileges: make([]schemamodel.DefaultPrivilege, 0),
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
			Name:    dbEnum.Name,
			Schema:  dbEnum.Schema,
			Values:  dbEnum.Values,
			Comment: dbEnum.Comment,
		})
	}
}

func convertTablesAndFields(
	database *schemamodel.Database,
	dbSchema *catalog.Database,
	fkByColumn map[tableMemberKey]foreignKeyInfo,
	tablePrimaryKeys map[string]tablePrimaryKey,
	tablePKColumns map[string]map[string]bool,
	dialect string,
) map[string]string {
	tableStructNames := assignTableStructNames(dbSchema.Tables)
	for _, dbTable := range dbSchema.Tables {
		structName := tableStructNames[dbTable.QualifiedName()]
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
			// So do the deferral and the access method, for the same reason.
			PrimaryKeyDeferrable: primaryKey.deferrable,
			PrimaryKeyInitially:  primaryKey.initially,
			PrimaryKeyMethod:     primaryKey.method,
			PrimaryKeyComment:    primaryKey.comment,
			PrimaryKeyBlockSize:  primaryKey.blockSize,
			Strict:               dbTable.Strict,
			WithoutRowID:         dbTable.WithoutRowID,
			Unlogged:             dbTable.Unlogged,
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
			YDBColumnFamilies: ast.CloneYDBColumnFamilies(dbTable.YDBColumnFamilies),
			Changefeeds:       ast.CloneChangefeeds(dbTable.Changefeeds),
			YDBPartitioning:   dbTable.YDBPartitioning.Clone(),
			Overrides:         tableStorageOverrides(dbTable),
		}
		database.Tables = append(database.Tables, table)

		// Convert columns to fields
		for _, dbColumn := range dbTable.Columns {
			opts := catalogfield.Options{
				CoveredByTablePrimaryKey: tablePKColumns[dbTable.QualifiedName()][dbColumn.Name],
				Dialect:                  dialect,
			}
			// Carry the field-level foreign key (reference + referential actions)
			// so down migrations can reconstruct it with the prior action.
			if fk, ok := fkByColumn[tableMemberKey{table: dbTable.QualifiedName(), member: dbColumn.Name}]; ok {
				opts.ForeignKey = &catalogfield.ForeignKey{
					Name:        fk.name,
					Reference:   fk.foreign,
					OnDelete:    fk.onDelete,
					OnUpdate:    fk.onUpdate,
					Deferrable:  fk.deferrable,
					Initially:   fk.initially,
					Match:       fk.match,
					NotEnforced: fk.notEnforced,
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

// assignTableStructNames gives every table a struct name of its own, keyed by
// its qualified name.
//
// The model joins a table's columns, indexes and constraints to it through the
// struct name, and the name [dbTableStructName] derives is not one-to-one: it
// capitalizes each underscore-separated part, so "Docs" and docs both derive
// Docs, and "orderItems" and order_items both derive OrderItems. Sharing one,
// two tables are one table to every consumer: `schema inspect` describes each
// with the other's columns, two primary keys included, and `introspect` writes
// one Go type holding both (stokaro/ptah#3647). So a table whose derived name
// is already taken takes the first free numbered form, by the rule every
// reader shares, [uniquename.Next].
//
// Tables claim their names in the byte order of their qualified names. The
// catalog's own order follows the server's collation, which puts "Docs" before
// docs under C and after it under most locales, so taken in that order the
// numbered name would move from one table to the other between two servers
// holding the same schema.
func assignTableStructNames(tables []catalog.Table) map[string]string {
	counts := tableNameCounts(tables)
	ordered := slices.SortedFunc(slices.Values(tables), func(a, b catalog.Table) int {
		return strings.Compare(a.QualifiedName(), b.QualifiedName())
	})
	names := make(map[string]string, len(tables))
	taken := make(map[string]bool, len(tables))
	for _, table := range ordered {
		name := uniquename.Next(dbTableStructName(table, counts), func(name string) bool { return taken[name] })
		taken[name] = true
		names[table.QualifiedName()] = name
	}
	return names
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
			Invisible:     dbIndex.Invisible,
			KeyBlockSize:  dbIndex.KeyBlockSize,
			NullsDistinct: cloneBoolPtr(dbIndex.NullsDistinct),
			Type:          indexType(dbIndex),
			Granularity:   dbIndex.Granularity,

			IncludeColumns: slices.Clone(dbIndex.IncludeColumns),
			StorageParams:  maps.Clone(dbIndex.StorageParams),
			Partitioning:   dbIndex.Partitioning.Clone(),
			Vector:         dbIndex.Vector.Clone(),
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
			// Without it a description of a restrictive policy declares a
			// permissive one, which widens the access it was written to narrow.
			Restrictive: dbPolicy.Restrictive,
			Comment:     dbPolicy.Comment,
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
			// every read-back procedure into a function, which is also what
			// filtering them out at the reader does (stokaro/ptah#1722).
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
			Comment:  domain.Comment,
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
			Name:    composite.Name,
			Schema:  composite.Schema,
			Fields:  fields,
			Comment: composite.Comment,
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
			Comment:        rangeType.Comment,
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
// though the reader finds every one and the HCL surface has a `synonym` block
// (stokaro/ptah#1031). The loss sits between the read and the document, so
// nothing that renders from a hand-built schema can see it
// (stokaro/ptah#2001).
//
// The target is rebuilt from the PARSED parts rather than copied. `Target` is
// base_object_name exactly as the catalog records it, brackets included, and
// [ptah.run/core/schemamodel.Synonym.Target] is the spelling that will be
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

// convertTopics carries the YDB topics a read found into the IR, each with
// the settings and consumers the server holds.
func convertTopics(database *schemamodel.Database, topics []catalog.Topic) {
	for _, topic := range topics {
		database.Topics = append(database.Topics, schemamodel.Topic{
			Name:   topic.Name,
			Schema: topic.Schema,
			Spec:   topic.Spec.Clone(),
		})
	}
}

// convertResourcePools carries the YDB resource pools and classifiers a read
// found into the IR. The pool `default` is left out while it holds no
// setting: YDB creates it with every setting unset, so a declaration of it
// would change nothing, and every model introspected from a database would
// carry it.
func convertResourcePools(
	database *schemamodel.Database,
	pools []catalog.ResourcePool,
	classifiers []catalog.ResourcePoolClassifier,
) {
	for _, pool := range pools {
		if pool.Name == ydbpool.DefaultPool && ydbpool.PoolsEqual(pool.Spec, ast.ResourcePoolSpec{}) {
			continue
		}
		database.ResourcePools = append(database.ResourcePools, schemamodel.ResourcePool{
			Name: pool.Name,
			Spec: pool.Spec.Clone(),
		})
	}
	for _, classifier := range classifiers {
		database.ResourcePoolClassifiers = append(database.ResourcePoolClassifiers, schemamodel.ResourcePoolClassifier{
			Name: classifier.Name,
			Spec: classifier.Spec,
		})
	}
}

// convertReplications carries the YDB async replications and transfers a read
// found into the IR, each as the server holds it. The state stays behind: a
// declaration names none, and a schema made from the read declares the
// objects rather than what was done to them.
func convertReplications(database *schemamodel.Database, replications []catalog.AsyncReplication,
	transfers []catalog.Transfer,
) {
	for _, replication := range replications {
		database.AsyncReplications = append(database.AsyncReplications, schemamodel.AsyncReplication{
			Name:   replication.Name,
			Schema: replication.Schema,
			Spec:   replication.Spec.Clone(),
		})
	}
	for _, transfer := range transfers {
		database.Transfers = append(database.Transfers, schemamodel.Transfer{
			Name:   transfer.Name,
			Schema: transfer.Schema,
			Spec:   transfer.Spec,
		})
	}
}

// convertCoordinationNodes carries the YDB coordination nodes a read found
// into the IR, with the configuration as YDB stores it: a setting nobody set
// stays unset, so the description declares only what the node was given.
func convertCoordinationNodes(database *schemamodel.Database, nodes []catalog.CoordinationNode) {
	for _, node := range nodes {
		database.CoordinationNodes = append(database.CoordinationNodes, schemamodel.CoordinationNode{
			Schema: node.Schema,
			Name:   node.Name,
			Spec:   node.Spec,
		})
	}
}

// convertSecrets carries the YDB secrets a read found into the IR. The read
// holds no value and names no variable, so each secret is declared with the
// variable [ydbsecret.DefaultValueEnv] names for its path: a document written
// from the read declares every secret the database holds, and applying it
// back plans nothing for them, because a secret both sides hold is equal by
// its presence.
func convertSecrets(database *schemamodel.Database, secrets []catalog.Secret) {
	for _, secret := range secrets {
		database.Secrets = append(database.Secrets, schemamodel.Secret{
			Name:     secret.Name,
			Schema:   secret.Schema,
			ValueEnv: ydbsecret.DefaultValueEnv(secret.Schema, secret.Name),
		})
	}
}

// convertExternalObjects carries the YDB external data sources and external
// tables a read found into the IR, as declarations of them: everything the
// read describes of each is what a declaration writes.
func convertExternalObjects(database *schemamodel.Database, dbSchema *catalog.Database) {
	for _, source := range dbSchema.ExternalDataSources {
		database.ExternalDataSources = append(database.ExternalDataSources, schemamodel.ExternalDataSource{
			Name: source.Name, Schema: source.Schema, SourceType: source.SourceType, Location: source.Location,
			AuthMethod: source.AuthMethod, Options: maps.Clone(source.Options),
		})
	}
	for _, table := range dbSchema.ExternalTables {
		converted := schemamodel.ExternalTable{
			Name: table.Name, Schema: table.Schema, DataSource: table.DataSource, Location: table.Location,
			Options: maps.Clone(table.Options),
		}
		for _, column := range table.Columns {
			converted.Columns = append(converted.Columns, schemamodel.ExternalColumn{
				Name: column.Name, Type: column.Type, NotNull: column.NotNull,
			})
		}
		database.ExternalTables = append(database.ExternalTables, converted)
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

			When:     dbTrigger.When,
			OldTable: dbTrigger.OldTable,
			NewTable: dbTrigger.NewTable,
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
		if trigger.RunsDeclaredFunction(dbTrigger.ExecuteFunction) {
			trigger.ExecuteFunction = dbTrigger.ExecuteFunction
		}
		database.Triggers = append(database.Triggers, trigger)
	}
}

// membershipsFor is the memberships a description declares: those of a read
// on a target where a declared membership is planned, and none elsewhere. A
// PostgreSQL read reports its role graph for analysis, and a description that
// declared it would be refused by the target it was read from.
func membershipsFor(memberships []catalog.RoleMembership, dialect string) []catalog.RoleMembership {
	if !capability.ForDialect(dialect).Has(capability.RoleMembership) {
		return nil
	}
	return memberships
}

// convertRoles describes the roles of a read, each with the groups it is a
// member of.
func convertRoles(database *schemamodel.Database, dbRoles []catalog.Role, memberships []catalog.RoleMembership) {
	memberOf := make(map[string][]string)
	for _, membership := range memberships {
		memberOf[membership.Member] = append(memberOf[membership.Member], membership.Role)
	}
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
			Group:       dbRole.Group,
			MemberOf:    memberOf[dbRole.Name],
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
				// A bare name here resolves against the search path, so a
				// description of a table outside the connection's
				// default schema enabled row security on whatever `users` the
				// path found first -- leaving that table with no policy, which
				// returns no rows to anyone but its owner, and leaving the real
				// table with a policy that was never enforced
				// (stokaro/ptah#2201).
				Table:   dbTable.QualifiedName(),
				Comment: "", // Comment not available in Table for RLS enablement
				// Without it a description of a forced table declares a
				// table whose owner reads past every policy.
				Forced: dbTable.RLSForced,
			}
			database.RLSEnabledTables = append(database.RLSEnabledTables, rlsEnabledTable)
		}
	}
}

// tablePrimaryKey is a primary key the description writes as a declaration of
// its own rather than as a flag on one column.
type tablePrimaryKey struct {
	columns    []string
	include    []string
	deferrable bool
	initially  string
	method     string
	comment    string
	blockSize  uint64
}

// primaryKeyMethod is the access method a PRIMARY KEY the catalog reports asks
// for; see [mysqlindex.Method].
func primaryKeyMethod(constraint catalog.Constraint) string {
	if constraint.UsingMethod == nil {
		return ""
	}
	return mysqlindex.Method(*constraint.UsingMethod)
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
// (stokaro/ptah#2199). A deferral is the same kind of payload
// (stokaro/ptah#3824), and so is an access method (stokaro/ptah#3853).
func primaryKeysByTable(dbSchema *catalog.Database, dialect string) map[string]tablePrimaryKey {
	result := make(map[string]tablePrimaryKey)
	for _, constraint := range dbSchema.Constraints {
		if !strings.EqualFold(constraint.Type, "PRIMARY KEY") {
			continue
		}
		columns := constraint.ColumnNamesOrDefault()
		if len(columns) == 0 {
			continue
		}
		method := primaryKeyMethod(constraint)
		comment := ""
		if dialect == platform.MySQL || dialect == platform.MariaDB {
			comment = constraint.Comment
		}
		if len(columns) == 1 && len(constraint.IncludeColumns) == 0 && !constraint.Deferrable && method == "" && comment == "" && constraint.KeyBlockSize == 0 {
			continue
		}
		result[constraint.QualifiedTableName()] = tablePrimaryKey{
			columns:    columns,
			include:    slices.Clone(constraint.IncludeColumns),
			deferrable: constraint.Deferrable,
			initially:  constraint.Initially,
			method:     method,
			comment:    comment,
			blockSize:  constraint.KeyBlockSize,
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
//
// A column keeps the flag when the flag is its own key: the catalog holds a
// UNIQUE over the column alone that no index or constraint of the description
// names, because [convertConstraint] left it to the flag. The named constraint
// is then a second key.
// Measured on PostgreSQL 18.6, `CREATE TABLE g2 (a int, UNIQUE (a))` and
// `ALTER TABLE g2 ADD UNIQUE (a)` build g2_a_key and g2_a_key1. Cleared for
// g2_a_key1, the flag takes g2_a_key out of the description, and a diff of the
// database with itself plans to drop it (stokaro/ptah#3819).
func clearColumnUniqueForNamedConstraints(
	database *schemamodel.Database,
	dbSchema *catalog.Database,
	tableStructNames map[string]string,
) {
	ownKeys := columnOwnUniqueKeys(database, dbSchema, tableStructNames)
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
		key := tableMemberKey{table: field.StructName, member: field.Name}
		if _, own := ownKeys[key]; own {
			continue
		}
		if _, isNamed := named[key]; isNamed {
			field.Unique = false
		}
	}
}

// columnOwnUniqueKeys answers the columns whose `unique = true` stands for a
// key of their own, keyed by struct and column name: a UNIQUE over the column
// alone that no index or constraint of database describes by its name.
func columnOwnUniqueKeys(
	database *schemamodel.Database,
	dbSchema *catalog.Database,
	tableStructNames map[string]string,
) map[tableMemberKey]struct{} {
	described := make(map[tableMemberKey]struct{}, len(database.Indexes)+len(database.Constraints))
	for _, index := range database.Indexes {
		described[tableMemberKey{table: index.StructName, member: index.Name}] = struct{}{}
	}
	for _, constraint := range database.Constraints {
		described[tableMemberKey{table: constraint.StructName, member: constraint.Name}] = struct{}{}
	}
	own := make(map[tableMemberKey]struct{})
	for _, constraint := range dbSchema.Constraints {
		columns := constraint.ColumnNamesOrDefault()
		if !strings.EqualFold(constraint.Type, "UNIQUE") || len(columns) != 1 {
			continue
		}
		structName := structNameForTable(tableStructNames, constraint.QualifiedTableName(), constraint.TableName)
		if _, ok := described[tableMemberKey{table: structName, member: constraint.Name}]; ok {
			continue
		}
		own[tableMemberKey{table: structName, member: columns[0]}] = struct{}{}
	}
	return own
}

// convertConstraints converts the constraints no column and no index carries.
// besideColumn names, by table and constraint name, the single-column foreign
// keys a column does not carry; see [indexForeignKeysByColumn].
func convertConstraints(
	dbSchema *catalog.Database,
	tableStructNames map[string]string,
	indexDescribed, besideColumn map[tableMemberKey]struct{},
) []schemamodel.Constraint {
	constraints := make([]schemamodel.Constraint, 0, len(dbSchema.Constraints))
	for _, dbConstraint := range dbSchema.Constraints {
		key := tableMemberKey{table: dbConstraint.QualifiedTableName(), member: dbConstraint.Name}
		if _, ok := indexDescribed[key]; ok {
			continue
		}
		if _, beside := besideColumn[key]; !beside && carriedByItsColumn(dbConstraint) {
			continue
		}
		constraint, ok := convertConstraint(dbConstraint, tableStructNames)
		if ok {
			constraints = append(constraints, constraint)
		}
	}
	return constraints
}

// carriedByItsColumn reports whether a constraint is a foreign key over at most
// one column, which the column carries unless another key over it does. A key
// the server has not validated is not: a column's reference has no room for
// NOT VALID, so it stays a constraint of the table.
func carriedByItsColumn(dbConstraint catalog.Constraint) bool {
	return strings.EqualFold(dbConstraint.Type, "FOREIGN KEY") && len(dbConstraint.ColumnNamesOrDefault()) <= 1 &&
		!dbConstraint.NotValid
}

func convertConstraint(dbConstraint catalog.Constraint, tableStructNames map[string]string) (schemamodel.Constraint, bool) {
	constraintType := strings.ToUpper(dbConstraint.Type)
	columns := dbConstraint.ColumnNamesOrDefault()
	switch constraintType {
	case "PRIMARY KEY":
		return schemamodel.Constraint{}, false
	case "FOREIGN KEY":
		// A key over one column reaches here only when its column carries
		// another; see convertConstraints.
	case "UNIQUE":
		// A single-column UNIQUE is normally carried by the column's own
		// `unique = true`, which is the compact spelling and the one a person
		// writing a schema uses. That spelling has no room for a NAME, so a
		// constraint somebody named was described without it and came back
		// under the server's generated one -- `customers_email_key` where the
		// author had written `customers_email_uq`. A constraint name is an
		// interface: it appears in every violation error the application sees
		// (stokaro/ptah#2102).
		//
		// NULLS NOT DISTINCT is the exception: the column's `unique = true`
		// has no room for it either, so such a key stays a constraint.
		// Measured on PostgreSQL 18.6, `a int UNIQUE NULLS NOT DISTINCT`
		// builds `<table>_a_key`, and described as the column's flag it
		// compares equal to a plain UNIQUE (stokaro/ptah#3821). A deferrable
		// key is the other exception, for the same reason
		// (stokaro/ptah#3824).
		if len(columns) <= 1 && generatedUniqueConstraintName(dbConstraint, columns) &&
			!nullsNotDistinct(dbConstraint) && !dbConstraint.Deferrable {
			return schemamodel.Constraint{}, false
		}
	case "CHECK":
		if dbConstraint.CheckClause == nil || strings.TrimSpace(*dbConstraint.CheckClause) == "" {
			return schemamodel.Constraint{}, false
		}
		if catalogfield.IsNotNullRow(dbConstraint) {
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
		KeyBlockSize:    dbConstraint.KeyBlockSize,
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
		OnDeleteColumns: slices.Clone(dbConstraint.OnDeleteColumns),
		Deferrable:      dbConstraint.Deferrable,
		Initially:       dbConstraint.Initially,
		Match:           dbConstraint.Match,
		NotEnforced:     dbConstraint.NotEnforced,
		NotValid:        dbConstraint.NotValid,
		Comment:         dbConstraint.Comment,
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
// INCLUDE payload, visibility, and MySQL-family index options cannot be kept
// by the UNIQUE constraint representation. Describe such an object as an
// index so a DB read and rollback preserve the complete definition.
func indexDescribedUniques(dbSchema *catalog.Database, dialect string) map[tableMemberKey]struct{} {
	covering := make(map[tableMemberKey]struct{})
	for _, index := range dbSchema.Indexes {
		if !index.IsUnique || index.IsPrimary || !uniqueNeedsIndexDescription(index, dialect) {
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
// [ptah.run/internal/clickhouserbac.ValidateLive] refuses to compare a
// managed role carrying one at all rather than leaving this function to
// approximate it.
// replayedColumnSequences maps each sequence a read column owns, by the target
// a grant names it with, to the name the description's replay gives it.
//
// The description writes such a column as its serial type or identity clause,
// which creates the sequence under the name PostgreSQL gives it, [pgname.Sequence].
// The database may hold another one: renaming the table or the column leaves
// the sequence's name as it was, so products.id can own items_id_seq. A
// description that named items_id_seq in a grant did not replay, because the
// replay creates products_id_seq (stokaro/ptah#4064). The comparator keys a
// grant on such a sequence by its column, so the two names compare equal.
func replayedColumnSequences(tables []catalog.Table) map[string]string {
	replayed := make(map[string]string)
	for _, table := range tables {
		for _, column := range table.Columns {
			if column.OwnedSequence == "" {
				continue
			}
			replayed[catalog.QualifyTableName(table.Schema, column.OwnedSequence)] =
				catalog.QualifyTableName(table.Schema, pgname.Sequence(table.Name, column.Name))
		}
	}
	return replayed
}

// convertGrants describes the grants of a read. replayed renames a grant on a
// column's sequence; see [replayedColumnSequences].
func convertGrants(dbGrants []catalog.Grant, replayed map[string]string) []schemamodel.Grant {
	grants := make([]schemamodel.Grant, 0, len(dbGrants))
	for _, dbGrant := range dbGrants {
		if dbGrant.IsPartialRevoke || dbGrant.Implicit {
			// An implicit row is the privilege a routine holds because it
			// exists, which the routine's own declaration already implies.
			continue
		}
		grant := schemamodel.Grant{
			Role:       dbGrant.Role,
			Privileges: []string{dbGrant.Privilege},
			WithOption: dbGrant.WithOption,
			GrantedBy:  dbGrant.GrantedBy,
		}
		switch {
		case strings.EqualFold(dbGrant.ObjectType, "DATABASE"):
			grant.OnDatabase = true
		case strings.EqualFold(dbGrant.ObjectType, "SCHEMA"):
			grant.OnSchema = dbGrant.ObjectName
		case routineGrantObjectTypes[strings.ToUpper(dbGrant.ObjectType)]:
			grant.OnRoutine = dbGrant.QualifiedTarget()
			grant.RoutineArguments = dbGrant.Arguments
			grant.RoutineKind = strings.ToUpper(dbGrant.ObjectType)
		case strings.EqualFold(dbGrant.ObjectType, "SEQUENCE"):
			// PostgreSQL accepts GRANT ... ON TABLE for a sequence, so a sequence
			// described under OnTable still replays. The comparator keys a grant
			// by its object type, though, and the read reports SEQUENCE, so that
			// description would never match the row it was made from.
			grant.OnSequence = dbGrant.QualifiedTarget()
			if name, ok := replayed[grant.OnSequence]; ok {
				grant.OnSequence = name
			}
		default:
			grant.OnTable = dbGrant.QualifiedTarget()
			if dbGrant.Column != "" {
				grant.Columns = []string{dbGrant.Column}
			}
		}
		grant.Canonicalize()
		grants = append(grants, grant)
	}
	return grants
}

// routineGrantObjectTypes are the object types a catalog read reports for a
// privilege on a function or procedure.
var routineGrantObjectTypes = map[string]bool{"FUNCTION": true, "PROCEDURE": true}

// revokedPublicExecute describes the routines whose PUBLIC EXECUTE a catalog
// read shows was revoked.
//
// PostgreSQL gives PUBLIC EXECUTE on a routine when it is created, and the read
// reports that as an Implicit row until the routine's ACL is first written. A
// routine whose ACL was written and holds no PUBLIC EXECUTE had it revoked, and
// a description that said nothing about it would recreate the routine with the
// privilege back. So each such routine gets a revoked grant. A routine with no
// row at all is outside the read and is left alone.
func revokedPublicExecute(dbGrants []catalog.Grant) []schemamodel.Grant {
	type routine struct{ kind, target, arguments string }
	held := make(map[routine]bool)
	var order []routine
	for _, dbGrant := range dbGrants {
		kind := strings.ToUpper(dbGrant.ObjectType)
		if !routineGrantObjectTypes[kind] {
			continue
		}
		key := routine{kind: kind, target: dbGrant.QualifiedTarget(), arguments: dbGrant.Arguments}
		if _, seen := held[key]; !seen {
			order = append(order, key)
			held[key] = false
		}
		if dbGrant.Role == "PUBLIC" && strings.EqualFold(dbGrant.Privilege, "EXECUTE") {
			held[key] = true
		}
	}
	revoked := make([]schemamodel.Grant, 0)
	for _, key := range order {
		if held[key] {
			continue
		}
		revoked = append(revoked, schemamodel.Grant{
			Role:             "PUBLIC",
			Privileges:       []string{"EXECUTE"},
			OnRoutine:        key.target,
			RoutineArguments: key.arguments,
			RoutineKind:      key.kind,
		})
	}
	return revoked
}

// defaultPrivilegeIdentity is what convertDefaultPrivileges groups rows by:
// the whole identity of a default-privilege object, since the catalog stores no
// name for one.
//
// A struct rather than a joined string, because the components are role, schema
// and object-type names and none of them is barred from holding whatever
// separator a joined key would pick.
type defaultPrivilegeIdentity struct {
	grantor    string
	schema     string
	objectType string
	grantee    string
}

// convertDefaultPrivileges folds catalog rows back into declarations, one per
// identity.
//
// The read's grain is one privilege per row, which is what aclexplode answers,
// and a declaration carries the whole privilege list of one identity. So this is
// the inverse of the fan-out in
// [ptah.run/internal/convert/goschematodb.ToDBSchema] and has to agree with it
// on cardinality: describing each row as its own declaration would claim one
// object per privilege where the server holds one per identity, and a
// comparison against the read those rows came from would plan a change on every
// run.
//
// Rows are grouped in the order they arrive, and the privileges of one identity
// keep the order of their rows, so the description of one read is stable. A read
// that found none gives an empty slice rather than nil, copying convertGrants
// and the initializer in newDatabase.
//
// A row marked [catalog.DefaultPrivilege.Revoked] is a global default taking a
// built-in privilege away, and becomes an entry of the declaration's Revoked:
// a new database starts from the built-in default, so the description has to
// say the revoke for the privilege to be gone there too.
func convertDefaultPrivileges(dbPrivileges []catalog.DefaultPrivilege) []schemamodel.DefaultPrivilege {
	position := make(map[defaultPrivilegeIdentity]int, len(dbPrivileges))
	privileges := make([]schemamodel.DefaultPrivilege, 0, len(dbPrivileges))
	for _, dbPrivilege := range dbPrivileges {
		declaration := schemamodel.DefaultPrivilege{
			Grantor:    dbPrivilege.Grantor,
			Schema:     dbPrivilege.Schema,
			ObjectType: dbPrivilege.ObjectType,
			Grantee:    dbPrivilege.Grantee,
			Privileges: []schemamodel.PrivilegeGrant{{
				Privilege:  dbPrivilege.Privilege,
				WithOption: dbPrivilege.WithOption,
			}},
		}
		if dbPrivilege.Revoked {
			declaration.Privileges = make([]schemamodel.PrivilegeGrant, 0)
			declaration.Revoked = []string{dbPrivilege.Privilege}
		}
		// Canonicalize before keying, so two rows whose identity differs only in
		// case or padding group together rather than becoming twin declarations
		// the comparison can never resolve.
		declaration.Canonicalize()
		key := defaultPrivilegeIdentity{
			grantor:    declaration.Grantor,
			schema:     declaration.Schema,
			objectType: declaration.ObjectType,
			grantee:    declaration.Grantee,
		}
		index, seen := position[key]
		if !seen {
			position[key] = len(privileges)
			privileges = append(privileges, declaration)
			continue
		}
		privileges[index].Privileges = append(privileges[index].Privileges, declaration.Privileges...)
		privileges[index].Revoked = append(privileges[index].Revoked, declaration.Revoked...)
		// Canonicalize again on the merged list: one identity holding the same
		// privilege name twice keeps the grantable spelling, the way PostgreSQL
		// merges two such statements.
		privileges[index].Canonicalize()
	}
	return privileges
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
	// match and notEnforced carry the MATCH type and enforcement, for the
	// same reason (stokaro/ptah#3853).
	match       string
	notEnforced bool
}

type tableMemberKey struct {
	table  string
	member string
}

// columnForeignKeys is where the database's single-column foreign keys go in
// the model.
type columnForeignKeys struct {
	// byColumn is the key each column carries, by table and column.
	byColumn map[tableMemberKey]foreignKeyInfo
	// besideColumn names, by table and constraint name, the other keys over a
	// column that already carries one. They stay constraints of the table.
	besideColumn map[tableMemberKey]struct{}
}

// indexForeignKeysByColumn decides which single-column FOREIGN KEY each column
// carries. Multi-column keys are not field-level and are left to
// [convertConstraints].
//
// A column carries one key. PostgreSQL 18.6 and MySQL 8.4.11 build two keys
// over one column when a table declares both, so a column with more than one
// carries the key whose name sorts first, and the rest stay constraints of the
// table. Carried on the column alone, the second key would be lost, and a
// database compared with itself would plan to drop one (stokaro/ptah#3873).
func indexForeignKeysByColumn(dbSchema *catalog.Database) columnForeignKeys {
	byColumn := make(map[tableMemberKey][]catalog.Constraint)
	for _, c := range dbSchema.Constraints {
		// A key the server has not validated stays a constraint of the table:
		// a column's reference has no room for NOT VALID.
		if c.Type != "FOREIGN KEY" || c.ColumnName == "" || c.ForeignTable == nil ||
			len(c.ColumnNamesOrDefault()) != 1 || c.NotValid {
			continue
		}
		column := tableMemberKey{table: c.QualifiedTableName(), member: c.ColumnName}
		byColumn[column] = append(byColumn[column], c)
	}
	result := columnForeignKeys{
		byColumn:     make(map[tableMemberKey]foreignKeyInfo, len(byColumn)),
		besideColumn: make(map[tableMemberKey]struct{}),
	}
	for column, keys := range byColumn {
		slices.SortFunc(keys, func(a, b catalog.Constraint) int { return strings.Compare(a.Name, b.Name) })
		result.byColumn[column] = columnForeignKey(keys[0])
		for _, beside := range keys[1:] {
			result.besideColumn[tableMemberKey{table: column.table, member: beside.Name}] = struct{}{}
		}
	}
	return result
}

// columnForeignKey is the field-level description of a single-column key.
func columnForeignKey(c catalog.Constraint) foreignKeyInfo {
	foreignTable := c.QualifiedForeignTableName()
	foreignColumn := ""
	if foreignColumns := c.ForeignColumnsOrDefault(); len(foreignColumns) == 1 {
		foreignColumn = foreignColumns[0]
	}
	foreign := foreignTable
	if foreignColumn != "" {
		foreign = foreignTable + "(" + foreignColumn + ")"
	}
	// An ON DELETE column list on a one-column key can name only that
	// column, which is what no list means, so the field needs none.
	return foreignKeyInfo{
		name:        c.Name,
		foreign:     foreign,
		onDelete:    derefString(c.DeleteRule),
		onUpdate:    derefString(c.UpdateRule),
		deferrable:  c.Deferrable,
		initially:   c.Initially,
		match:       c.Match,
		notEnforced: c.NotEnforced,
	}
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

// nullsNotDistinct reports whether a UNIQUE treats NULLs as equal.
func nullsNotDistinct(constraint catalog.Constraint) bool {
	return constraint.NullsDistinct != nil && !*constraint.NullsDistinct
}

// tableStorageOverrides preserves compression needed to replay MySQL index hints.
func tableStorageOverrides(table catalog.Table) map[string]map[string]string {
	overrides := clickHouseTableOverrides(table)
	if !strings.EqualFold(table.RowFormat, "Compressed") {
		return overrides
	}
	if overrides == nil {
		overrides = make(map[string]map[string]string)
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		overrides[dialect] = map[string]string{"row_format": "COMPRESSED"}
	}
	return overrides
}

func uniqueNeedsIndexDescription(index catalog.Index, dialect string) bool {
	if len(index.IncludeColumns) > 0 || index.Invisible {
		return true
	}
	if dialect == platform.MySQL || dialect == platform.MariaDB {
		return index.KeyBlockSize != 0 || index.Comment != "" || mysqlindex.Method(index.Method) != ""
	}
	return false
}
