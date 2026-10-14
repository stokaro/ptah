// Package goschematodb converts Ptah's desired-schema IR into the DB schema
// shape used by schema comparison. It sits with the other representation
// conversions under internal/convert and serves the comparisons whose current
// side is another desired-schema document rather than a live database reader:
// the file-to-file schema diff and schemadiff.CompareSchemas.
package goschematodb

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/cockroachtype"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/columnsequence"
	"ptah.run/internal/convert/features"
	"ptah.run/internal/mysqlindex"
	"ptah.run/internal/oracletype"
	"ptah.run/internal/routineargs"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/sqlitekey"
	"ptah.run/internal/tableref"
)

// ToDBSchema converts Ptah's desired-schema IR into the DB schema shape used by
// schema comparison. It is intended for comparisons where the current side is
// another desired-schema document -- a local file-to-file diff, or two
// in-memory schemas -- and no live database reader is involved.
//
// dialect names the target the converted schema will be compared under. It
// decides how the one goschema field that carries two concepts is unpacked:
// schemamodel.Index.Type is the PostgreSQL access method on a PostgreSQL-family
// target and the ClickHouse data-skipping-index type on ClickHouse, and the DB
// shape keeps those apart in Method and Type. On a PostgreSQL-family target it
// also adds the EXECUTE that PUBLIC holds on every declared routine the
// document does not revoke it from, because a database built from the document
// would hold it. The target must be registered in runtime. Named feature
// objects and facets use its batched conversion service; a failed conversion
// returns no schema.
func ToDBSchema(ctx context.Context, db *schemamodel.Database, dialect string, runtime Runtime) (*catalog.Database, error) {
	if db == nil {
		db = &schemamodel.Database{}
	}
	if err := schemaext.RequireRuntime(ctx, runtime); err != nil {
		return nil, err
	}
	selected, err := runtime.ResolveTarget(dialect)
	if err != nil {
		return nil, err
	}
	dialect = selected.Name()
	db, prepared, err := prepareSourceTables(ctx, db, dialect, runtime)
	if err != nil {
		return nil, err
	}
	// A document converted to stand for a database holds what the target's
	// reader would report, such as the unique index YDB holds for a UNIQUE
	// constraint. No database is read, so nothing is adopted from one.
	db, err = runtime.LowerDesired(ctx, schemapreparation.LoweringRequest{
		Target: dialect, Desired: db, Capabilities: capability.ForDialect(dialect), Semantics: identifier.ForDialect(dialect),
	})
	if err != nil {
		return nil, err
	}
	// A target without standalone enums builds an enum column inline, and a
	// read reports what it built: enum('a','b') on MySQL and MariaDB, a text
	// column with a CHECK on SQLite, SQL Server and Oracle. The comparison
	// lowers the declared side by the same function.
	if schemaprep.InlineEnumDialect(dialect) {
		lowered := *db
		lowered.Fields = schemaprep.LowerInlineEnums(db.Fields, db.Enums, dialect)
		db = &lowered
	}

	tableByStruct := make(map[string]schemamodel.Table, len(db.Tables))
	for _, table := range db.Tables {
		tableByStruct[table.StructName] = table
	}

	out := &catalog.Database{
		Facets:      db.Facets,
		Schemas:     toDBSchemas(db.Schemas),
		Tables:      toDBTables(db.Tables, db.Fields, db.RLSEnabledTables, dialect),
		Enums:       toDBEnums(db.Enums),
		Indexes:     toDBIndexes(db.Indexes, tableByStruct, dialect),
		Constraints: toDBConstraints(db, tableByStruct, dialect),
		Functions:   toDBFunctions(db.Functions),
		Sequences:   toDBSequences(db.Sequences),
		Views:       toDBViews(db.Views),
		MatViews:    toDBMaterializedViews(db.MaterializedViews),
		Triggers:    toDBTriggers(db.Triggers, tableByStruct),
		RLSPolicies: toDBRLSPolicies(db.RLSPolicies),
		Roles:       toDBRoles(db.Roles),
		Grants:      toDBGrants(db, dialect),

		RoleMemberships: toDBRoleMemberships(db.Roles),
		// A default privilege is an object family of its own rather than an
		// attribute of a grant. Without this line the declaration still parses
		// and the model still carries it, while this side of a file-to-file
		// comparison reports none, so the diff plans the same ALTER DEFAULT
		// PRIVILEGES on every run and never converges.
		// A file-to-file comparison uses this side as the current state, and a
		// document that declared its own limits declares them here too
		// (stokaro/ptah#1276).
		NotDescribed: db.NotDescribed,
	}
	applyTablePrimaryKeys(out, db.Tables)
	applyConstraintPrimaryKeys(out)
	applyPreparedColumnKeys(out, prepared)
	if err := features.RequireFacetPreservation(db.FacetSlots(), out.FacetSlots()); err != nil {
		return nil, err
	}
	objects, coverage, err := features.Convert(ctx, runtime, dialect, schemaext.Desired, schemaext.Observed, db.FeatureObjects, db.FeatureCoverage, out.FacetSlots())
	if err != nil {
		return nil, err
	}
	out.FeatureObjects, out.FeatureCoverage = objects, coverage
	if err := applyCreationObservations(out, prepared, identifier.ForDialect(dialect), dialect); err != nil {
		return nil, err
	}
	return out, nil
}

func toDBSchemas(schemas []schemamodel.Schema) []catalog.Schema {
	out := make([]catalog.Schema, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, catalog.Schema{
			Facets:  schema.Facets,
			Name:    schema.Name,
			Comment: schema.Comment,
			Charset: schema.Charset,
			Collate: schema.Collate,
		})
	}
	return out
}

func toDBTables(
	tables []schemamodel.Table,
	fields []schemamodel.Field,
	rlsEnabledTables []schemamodel.RLSEnabledTable,
	dialect string,
) []catalog.Table {
	fieldsByStruct := make(map[string][]schemamodel.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]catalog.Table, 0, len(tables))
	for _, table := range tables {
		enablement, rlsEnabled := tableRLSEnablement(table, rlsEnabledTables)
		out = append(out, catalog.Table{
			Facets:     table.Facets,
			Name:       table.Name,
			Schema:     table.Schema,
			Type:       "TABLE",
			Comment:    table.Comment,
			RowFormat:  table.Overrides[platform.NormalizeDialect(dialect)]["row_format"],
			Columns:    toDBColumns(table, fieldsByStruct[table.StructName], dialect),
			RLSEnabled: rlsEnabled,
			RLSForced:  rlsEnabled && enablement.Forced,
		})
	}
	return out
}

// tableRLSEnablement finds the enablement a table declares, if any. Both flags
// the catalog carries -- enabled and forced -- are read off the one entry this
// finds, so the two cannot be matched to different declarations.
func tableRLSEnablement(
	table schemamodel.Table,
	enabledTables []schemamodel.RLSEnabledTable,
) (schemamodel.RLSEnabledTable, bool) {
	index := slices.IndexFunc(enabledTables, func(enabled schemamodel.RLSEnabledTable) bool {
		return enabled.StructName != "" && enabled.StructName == table.StructName ||
			enabled.Table == table.QualifiedName() ||
			table.Schema == "" && enabled.Table == table.Name
	})
	if index < 0 {
		return schemamodel.RLSEnabledTable{}, false
	}
	return enabledTables[index], true
}

// toDBColumns describes a table's columns the way the server would report
// them.
//
// A key column is NOT NULL on the server whatever it declares -- on SQLite,
// depending on the table's shape -- and the comparison normalizes the desired
// side to that answer. A schema file read as the current side of a diff keeps
// `id bigint PRIMARY KEY` as a nullable field, so without the same answer here
// the same file compared with itself planned SET NOT NULL on every key column
// (stokaro/ptah#3658).
func toDBColumns(table schemamodel.Table, fields []schemamodel.Field, dialect string) []catalog.Column {
	keyColumns := sqlitekey.KeyColumns(table, fields)
	out := make([]catalog.Column, 0, len(fields))
	for i, field := range fields {
		if sqlitekey.KeyColumnIsNotNull(dialect, table, keyColumns, field) {
			field.Nullable = false
		}
		column := toDBColumn(field, i+1)
		if dialect == platform.Oracle {
			// Oracle stores few declared type names as written: a TEXT
			// column is a CLOB and a BIGINT a NUMBER(19). The reader reports
			// what Oracle stores, and the comparison folds the declared side
			// through the same mapping, so a declared type kept here compared
			// unequal to itself and planned a MODIFY for every column.
			column.DataType = oracletype.Map(field.Type)
			column.ColumnType = column.DataType
		}
		if dialect == platform.CockroachDB {
			// CockroachDB builds a column declared INT or INTEGER at the
			// session's integer width, and a read reports that width. The
			// comparison resolves the declared side the same way, so the
			// declared spelling kept here planned ALTER COLUMN ... TYPE
			// INTEGER for every such column. No session is read here, so
			// the width is CockroachDB's own default, as it is for an offline
			// comparison.
			column.DataType = cockroachtype.Resolve(field.Type, 0)
			column.ColumnType = column.DataType
		}
		if platform.IsPostgresFamily(dialect) {
			column.OwnedSequence, _ = columnsequence.Declared(table.Name, field)
		}
		out = append(out, column)
	}
	return out
}

func toDBColumn(field schemamodel.Field, ordinal int) catalog.Column {
	nullable := "NO"
	if field.Nullable {
		nullable = "YES"
	}
	column := catalog.Column{
		Facets:     field.Facets,
		Name:       field.Name,
		DataType:   field.Type,
		ColumnType: field.Type,
		IsNullable: nullable,
		// A description's own name for the NOT NULL, so a comparison between
		// two descriptions sees it the way it sees one read from a catalog.
		NotNullConstraintName: field.NotNullConstraintName,
		OrdinalPosition:       ordinal,
		IsAutoIncrement:       field.AutoInc || field.IdentityGeneration != "",
		IsPrimaryKey:          field.Primary,
		IsUnique:              field.Unique,
		Collate:               field.Collate,
		// The column's own comment, which a read reports beside it. Without
		// it, a schema file compared with itself planned the comment again
		// on every commented column.
		Comment:       field.Comment,
		GeneratedKind: field.GeneratedKind,
		// The start and the increment a declaration gives an identity or a
		// Serial, which a YDB comparison reads a Serial's sequence by: a
		// description compared with itself has to find them equal.
		IdentityStart:     field.IdentityStart,
		IdentityIncrement: field.IdentityIncrement,
	}
	if field.DefaultSet {
		column.ColumnDefault = new(field.Default)
	} else if field.DefaultExpr != "" {
		column.ColumnDefault = new(field.DefaultExpr)
	}
	if field.GeneratedExpression != "" {
		column.GeneratedExpression = new(field.GeneratedExpression)
	}
	return column
}

func applyTablePrimaryKeys(schema *catalog.Database, tables []schemamodel.Table) {
	primaryByTable := make(map[string]map[string]struct{})
	for _, table := range tables {
		if len(table.PrimaryKey) == 0 {
			continue
		}
		columns := make(map[string]struct{}, len(table.PrimaryKey))
		for _, column := range table.PrimaryKey {
			columns[column] = struct{}{}
		}
		primaryByTable[table.QualifiedName()] = columns
	}
	for tableIdx, table := range schema.Tables {
		columns := primaryByTable[table.QualifiedName()]
		if len(columns) == 0 {
			continue
		}
		for columnIdx, column := range table.Columns {
			if _, ok := columns[column.Name]; ok {
				schema.Tables[tableIdx].Columns[columnIdx].IsPrimaryKey = true
			}
		}
	}
}

// applyConstraintPrimaryKeys marks the columns of every PRIMARY KEY declared
// as a table constraint, as a read marks the columns of the key it reports.
//
// A key declared through the table or a field is marked where its columns are
// described. One declared as a constraint was left unmarked, and dbschematogo
// reads a single-column key from the column's mark: converted back, the
// declaration had no primary key, and the plan dropped the one it came from.
func applyConstraintPrimaryKeys(schema *catalog.Database) {
	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, "PRIMARY KEY") {
			continue
		}
		columns := constraint.ColumnNamesOrDefault()
		for tableIdx, table := range schema.Tables {
			if table.QualifiedName() != constraint.QualifiedTableName() {
				continue
			}
			for columnIdx, column := range table.Columns {
				if slices.Contains(columns, column.Name) {
					schema.Tables[tableIdx].Columns[columnIdx].IsPrimaryKey = true
				}
			}
		}
	}
}

func toDBEnums(enums []schemamodel.Enum) []catalog.Enum {
	out := make([]catalog.Enum, 0, len(enums))
	for _, enum := range enums {
		out = append(out, catalog.Enum{
			Facets:  enum.Facets,
			Name:    enum.Name,
			Schema:  enum.Schema,
			Values:  append([]string(nil), enum.Values...),
			Comment: enum.Comment,
		})
	}
	return out
}

// toDBIndexes converts desired-state indexes into the DB shape.
//
// Everything the comparator reads has to survive this hop. Before issue #1272
// the access method, the structured key parts and the INCLUDE payload were
// dropped here, which was invisible only because the PostgreSQL comparator
// ignored all three; once it started reading them, a `schema diff` whose
// --from side is a local file would have reported a rebuild for every index
// carrying any of them against the database it was inspected from.
func toDBIndexes(
	indexes []schemamodel.Index,
	tables map[string]schemamodel.Table,
	dialect string,
) []catalog.Index {
	out := make([]catalog.Index, 0, len(indexes))
	for _, index := range indexes {
		tableName, schema := indexTable(index.StructName, index.TableName, tables)
		out = append(out, catalog.Index{
			Facets:         index.Facets,
			Name:           index.Name,
			TableName:      tableName,
			Schema:         schema,
			Columns:        append([]string(nil), index.Fields...),
			Parts:          toDBIndexParts(index.Parts, index.Fields, index.Operator),
			IsUnique:       index.Unique,
			Condition:      index.Condition,
			Comment:        index.Comment,
			Invisible:      index.Invisible,
			NullsDistinct:  clonePtr(index.NullsDistinct),
			Method:         indexAccessMethod(index.Type, dialect),
			IncludeColumns: append([]string(nil), index.IncludeColumns...),
			StorageParams:  maps.Clone(index.StorageParams),
			Requires:       slices.Clone(index.Requires),
		})
	}
	return out
}

// indexAccessMethod reports schemamodel.Index.Type as an access method only where
// that is what it means. On ClickHouse the selected owner consumes the field as
// the data-skipping-index type, a facet rather than an access method. On YDB it
// carries how the index is maintained, synchronously or not, and the YDB
// comparison reads that from Method on the current side.
func indexAccessMethod(indexType, dialect string) string {
	switch {
	case platform.IsPostgresFamily(dialect), platform.NormalizeDialect(dialect) == platform.YDB:
		return indexType
	case platform.NormalizeDialect(dialect) == platform.MySQL, platform.NormalizeDialect(dialect) == platform.MariaDB:
		// A MySQL-family read reports INDEX_TYPE here, and the comparison
		// reads the index's kind and method from it through mysqlindex. Left
		// empty, a declared FULLTEXT or SPATIAL index compared with its own
		// conversion read as a plain one and was dropped and added again.
		if kind := mysqlindex.KindOf(indexType); kind != mysqlindex.Plain {
			return string(kind)
		}
		return mysqlindex.Method(indexType)
	default:
		return ""
	}
}

// toDBIndexParts converts structured key parts, resolving the index-level
// operator class the way the renderer applies it: a part without its own class
// inherits the index's, so the DB shape -- which has no index-level slot --
// records the resolved value per part.
//
// An index that names its columns as Fields and an operator class for the
// whole index has no parts to carry the class, so each column becomes a part
// that holds it, as a read reports the class of every key. Without them the
// class was lost, and a GIN index on gin_trgm_ops compared with its own
// conversion was dropped and created again.
func toDBIndexParts(parts []schemamodel.IndexPart, fields []string, indexOperator string) []catalog.IndexPart {
	if len(parts) == 0 && strings.TrimSpace(indexOperator) != "" {
		for _, field := range fields {
			parts = append(parts, schemamodel.IndexPart{Name: field})
		}
	}
	if len(parts) == 0 {
		return nil
	}
	converted := make([]catalog.IndexPart, len(parts))
	for position, part := range parts {
		operator := part.Operator
		if operator == "" {
			operator = indexOperator
		}
		converted[position] = catalog.IndexPart{
			Name:       part.Name,
			Expr:       part.Expr,
			Operator:   operator,
			Prefix:     part.Prefix,
			Desc:       part.Desc,
			NullsOrder: part.NullsOrder,
		}
	}
	return converted
}

func toDBConstraints(declared *schemamodel.Database, tables map[string]schemamodel.Table, dialect string) []catalog.Constraint {
	tablesList, fields, constraints := declared.Tables, declared.Fields, declared.Constraints
	fieldsByStruct := make(map[string][]schemamodel.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]catalog.Constraint, 0, len(constraints)+len(tablesList)+len(fields))
	type constraintIdentity struct {
		schema string
		table  string
		name   string
		// definition tells unnamed constraints apart. Keyed by the empty name
		// alone, every unnamed constraint of a table after the first would be
		// dropped here, and a `schema diff` whose current side declares two
		// unnamed CHECKs on one table would compare against one of them
		// (stokaro/ptah#3729).
		definition string
	}
	seen := make(map[constraintIdentity]struct{})
	appendConstraint := func(constraint catalog.Constraint) {
		key := constraintIdentity{
			schema: constraint.Schema,
			table:  constraint.TableName,
			name:   constraint.Name,
		}
		if constraint.Name == "" {
			key.definition = unnamedConstraintDefinition(constraint)
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, constraint)
	}

	for _, table := range tablesList {
		if len(table.PrimaryKey) == 0 {
			continue
		}
		appendConstraint(catalog.Constraint{
			// The key's settings ride on the constraint the key becomes.
			Facets:      table.PrimaryKeyFacets,
			Name:        tablePrimaryKeyName(table),
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "PRIMARY KEY",
			ColumnNames: append([]string(nil), table.PrimaryKey...),
			ColumnName:  first(table.PrimaryKey),
			// The key's INCLUDE payload, which a read reports beside its
			// columns and the comparison compares.
			IncludeColumns: slices.Clone(table.PrimaryKeyInclude),
			Deferrable:     table.PrimaryKeyDeferrable,
			Initially:      table.PrimaryKeyInitially,
			UsingMethod:    optionalStringPtr(table.PrimaryKeyMethod),
			Comment:        table.PrimaryKeyComment,
		})
	}
	for _, constraint := range constraints {
		tableName, schema := indexTable(constraint.StructName, constraint.Table, tables)
		dbConstraint := catalog.Constraint{
			Facets:         constraint.Facets,
			Name:           constraint.Name,
			TableName:      tableName,
			Schema:         schema,
			Type:           constraint.Type,
			ColumnNames:    append([]string(nil), constraint.Columns...),
			ColumnName:     first(constraint.Columns),
			CheckClause:    optionalStringPtr(constraint.CheckExpression),
			NullsDistinct:  clonePtr(constraint.NullsDistinct),
			IncludeColumns: append([]string(nil), constraint.IncludeColumns...),
			UsingMethod:    optionalStringPtr(constraint.UsingMethod),
			NotValid:       constraint.NotValid,
			Comment:        constraint.Comment,
			Deferrable:     constraint.Deferrable,
			Initially:      constraint.Initially,
			Match:          constraint.Match,
			NotEnforced:    constraint.NotEnforced,
			Requires:       slices.Clone(constraint.Requires),
		}
		if constraint.ForeignTable != "" {
			foreignTable, foreignSchema := splitTableIdentity(constraint.ForeignTable)
			dbConstraint.ForeignTable = new(foreignTable)
			dbConstraint.ForeignSchema = foreignSchema
			dbConstraint.ForeignColumn = optionalStringPtr(constraint.ForeignColumn)
			dbConstraint.ForeignColumns = append([]string(nil), constraint.ForeignColumnsOrDefault()...)
			dbConstraint.DeleteRule = optionalStringPtr(constraint.OnDelete)
			dbConstraint.UpdateRule = optionalStringPtr(constraint.OnUpdate)
		}
		appendConstraint(dbConstraint)
	}
	for _, table := range tablesList {
		// The names the comparison's other side gives these CHECKs, so a
		// document compared with itself pairs every one of them.
		checkNames := schemaprep.ColumnCheckNames(table, fields, constraints, dialect)
		for _, field := range fieldsByStruct[table.StructName] {
			for _, constraint := range toDBFieldConstraints(table, field, checkNames[field.Name]) {
				appendConstraint(constraint)
			}
		}
		for _, key := range columnOwnKeys(declared, table, fieldsByStruct[table.StructName], dialect) {
			appendConstraint(key)
		}
		// A table's `checks` list renders as named CHECKs, and the comparison's
		// other side holds them. Left out here, a document compared with itself
		// planned every entry as an addition.
		for _, check := range schemaprep.TableCheckConstraints(table, fields, constraints, dialect) {
			appendConstraint(catalog.Constraint{
				Name:        check.Name,
				TableName:   table.Name,
				Schema:      table.Schema,
				Type:        "CHECK",
				CheckClause: new(check.CheckExpression),
			})
		}
	}
	return out
}

// unnamedConstraintDefinition spells what a constraint without a name declares,
// for telling it apart from another unnamed constraint of the same table.
func unnamedConstraintDefinition(constraint catalog.Constraint) string {
	deref := func(value *string) string {
		if value == nil {
			return ""
		}
		return *value
	}
	return strings.Join([]string{
		constraint.Type,
		strings.Join(constraint.ColumnNamesOrDefault(), "\x01"),
		deref(constraint.CheckClause),
		deref(constraint.UsingMethod),
		constraint.QualifiedForeignTableName(),
		strings.Join(constraint.ForeignColumnsOrDefault(), "\x01"),
		// Two constraints that differ only in how they check their rows are
		// two constraints (stokaro/ptah#3853).
		strings.ToUpper(constraint.Match),
		strconv.FormatBool(constraint.NotEnforced),
	}, "\x00")
}

// toDBFieldConstraints converts the CHECK and the foreign key a column declares.
// checkName is the name the column's CHECK takes; see
// [schemaprep.ColumnCheckNames].
// columnOwnKeys describes the key each column's own UNIQUE builds, under the
// name the server gives it, on the engines whose naming [columnkey] measures.
//
// The comparison reads such a column's uniqueness from the database key with
// that name, by the same [columnkey.Name] and [columnkey.Taken] it asks here.
// Without the key, a document compared with itself read every UNIQUE column
// as one without its key, and the plan added the key again.
func columnOwnKeys(declared *schemamodel.Database, table schemamodel.Table, fields []schemamodel.Field, dialect string) []catalog.Constraint {
	if !columnkey.Named(dialect) {
		return nil
	}
	taken := columnkey.Taken(dialect, []*schemamodel.Database{declared}, table)
	var keys []catalog.Constraint
	for _, field := range fields {
		if !field.Unique {
			continue
		}
		name, ok := columnkey.Name(dialect, table.Name, field.Name, taken)
		if !ok {
			continue
		}
		keys = append(keys, catalog.Constraint{
			Name:        name,
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "UNIQUE",
			ColumnName:  field.Name,
			ColumnNames: []string{field.Name},
		})
	}
	return keys
}

func toDBFieldConstraints(table schemamodel.Table, field schemamodel.Field, checkName string) []catalog.Constraint {
	var out []catalog.Constraint
	if field.Check != "" {
		out = append(out, catalog.Constraint{
			Name:        checkName,
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "CHECK",
			ColumnName:  field.Name,
			ColumnNames: []string{field.Name},
			CheckClause: new(field.Check),
			NotEnforced: field.CheckNotEnforced,
		})
	}
	if field.Foreign != "" {
		fkRef := schemaprep.ParseForeignKeyReference(field.Foreign)
		if fkRef == nil {
			return out
		}
		name := field.ForeignKeyName
		if name == "" {
			name = schemaprep.GenerateForeignKeyName(table.Name, field.Name)
		}
		foreignTable, foreignSchema := splitTableIdentity(fkRef.Table)
		out = append(out, catalog.Constraint{
			Name:           name,
			TableName:      table.Name,
			Schema:         table.Schema,
			Type:           "FOREIGN KEY",
			ColumnName:     field.Name,
			ColumnNames:    []string{field.Name},
			ForeignTable:   new(foreignTable),
			ForeignSchema:  foreignSchema,
			ForeignColumn:  optionalStringPtr(fkRef.Column),
			ForeignColumns: append([]string(nil), fkRef.ReferencedColumns()...),
			DeleteRule:     optionalStringPtr(field.OnDelete),
			UpdateRule:     optionalStringPtr(field.OnUpdate),
			Deferrable:     field.Deferrable,
			Initially:      field.Initially,
			Match:          field.ForeignKeyMatch,
			NotEnforced:    field.ForeignKeyNotEnforced,
		})
	}
	return out
}

func tablePrimaryKeyName(table schemamodel.Table) string {
	if table.Name == "" {
		return "primary"
	}
	return table.Name + "_pkey"
}

func indexTable(structName, explicitTable string, tables map[string]schemamodel.Table) (tableName, schema string) {
	if explicitTable != "" {
		return splitTableIdentity(explicitTable)
	}
	table, ok := tables[structName]
	if !ok {
		return structName, ""
	}
	return table.Name, table.Schema
}

func splitTableIdentity(value string) (name, schema string) {
	ref, ok := tableref.Parse(value)
	if !ok {
		return value, ""
	}
	return ref.Name, ref.Schema
}

func toDBSequences(sequences []schemamodel.Sequence) []catalog.Sequence {
	out := make([]catalog.Sequence, 0, len(sequences))
	for _, sequence := range sequences {
		out = append(out, catalog.Sequence{
			Facets:    sequence.Facets,
			Name:      sequence.Name,
			Schema:    sequence.Schema,
			DataType:  sequence.AsType,
			Start:     clonePtr(sequence.Start),
			Increment: clonePtr(sequence.Increment),
			MinValue:  clonePtr(sequence.MinValue),
			MaxValue:  clonePtr(sequence.MaxValue),
			Cache:     clonePtr(sequence.Cache),
			Cycle:     sequence.Cycle,
			OwnedBy:   sequence.OwnedBy,
			Comment:   sequence.Comment,
		})
	}
	return out
}

func toDBFunctions(functions []schemamodel.Function) []catalog.Function {
	out := make([]catalog.Function, 0, len(functions))
	for _, function := range functions {
		function.Canonicalize()
		name, schema := splitTableIdentity(function.Name)
		out = append(out, catalog.Function{
			Facets:     function.Facets,
			Name:       name,
			Schema:     schema,
			Parameters: function.Parameters,
			Returns:    function.Returns,
			Language:   function.Language,
			Security:   function.Security,
			Volatility: function.Volatility,
			Settings:   function.Settings,
			Body:       function.Body,
			Comment:    function.Comment,
		})
	}
	return out
}

func toDBViews(views []schemamodel.View) []catalog.View {
	out := make([]catalog.View, 0, len(views))
	for _, view := range views {
		name, schema := splitTableIdentity(view.Name)
		checkOption := "NONE"
		if view.WithCheck {
			checkOption = "LOCAL"
		}
		out = append(out, catalog.View{
			Facets:      view.Facets,
			Name:        name,
			Schema:      schema,
			Body:        view.Body,
			CheckOption: checkOption,
			Comment:     view.Comment,
			// The view's own WITH clause, which the comparison compares.
			// Without it, a view declaring SCHEMABINDING compared with its
			// own conversion was replaced on every run.
			Attributes: slices.Clone(view.Attributes),
		})
	}
	return out
}

func toDBMaterializedViews(views []schemamodel.MaterializedView) []catalog.MaterializedView {
	out := make([]catalog.MaterializedView, 0, len(views))
	for _, view := range views {
		name, schema := splitTableIdentity(view.Name)
		out = append(out, catalog.MaterializedView{
			Facets:  view.Facets,
			Name:    name,
			Schema:  schema,
			Body:    view.Body,
			Comment: view.Comment,
		})
	}
	return out
}

func toDBTriggers(triggers []schemamodel.Trigger, tables map[string]schemamodel.Table) []catalog.Trigger {
	out := make([]catalog.Trigger, 0, len(triggers))
	for _, trigger := range triggers {
		trigger.Canonicalize()
		tableName, schema := indexTable(trigger.StructName, trigger.Table, tables)
		out = append(out, catalog.Trigger{
			Facets:  trigger.Facets,
			Name:    trigger.Name,
			Schema:  schema,
			Table:   tableName,
			Timing:  trigger.Timing,
			Event:   trigger.Event,
			ForEach: trigger.ForEach,
			Body:    trigger.Body,
			Comment: trigger.Comment,

			When:     trigger.When,
			OldTable: trigger.OldTable,
			NewTable: trigger.NewTable,
		})
	}
	return out
}

func toDBRLSPolicies(policies []schemamodel.RLSPolicy) []catalog.RLSPolicy {
	out := make([]catalog.RLSPolicy, 0, len(policies))
	for _, policy := range policies {
		out = append(out, catalog.RLSPolicy{
			Name:                policy.Name,
			Table:               policy.Table,
			PolicyFor:           policy.PolicyFor,
			ToRoles:             policy.ToRoles,
			UsingExpression:     policy.UsingExpression,
			WithCheckExpression: policy.WithCheckExpression,
			Restrictive:         policy.Restrictive,
			Comment:             policy.Comment,
		})
	}
	return out
}

func toDBRoles(roles []schemamodel.Role) []catalog.Role {
	out := make([]catalog.Role, 0, len(roles))
	for _, role := range roles {
		passwordState := catalog.RolePasswordAbsent
		if role.Password != "" {
			passwordState = catalog.RolePasswordPresent
		}
		out = append(out, catalog.Role{
			Facets:        role.Facets,
			Name:          role.Name,
			Login:         role.Login,
			Superuser:     role.Superuser,
			CreateDB:      role.CreateDB,
			CreateRole:    role.CreateRole,
			Inherit:       role.Inherit,
			Replication:   role.Replication,
			PasswordState: passwordState,
			Comment:       role.Comment,
			Group:         role.Group,
		})
	}
	return out
}

// toDBRoleMemberships describes each declared membership the way a catalog
// read reports it: one edge from the group to its member, in declaration
// order.
func toDBRoleMemberships(roles []schemamodel.Role) []catalog.RoleMembership {
	var out []catalog.RoleMembership
	for _, role := range roles {
		for _, group := range role.MemberOf {
			if group = strings.TrimSpace(group); group != "" {
				out = append(out, catalog.RoleMembership{Role: group, Member: role.Name})
			}
		}
	}
	return out
}

func toDBGrants(db *schemamodel.Database, dialect string) []catalog.Grant {
	var out []catalog.Grant
	for _, grant := range db.Grants {
		grant.Canonicalize()
		for _, privilege := range grant.Privileges {
			objectType := "TABLE"
			objectName := grant.OnTable
			objectSchema := ""
			arguments := ""
			switch {
			case grant.OnDatabase:
				objectType = "DATABASE"
				objectName = ""
			case grant.OnObject != "":
				objectType = grant.ObjectKind
				objectName, objectSchema = splitTableIdentity(grant.OnObject)
			case grant.OnSchema != "":
				objectType = "SCHEMA"
				objectName = grant.OnSchema
			case grant.OnSequence != "":
				objectType = "SEQUENCE"
				objectName, objectSchema = splitTableIdentity(grant.OnSequence)
			case grant.OnRoutine != "":
				objectType = grant.RoutineKind
				objectName, objectSchema = splitTableIdentity(grant.OnRoutine)
				arguments = grant.RoutineArguments
			default:
				objectName, objectSchema = splitTableIdentity(objectName)
			}
			columns := grant.Columns
			if len(columns) == 0 {
				columns = []string{""}
			}
			for _, column := range columns {
				out = append(out, catalog.Grant{
					Role:       grant.Role,
					Privilege:  privilege,
					ObjectType: objectType,
					Schema:     objectSchema,
					ObjectName: objectName,
					Arguments:  arguments,
					Column:     column,
					WithOption: grant.WithOption,
					GrantedBy:  grant.GrantedBy,
				})
			}
		}
	}
	if platform.IsPostgresFamily(dialect) {
		out = append(out, implicitRoutineGrants(db)...)
	}
	return out
}

// implicitRoutineGrants describes the EXECUTE PUBLIC holds on every routine the
// document declares and does not revoke it from, the way a catalog read reports
// it: one [catalog.Grant] marked Implicit.
//
// PostgreSQL gives PUBLIC that privilege when CREATE FUNCTION or CREATE
// PROCEDURE runs, so a database built from this document holds it without a
// GRANT. Leaving it out would make this side of a file-to-file comparison
// claim the privilege is absent, and a document that revokes it would plan no
// REVOKE against a document that does not.
func implicitRoutineGrants(db *schemamodel.Database) []catalog.Grant {
	var out []catalog.Grant
	for _, function := range db.Functions {
		function.Canonicalize()
		kind := "FUNCTION"
		if function.IsProcedure() {
			kind = "PROCEDURE"
		}
		routine := schemamodel.Grant{OnRoutine: function.Name, RoutineArguments: function.Parameters}
		if publicExecuteDeclared(db, routine) {
			continue
		}
		name, schema := splitTableIdentity(function.Name)
		out = append(out, catalog.Grant{
			Role:       "PUBLIC",
			Privilege:  "EXECUTE",
			ObjectType: kind,
			Schema:     schema,
			ObjectName: name,
			Arguments:  function.Parameters,
			Implicit:   true,
		})
	}
	return out
}

// publicExecuteDeclared reports whether the document grants or revokes
// PUBLIC's EXECUTE on routine itself, either of which replaces the implicit
// privilege: a grant is described by [toDBGrants], and a revoke says it is
// absent.
func publicExecuteDeclared(db *schemamodel.Database, routine schemamodel.Grant) bool {
	matches := func(grant schemamodel.Grant) bool {
		return grant.Role == "PUBLIC" && grant.OnRoutine != "" &&
			sameRoutine(grant, routine) &&
			(slices.Contains(grant.Privileges, "EXECUTE") || slices.Contains(grant.Privileges, "ALL"))
	}
	return slices.ContainsFunc(db.RevokedGrants, matches) || slices.ContainsFunc(db.Grants, matches)
}

// sameRoutine compares two routine targets by name and input argument types,
// the way PostgreSQL resolves the routine a GRANT or REVOKE names.
func sameRoutine(a, b schemamodel.Grant) bool {
	aName, aSchema := splitTableIdentity(a.OnRoutine)
	bName, bSchema := splitTableIdentity(b.OnRoutine)
	return aName == bName && (aSchema == "" || bSchema == "" || aSchema == bSchema) &&
		routineargs.InputTypes(a.RoutineArguments) == routineargs.InputTypes(b.RoutineArguments)
}

func clonePtr[T any](value *T) *T {
	if value == nil {
		return nil
	}
	return new(*value)
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func optionalStringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return new(value)
}
