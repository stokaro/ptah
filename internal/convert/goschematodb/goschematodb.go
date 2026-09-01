// Package goschematodb converts Ptah's desired-schema IR into the DB schema
// shape used by schema comparison. It sits with the other representation
// conversions under internal/convert and serves the comparisons whose current
// side is another desired-schema document rather than a live database reader:
// the file-to-file schema diff and schemadiff.CompareSchemas.
package goschematodb

import (
	"maps"
	"slices"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/schemaprep"
	"go.5x5.cz/ptah/internal/tableref"
)

// ToDBSchema converts Ptah's desired-schema IR into the DB schema shape used by
// schema comparison. It is intended for comparisons where the current side is
// another desired-schema document -- a local file-to-file diff, or two
// in-memory schemas -- and no live database reader is involved.
//
// dialect names the target the converted schema will be compared under. It only
// decides how the one goschema field that carries two concepts is unpacked:
// schemamodel.Index.Type is the PostgreSQL access method on a PostgreSQL-family
// target and the ClickHouse data-skipping-index type on ClickHouse, and the DB
// shape keeps those apart in Method and Type. An empty dialect converts as if
// no target were known and leaves Method unset.
func ToDBSchema(db *schemamodel.Database, dialect string) *catalog.Database {
	if db == nil {
		return &catalog.Database{}
	}

	tableByStruct := make(map[string]schemamodel.Table, len(db.Tables))
	for _, table := range db.Tables {
		tableByStruct[table.StructName] = table
	}

	out := &catalog.Database{
		Schemas:     toDBSchemas(db.Schemas),
		Tables:      toDBTables(db.Tables, db.Fields, db.RLSEnabledTables),
		Enums:       toDBEnums(db.Enums),
		Indexes:     toDBIndexes(db.Indexes, tableByStruct, dialect),
		Constraints: toDBConstraints(db.Tables, db.Fields, db.Constraints, tableByStruct),
		Extensions:  toDBExtensions(db.Extensions),
		Functions:   toDBFunctions(db.Functions),
		Sequences:   toDBSequences(db.Sequences),
		Domains:     toDBDomains(db.Domains),
		Composites:  toDBCompositeTypes(db.CompositeTypes),
		Ranges:      toDBRanges(db.Ranges),
		Views:       toDBViews(db.Views),
		MatViews:    toDBMaterializedViews(db.MaterializedViews),
		Triggers:    toDBTriggers(db.Triggers, tableByStruct),
		RLSPolicies: toDBRLSPolicies(db.RLSPolicies),
		Roles:       toDBRoles(db.Roles),
		Grants:      toDBGrants(db.Grants),
		// A file-to-file comparison uses this side as the current state, and a
		// document that declared its own limits declares them here too
		// (stokaro/ptah#1276).
		NotDescribed: db.NotDescribed,
	}
	applyTablePrimaryKeys(out, db.Tables)
	return out
}

func toDBSchemas(schemas []schemamodel.Schema) []catalog.Schema {
	out := make([]catalog.Schema, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, catalog.Schema{
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
) []catalog.Table {
	fieldsByStruct := make(map[string][]schemamodel.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]catalog.Table, 0, len(tables))
	for _, table := range tables {
		out = append(out, catalog.Table{
			Name:         table.Name,
			Schema:       table.Schema,
			Type:         "TABLE",
			Comment:      table.Comment,
			Columns:      toDBColumns(fieldsByStruct[table.StructName]),
			RLSEnabled:   tableRLSEnabled(table, rlsEnabledTables),
			Strict:       table.Strict,
			WithoutRowID: table.WithoutRowID,
			// Non-empty when the desired state came from a `.sql` file
			// declaring CREATE VIRTUAL TABLE, which is how `ptah db read`
			// output is read back. See stokaro/ptah#1028.
			VirtualModule:    table.VirtualModule,
			VirtualArguments: table.VirtualArguments,
		})
	}
	return out
}

func tableRLSEnabled(table schemamodel.Table, enabledTables []schemamodel.RLSEnabledTable) bool {
	return slices.ContainsFunc(enabledTables, func(enabled schemamodel.RLSEnabledTable) bool {
		return enabled.StructName != "" && enabled.StructName == table.StructName ||
			enabled.Table == table.QualifiedName() ||
			table.Schema == "" && enabled.Table == table.Name
	})
}

func toDBColumns(fields []schemamodel.Field) []catalog.Column {
	out := make([]catalog.Column, 0, len(fields))
	for i, field := range fields {
		out = append(out, toDBColumn(field, i+1))
	}
	return out
}

func toDBColumn(field schemamodel.Field, ordinal int) catalog.Column {
	nullable := "NO"
	if field.Nullable {
		nullable = "YES"
	}
	column := catalog.Column{
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
		Charset:               field.Charset,
		Collate:               field.Collate,
		GeneratedKind:         field.GeneratedKind,
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

func toDBEnums(enums []schemamodel.Enum) []catalog.Enum {
	out := make([]catalog.Enum, 0, len(enums))
	for _, enum := range enums {
		out = append(out, catalog.Enum{Name: enum.Name, Values: append([]string(nil), enum.Values...)})
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
			Name:           index.Name,
			TableName:      tableName,
			Schema:         schema,
			Columns:        append([]string(nil), index.Fields...),
			Parts:          toDBIndexParts(index.Parts, index.Operator),
			IsUnique:       index.Unique,
			Condition:      index.Condition,
			NullsDistinct:  index.NullsDistinct,
			Method:         indexAccessMethod(index.Type, dialect),
			IncludeColumns: append([]string(nil), index.IncludeColumns...),
			StorageParams:  maps.Clone(index.StorageParams),
			Type:           index.Type,
			Granularity:    index.Granularity,
		})
	}
	return out
}

// indexAccessMethod reports schemamodel.Index.Type as an access method only where
// that is what it means. On ClickHouse the same field carries the
// data-skipping-index type, which is a different concept the DB shape keeps in
// Index.Type.
func indexAccessMethod(indexType, dialect string) string {
	if !platform.IsPostgresFamily(dialect) {
		return ""
	}
	return indexType
}

// toDBIndexParts converts structured key parts, resolving the index-level
// operator class the way the renderer applies it: a part without its own class
// inherits the index's, so the DB shape -- which has no index-level slot --
// records the resolved value per part.
func toDBIndexParts(parts []schemamodel.IndexPart, indexOperator string) []catalog.IndexPart {
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
			Desc:       part.Desc,
			NullsOrder: part.NullsOrder,
		}
	}
	return converted
}

func toDBConstraints(
	tablesList []schemamodel.Table,
	fields []schemamodel.Field,
	constraints []schemamodel.Constraint,
	tables map[string]schemamodel.Table,
) []catalog.Constraint {
	fieldsByStruct := make(map[string][]schemamodel.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]catalog.Constraint, 0, len(constraints)+len(tablesList)+len(fields))
	type constraintIdentity struct {
		schema string
		table  string
		name   string
	}
	seen := make(map[constraintIdentity]struct{})
	appendConstraint := func(constraint catalog.Constraint) {
		key := constraintIdentity{
			schema: constraint.Schema,
			table:  constraint.TableName,
			name:   constraint.Name,
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
			Name:        tablePrimaryKeyName(table),
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "PRIMARY KEY",
			ColumnNames: append([]string(nil), table.PrimaryKey...),
			ColumnName:  first(table.PrimaryKey),
		})
	}
	for _, constraint := range constraints {
		tableName, schema := indexTable(constraint.StructName, constraint.Table, tables)
		dbConstraint := catalog.Constraint{
			Name:           constraint.Name,
			TableName:      tableName,
			Schema:         schema,
			Type:           constraint.Type,
			ColumnNames:    append([]string(nil), constraint.Columns...),
			ColumnName:     first(constraint.Columns),
			CheckClause:    optionalStringPtr(constraint.CheckExpression),
			NullsDistinct:  constraint.NullsDistinct,
			IncludeColumns: append([]string(nil), constraint.IncludeColumns...),
			UsingMethod:    optionalStringPtr(constraint.UsingMethod),
			ExcludeElements: optionalStringPtr(
				constraint.ExcludeElements,
			),
			WhereCondition: optionalStringPtr(constraint.WhereCondition),
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
		for _, field := range fieldsByStruct[table.StructName] {
			for _, constraint := range toDBFieldConstraints(table, field) {
				appendConstraint(constraint)
			}
		}
	}
	return out
}

func toDBFieldConstraints(table schemamodel.Table, field schemamodel.Field) []catalog.Constraint {
	var out []catalog.Constraint
	if field.Check != "" {
		name := field.CheckName
		if name == "" {
			name = table.Name + "_" + field.Name + "_check"
		}
		out = append(out, catalog.Constraint{
			Name:        name,
			TableName:   table.Name,
			Schema:      table.Schema,
			Type:        "CHECK",
			ColumnName:  field.Name,
			ColumnNames: []string{field.Name},
			CheckClause: new(field.Check),
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

func toDBExtensions(extensions []schemamodel.Extension) []catalog.Extension {
	out := make([]catalog.Extension, 0, len(extensions))
	for _, extension := range extensions {
		out = append(out, catalog.Extension{
			Name:    extension.Name,
			Schema:  extension.Schema,
			Version: extension.Version,
			Comment: optionalStringPtr(
				extension.Comment,
			),
		})
	}
	return out
}

func toDBSequences(sequences []schemamodel.Sequence) []catalog.Sequence {
	out := make([]catalog.Sequence, 0, len(sequences))
	for _, sequence := range sequences {
		out = append(out, catalog.Sequence{
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

func toDBDomains(domains []schemamodel.Domain) []catalog.Domain {
	out := make([]catalog.Domain, 0, len(domains))
	for _, domain := range domains {
		defaultValue := domain.Default
		if domain.DefaultExpr != "" {
			defaultValue = domain.DefaultExpr
		}
		out = append(out, catalog.Domain{
			Name:     domain.Name,
			Schema:   domain.Schema,
			BaseType: domain.BaseType,
			NotNull:  domain.NotNull,
			Default:  defaultValue,
			Check:    domain.Check,
		})
	}
	return out
}

func toDBCompositeTypes(composites []schemamodel.CompositeType) []catalog.CompositeType {
	out := make([]catalog.CompositeType, 0, len(composites))
	for _, composite := range composites {
		fields := make([]catalog.CompositeField, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			fields = append(fields, catalog.CompositeField{
				Name: field.Name,
				Type: field.Type,
			})
		}
		out = append(out, catalog.CompositeType{
			Name:   composite.Name,
			Schema: composite.Schema,
			Fields: fields,
		})
	}
	return out
}

func toDBRanges(ranges []schemamodel.Range) []catalog.Range {
	out := make([]catalog.Range, 0, len(ranges))
	for _, rangeType := range ranges {
		out = append(out, catalog.Range{
			Name:    rangeType.Name,
			Schema:  rangeType.Schema,
			Subtype: rangeType.Subtype,
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
			Name:        name,
			Schema:      schema,
			Body:        view.Body,
			CheckOption: checkOption,
			Comment:     view.Comment,
		})
	}
	return out
}

func toDBMaterializedViews(views []schemamodel.MaterializedView) []catalog.MaterializedView {
	out := make([]catalog.MaterializedView, 0, len(views))
	for _, view := range views {
		name, schema := splitTableIdentity(view.Name)
		out = append(out, catalog.MaterializedView{
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
			Name:    trigger.Name,
			Schema:  schema,
			Table:   tableName,
			Timing:  trigger.Timing,
			Event:   trigger.Event,
			ForEach: trigger.ForEach,
			Body:    trigger.Body,
			Comment: trigger.Comment,
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
			Comment:             policy.Comment,
		})
	}
	return out
}

func toDBRoles(roles []schemamodel.Role) []catalog.Role {
	out := make([]catalog.Role, 0, len(roles))
	for _, role := range roles {
		out = append(out, catalog.Role{
			Name:        role.Name,
			Login:       role.Login,
			Superuser:   role.Superuser,
			CreateDB:    role.CreateDB,
			CreateRole:  role.CreateRole,
			Inherit:     role.Inherit,
			Replication: role.Replication,
			HasPassword: role.Password != "",
			Comment:     role.Comment,
		})
	}
	return out
}

func toDBGrants(grants []schemamodel.Grant) []catalog.Grant {
	var out []catalog.Grant
	for _, grant := range grants {
		grant.Canonicalize()
		for _, privilege := range grant.Privileges {
			objectType := "TABLE"
			objectName := grant.OnTable
			objectSchema := ""
			switch {
			case grant.OnSchema != "":
				objectType = "SCHEMA"
				objectName = grant.OnSchema
			case grant.OnSequence != "":
				objectType = "SEQUENCE"
				objectName, objectSchema = splitTableIdentity(grant.OnSequence)
			default:
				objectName, objectSchema = splitTableIdentity(objectName)
			}
			out = append(out, catalog.Grant{
				Role:       grant.Role,
				Privilege:  privilege,
				ObjectType: objectType,
				Schema:     objectSchema,
				ObjectName: objectName,
				WithOption: grant.WithOption,
				GrantedBy:  grant.GrantedBy,
			})
		}
	}
	return out
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
