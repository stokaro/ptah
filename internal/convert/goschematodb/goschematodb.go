// Package goschematodb converts Ptah's desired-schema IR into the DB schema
// shape used by schema comparison. It sits with the other representation
// conversions under internal/convert and serves the comparisons whose current
// side is another desired-schema document rather than a live database reader:
// the file-to-file schema diff and schemadiff.CompareSchemas.
package goschematodb

import (
	"maps"
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/tableref"
)

// ToDBSchema converts Ptah's desired-schema IR into the DB schema shape used by
// schema comparison. It is intended for comparisons where the current side is
// another desired-schema document -- a local file-to-file diff, or two
// in-memory schemas -- and no live database reader is involved.
//
// dialect names the target the converted schema will be compared under. It only
// decides how the one goschema field that carries two concepts is unpacked:
// goschema.Index.Type is the PostgreSQL access method on a PostgreSQL-family
// target and the ClickHouse data-skipping-index type on ClickHouse, and the DB
// shape keeps those apart in Method and Type. An empty dialect converts as if
// no target were known and leaves Method unset.
func ToDBSchema(db *goschema.Database, dialect string) *dbschematypes.DBSchema {
	if db == nil {
		return &dbschematypes.DBSchema{}
	}

	tableByStruct := make(map[string]goschema.Table, len(db.Tables))
	for _, table := range db.Tables {
		tableByStruct[table.StructName] = table
	}

	out := &dbschematypes.DBSchema{
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

func toDBSchemas(schemas []goschema.Schema) []dbschematypes.DBSchemaInfo {
	out := make([]dbschematypes.DBSchemaInfo, 0, len(schemas))
	for _, schema := range schemas {
		out = append(out, dbschematypes.DBSchemaInfo{
			Name:    schema.Name,
			Comment: schema.Comment,
			Charset: schema.Charset,
			Collate: schema.Collate,
		})
	}
	return out
}

func toDBTables(
	tables []goschema.Table,
	fields []goschema.Field,
	rlsEnabledTables []goschema.RLSEnabledTable,
) []dbschematypes.DBTable {
	fieldsByStruct := make(map[string][]goschema.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]dbschematypes.DBTable, 0, len(tables))
	for _, table := range tables {
		out = append(out, dbschematypes.DBTable{
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

func tableRLSEnabled(table goschema.Table, enabledTables []goschema.RLSEnabledTable) bool {
	return slices.ContainsFunc(enabledTables, func(enabled goschema.RLSEnabledTable) bool {
		return enabled.StructName != "" && enabled.StructName == table.StructName ||
			enabled.Table == table.QualifiedName() ||
			table.Schema == "" && enabled.Table == table.Name
	})
}

func toDBColumns(fields []goschema.Field) []dbschematypes.DBColumn {
	out := make([]dbschematypes.DBColumn, 0, len(fields))
	for i, field := range fields {
		out = append(out, toDBColumn(field, i+1))
	}
	return out
}

func toDBColumn(field goschema.Field, ordinal int) dbschematypes.DBColumn {
	nullable := "NO"
	if field.Nullable {
		nullable = "YES"
	}
	column := dbschematypes.DBColumn{
		Name:            field.Name,
		DataType:        field.Type,
		ColumnType:      field.Type,
		IsNullable:      nullable,
		OrdinalPosition: ordinal,
		IsAutoIncrement: field.AutoInc || field.IdentityGeneration != "",
		IsPrimaryKey:    field.Primary,
		IsUnique:        field.Unique,
		Charset:         field.Charset,
		Collate:         field.Collate,
		GeneratedKind:   field.GeneratedKind,
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

func applyTablePrimaryKeys(schema *dbschematypes.DBSchema, tables []goschema.Table) {
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

func toDBEnums(enums []goschema.Enum) []dbschematypes.DBEnum {
	out := make([]dbschematypes.DBEnum, 0, len(enums))
	for _, enum := range enums {
		out = append(out, dbschematypes.DBEnum{Name: enum.Name, Values: append([]string(nil), enum.Values...)})
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
	indexes []goschema.Index,
	tables map[string]goschema.Table,
	dialect string,
) []dbschematypes.DBIndex {
	out := make([]dbschematypes.DBIndex, 0, len(indexes))
	for _, index := range indexes {
		tableName, schema := indexTable(index.StructName, index.TableName, tables)
		out = append(out, dbschematypes.DBIndex{
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

// indexAccessMethod reports goschema.Index.Type as an access method only where
// that is what it means. On ClickHouse the same field carries the
// data-skipping-index type, which is a different concept the DB shape keeps in
// DBIndex.Type.
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
func toDBIndexParts(parts []goschema.IndexPart, indexOperator string) []dbschematypes.DBIndexPart {
	if len(parts) == 0 {
		return nil
	}
	converted := make([]dbschematypes.DBIndexPart, len(parts))
	for position, part := range parts {
		operator := part.Operator
		if operator == "" {
			operator = indexOperator
		}
		converted[position] = dbschematypes.DBIndexPart{
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
	tablesList []goschema.Table,
	fields []goschema.Field,
	constraints []goschema.Constraint,
	tables map[string]goschema.Table,
) []dbschematypes.DBConstraint {
	fieldsByStruct := make(map[string][]goschema.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]dbschematypes.DBConstraint, 0, len(constraints)+len(tablesList)+len(fields))
	type constraintIdentity struct {
		schema string
		table  string
		name   string
	}
	seen := make(map[constraintIdentity]struct{})
	appendConstraint := func(constraint dbschematypes.DBConstraint) {
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
		appendConstraint(dbschematypes.DBConstraint{
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
		dbConstraint := dbschematypes.DBConstraint{
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

func toDBFieldConstraints(table goschema.Table, field goschema.Field) []dbschematypes.DBConstraint {
	var out []dbschematypes.DBConstraint
	if field.Check != "" {
		name := field.CheckName
		if name == "" {
			name = table.Name + "_" + field.Name + "_check"
		}
		out = append(out, dbschematypes.DBConstraint{
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
		fkRef := fromschema.ParseForeignKeyReference(field.Foreign)
		if fkRef == nil {
			return out
		}
		name := field.ForeignKeyName
		if name == "" {
			name = fromschema.GenerateForeignKeyName(table.Name, field.Name)
		}
		foreignTable, foreignSchema := splitTableIdentity(fkRef.Table)
		out = append(out, dbschematypes.DBConstraint{
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

func tablePrimaryKeyName(table goschema.Table) string {
	if table.Name == "" {
		return "primary"
	}
	return table.Name + "_pkey"
}

func indexTable(structName, explicitTable string, tables map[string]goschema.Table) (tableName, schema string) {
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

func toDBExtensions(extensions []goschema.Extension) []dbschematypes.DBExtension {
	out := make([]dbschematypes.DBExtension, 0, len(extensions))
	for _, extension := range extensions {
		out = append(out, dbschematypes.DBExtension{
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

func toDBSequences(sequences []goschema.Sequence) []dbschematypes.DBSequence {
	out := make([]dbschematypes.DBSequence, 0, len(sequences))
	for _, sequence := range sequences {
		out = append(out, dbschematypes.DBSequence{
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

func toDBDomains(domains []goschema.Domain) []dbschematypes.DBDomain {
	out := make([]dbschematypes.DBDomain, 0, len(domains))
	for _, domain := range domains {
		defaultValue := domain.Default
		if domain.DefaultExpr != "" {
			defaultValue = domain.DefaultExpr
		}
		out = append(out, dbschematypes.DBDomain{
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

func toDBCompositeTypes(composites []goschema.CompositeType) []dbschematypes.DBComposite {
	out := make([]dbschematypes.DBComposite, 0, len(composites))
	for _, composite := range composites {
		fields := make([]dbschematypes.DBCompositeField, 0, len(composite.Fields))
		for _, field := range composite.Fields {
			fields = append(fields, dbschematypes.DBCompositeField{
				Name: field.Name,
				Type: field.Type,
			})
		}
		out = append(out, dbschematypes.DBComposite{
			Name:   composite.Name,
			Schema: composite.Schema,
			Fields: fields,
		})
	}
	return out
}

func toDBRanges(ranges []goschema.Range) []dbschematypes.DBRange {
	out := make([]dbschematypes.DBRange, 0, len(ranges))
	for _, rangeType := range ranges {
		out = append(out, dbschematypes.DBRange{
			Name:    rangeType.Name,
			Schema:  rangeType.Schema,
			Subtype: rangeType.Subtype,
		})
	}
	return out
}

func toDBFunctions(functions []goschema.Function) []dbschematypes.DBFunction {
	out := make([]dbschematypes.DBFunction, 0, len(functions))
	for _, function := range functions {
		function.Canonicalize()
		name, schema := splitTableIdentity(function.Name)
		out = append(out, dbschematypes.DBFunction{
			Name:       name,
			Schema:     schema,
			Parameters: function.Parameters,
			Returns:    function.Returns,
			Language:   function.Language,
			Security:   function.Security,
			Volatility: function.Volatility,
			Body:       function.Body,
			Comment:    function.Comment,
		})
	}
	return out
}

func toDBViews(views []goschema.View) []dbschematypes.DBView {
	out := make([]dbschematypes.DBView, 0, len(views))
	for _, view := range views {
		name, schema := splitTableIdentity(view.Name)
		checkOption := "NONE"
		if view.WithCheck {
			checkOption = "LOCAL"
		}
		out = append(out, dbschematypes.DBView{
			Name:        name,
			Schema:      schema,
			Body:        view.Body,
			CheckOption: checkOption,
			Comment:     view.Comment,
		})
	}
	return out
}

func toDBMaterializedViews(views []goschema.MaterializedView) []dbschematypes.DBMatView {
	out := make([]dbschematypes.DBMatView, 0, len(views))
	for _, view := range views {
		name, schema := splitTableIdentity(view.Name)
		out = append(out, dbschematypes.DBMatView{
			Name:    name,
			Schema:  schema,
			Body:    view.Body,
			Comment: view.Comment,
		})
	}
	return out
}

func toDBTriggers(triggers []goschema.Trigger, tables map[string]goschema.Table) []dbschematypes.DBTrigger {
	out := make([]dbschematypes.DBTrigger, 0, len(triggers))
	for _, trigger := range triggers {
		trigger.Canonicalize()
		tableName, schema := indexTable(trigger.StructName, trigger.Table, tables)
		out = append(out, dbschematypes.DBTrigger{
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

func toDBRLSPolicies(policies []goschema.RLSPolicy) []dbschematypes.DBRLSPolicy {
	out := make([]dbschematypes.DBRLSPolicy, 0, len(policies))
	for _, policy := range policies {
		out = append(out, dbschematypes.DBRLSPolicy{
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

func toDBRoles(roles []goschema.Role) []dbschematypes.DBRole {
	out := make([]dbschematypes.DBRole, 0, len(roles))
	for _, role := range roles {
		out = append(out, dbschematypes.DBRole{
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

func toDBGrants(grants []goschema.Grant) []dbschematypes.DBGrant {
	var out []dbschematypes.DBGrant
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
			out = append(out, dbschematypes.DBGrant{
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
