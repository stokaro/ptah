// Package schemafile loads local schema definition files into Ptah's schema IR.
package schemafile

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlashcl"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	"github.com/stokaro/ptah/internal/convert/toschema"
	"github.com/stokaro/ptah/internal/parser"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/internal/yamlschema"
)

// Options configures schema file loading.
type Options struct {
	Dialect string
}

// Load reads one local schema file from either a plain path or file:// URL.
func Load(rawURL string, opts Options) (*goschema.Database, error) {
	path, err := LocalFilePath(rawURL)
	if err != nil {
		return nil, err
	}
	return LoadPath(path, opts)
}

// LoadPath reads one local schema file from a resolved filesystem path.
func LoadPath(path string, opts Options) (*goschema.Database, error) {
	resolved, err := pathguard.ResolveCLIPath(path)
	if err != nil {
		return nil, fmt.Errorf("resolve schema file path: %w", err)
	}

	info, err := os.Stat(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("schema file does not exist: %s", resolved)
		}
		return nil, fmt.Errorf("stat schema file: %w", err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("schema file is a directory: %s", resolved)
	}

	switch strings.ToLower(filepath.Ext(resolved)) {
	case ".hcl":
		return atlashcl.ParseFile(resolved)
	case ".yaml", ".yml":
		return yamlschema.ParseFile(resolved)
	case ".sql":
		return loadSQLFile(resolved, opts)
	default:
		return nil, fmt.Errorf("unsupported schema file extension %q: only .yaml, .yml, .hcl, and .sql are supported", filepath.Ext(resolved))
	}
}

// LoadAll reads all schema files and merges them into one database IR.
func LoadAll(rawURLs []string, opts Options) (*goschema.Database, error) {
	if len(rawURLs) == 0 {
		return nil, fmt.Errorf("at least one schema file URL is required")
	}

	merged := &goschema.Database{}
	for _, rawURL := range rawURLs {
		db, err := Load(rawURL, opts)
		if err != nil {
			return nil, err
		}
		appendDatabase(merged, db)
	}
	goschema.Finalize(merged)
	return merged, nil
}

// ToDBSchema converts Ptah's desired-schema IR into the DB schema shape used by
// schema comparison. It is intended for local file-to-file comparisons where no
// live database reader is involved.
func ToDBSchema(db *goschema.Database) *dbschematypes.DBSchema {
	if db == nil {
		return &dbschematypes.DBSchema{}
	}

	tableByStruct := make(map[string]goschema.Table, len(db.Tables))
	for _, table := range db.Tables {
		tableByStruct[table.StructName] = table
	}

	out := &dbschematypes.DBSchema{
		Tables:      toDBTables(db.Tables, db.Fields),
		Enums:       toDBEnums(db.Enums),
		Indexes:     toDBIndexes(db.Indexes, tableByStruct),
		Constraints: toDBConstraints(db.Tables, db.Fields, db.Constraints, tableByStruct),
		Extensions:  toDBExtensions(db.Extensions),
		Functions:   toDBFunctions(db.Functions),
		Views:       toDBViews(db.Views),
		MatViews:    toDBMaterializedViews(db.MaterializedViews),
		Triggers:    toDBTriggers(db.Triggers, tableByStruct),
		RLSPolicies: toDBRLSPolicies(db.RLSPolicies),
		Roles:       toDBRoles(db.Roles),
		Grants:      toDBGrants(db.Grants),
	}
	applyTablePrimaryKeys(out, db.Tables)
	return out
}

// LocalFilePath converts a local schema source URL into a filesystem path.
func LocalFilePath(rawURL string) (string, error) {
	base, rawQuery, _ := strings.Cut(strings.TrimSpace(rawURL), "?")
	if base == "" {
		return "", fmt.Errorf("schema file URL is required")
	}
	if rawQuery != "" {
		if _, err := url.ParseQuery(rawQuery); err != nil {
			return "", fmt.Errorf("parse schema file URL query: %w", err)
		}
		return "", fmt.Errorf("schema file URL query parameters are not supported yet")
	}
	if strings.Contains(base, "://") && !strings.HasPrefix(base, "file://") {
		return "", fmt.Errorf("only local file:// schema files are supported")
	}

	path := strings.TrimPrefix(base, "file://")
	if path == "" {
		path = "."
	}
	path, err := url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("decode schema file URL path: %w", err)
	}
	return filepath.Clean(path), nil
}

func loadSQLFile(path string, opts Options) (*goschema.Database, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read SQL schema file: %w", err)
	}

	statements, err := parser.NewParser(string(data), parser.WithDialect(opts.Dialect)).Parse()
	if err != nil {
		return nil, fmt.Errorf("parse SQL schema file: %w", err)
	}
	db := toschema.ToDatabase(statements)
	goschema.Finalize(&db)
	return &db, nil
}

func appendDatabase(dst, src *goschema.Database) {
	dst.Schemas = append(dst.Schemas, src.Schemas...)
	dst.Tables = append(dst.Tables, src.Tables...)
	dst.Fields = append(dst.Fields, src.Fields...)
	dst.Indexes = append(dst.Indexes, src.Indexes...)
	dst.Constraints = append(dst.Constraints, src.Constraints...)
	dst.Enums = append(dst.Enums, src.Enums...)
	dst.EmbeddedFields = append(dst.EmbeddedFields, src.EmbeddedFields...)
	dst.Extensions = append(dst.Extensions, src.Extensions...)
	dst.Functions = append(dst.Functions, src.Functions...)
	dst.Views = append(dst.Views, src.Views...)
	dst.MaterializedViews = append(dst.MaterializedViews, src.MaterializedViews...)
	dst.Triggers = append(dst.Triggers, src.Triggers...)
	dst.RLSPolicies = append(dst.RLSPolicies, src.RLSPolicies...)
	dst.RLSEnabledTables = append(dst.RLSEnabledTables, src.RLSEnabledTables...)
	dst.Roles = append(dst.Roles, src.Roles...)
	dst.Grants = append(dst.Grants, src.Grants...)
}

func toDBTables(tables []goschema.Table, fields []goschema.Field) []dbschematypes.DBTable {
	fieldsByStruct := make(map[string][]goschema.Field)
	for _, field := range fields {
		fieldsByStruct[field.StructName] = append(fieldsByStruct[field.StructName], field)
	}

	out := make([]dbschematypes.DBTable, 0, len(tables))
	for _, table := range tables {
		out = append(out, dbschematypes.DBTable{
			Name:    table.Name,
			Schema:  table.Schema,
			Type:    "TABLE",
			Comment: table.Comment,
			Columns: toDBColumns(fieldsByStruct[table.StructName]),
		})
	}
	return out
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
		primaryByTable[table.Name] = columns
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

func toDBIndexes(indexes []goschema.Index, tables map[string]goschema.Table) []dbschematypes.DBIndex {
	out := make([]dbschematypes.DBIndex, 0, len(indexes))
	for _, index := range indexes {
		tableName, schema := indexTable(index.StructName, index.TableName, tables)
		out = append(out, dbschematypes.DBIndex{
			Name:          index.Name,
			TableName:     tableName,
			Schema:        schema,
			Columns:       append([]string(nil), index.Fields...),
			IsUnique:      index.Unique,
			Condition:     index.Condition,
			NullsDistinct: index.NullsDistinct,
			Type:          index.Type,
			Granularity:   index.Granularity,
		})
	}
	return out
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
	seen := make(map[string]struct{})
	appendConstraint := func(constraint dbschematypes.DBConstraint) {
		key := constraint.QualifiedTableName() + "." + constraint.Name
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
			dbConstraint.ForeignTable = new(constraint.ForeignTable)
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
		out = append(out, dbschematypes.DBConstraint{
			Name:           name,
			TableName:      table.Name,
			Schema:         table.Schema,
			Type:           "FOREIGN KEY",
			ColumnName:     field.Name,
			ColumnNames:    []string{field.Name},
			ForeignTable:   new(fkRef.Table),
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

func indexTable(structName, explicitTable string, tables map[string]goschema.Table) (tableName string, schema string) {
	if explicitTable != "" {
		return explicitTable, ""
	}
	table, ok := tables[structName]
	if !ok {
		return structName, ""
	}
	return table.Name, table.Schema
}

func toDBExtensions(extensions []goschema.Extension) []dbschematypes.DBExtension {
	out := make([]dbschematypes.DBExtension, 0, len(extensions))
	for _, extension := range extensions {
		out = append(out, dbschematypes.DBExtension{
			Name:    extension.Name,
			Version: extension.Version,
			Comment: optionalStringPtr(
				extension.Comment,
			),
		})
	}
	return out
}

func toDBFunctions(functions []goschema.Function) []dbschematypes.DBFunction {
	out := make([]dbschematypes.DBFunction, 0, len(functions))
	for _, function := range functions {
		function.Canonicalize()
		out = append(out, dbschematypes.DBFunction{
			Name:       function.Name,
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
		checkOption := "NONE"
		if view.WithCheck {
			checkOption = "LOCAL"
		}
		out = append(out, dbschematypes.DBView{
			Name:        view.Name,
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
		view.Canonicalize()
		out = append(out, dbschematypes.DBMatView{
			Name:            view.Name,
			Body:            view.Body,
			RefreshStrategy: view.RefreshStrategy,
			Comment:         view.Comment,
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
			if grant.OnSchema != "" {
				objectType = "SCHEMA"
				objectName = grant.OnSchema
			}
			out = append(out, dbschematypes.DBGrant{
				Role:       grant.Role,
				Privilege:  privilege,
				ObjectType: objectType,
				ObjectName: objectName,
				WithOption: grant.WithOption,
				GrantedBy:  grant.GrantedBy,
			})
		}
	}
	return out
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
