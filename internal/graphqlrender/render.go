// Package graphqlrender renders a Ptah schema as GraphQL SDL: one object type per
// table, plus an input type, a Relay-style connection/edge pair, enum types for
// enum columns and object relations for foreign keys. The output parses with a
// standard GraphQL parser and is a usable starting point for a schema.
package graphqlrender

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/schemaexport"
)

// Options controls the GraphQL export.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
}

// Result is the rendered GraphQL SDL plus any lossy-export diagnostics.
type Result struct {
	Data        []byte
	Diagnostics []schemaexport.Diagnostic
}

// Render renders db as deterministic GraphQL SDL.
func Render(db *goschema.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}

	tables := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: opts.IncludeTables,
		ExcludeTables: opts.ExcludeTables,
	})
	enums := schemaexport.EnumIndex(db)

	reg := newNameRegistry()
	// Reserve built-in and structural names so no generated type can shadow them.
	for _, reserved := range []string{
		scalarInt, scalarFloat, scalarString, scalarBoolean, scalarID, scalarDateTime, scalarJSON,
		pageInfoType, queryType, "Mutation", "Subscription",
	} {
		reg.reserve(reserved)
	}

	// Pass 1: assign a unique object-type name per table, so relations can
	// reference targets defined later in the file.
	typeNames := make(map[string]string, len(tables))
	for _, table := range tables {
		typeNames[table.Name] = reg.unique(schemaexport.SanitizeGraphQLName(schemaexport.TypeName(table.Name)))
	}

	b := &builder{
		reg:             reg,
		enums:           enums,
		typeNames:       typeNames,
		enumNameByKey:   map[string]string{},
		customScalars:   map[string]bool{},
		usedQueryFields: map[string]bool{},
	}
	for _, table := range tables {
		b.addTable(db, table)
	}
	if len(b.objectTypes) == 0 {
		b.warn("schema", "no exportable tables; emitted a placeholder Query type")
	}

	return Result{Data: []byte(b.render()), Diagnostics: b.diagnostics}, nil
}

// builder accumulates the SDL model across tables before serialization.
type builder struct {
	reg           *nameRegistry
	enums         map[string][]string
	typeNames     map[string]string
	enumNameByKey map[string]string
	customScalars map[string]bool

	objectTypes     []gqlType
	inputTypes      []gqlType
	edgeTypes       []gqlType
	connTypes       []gqlType
	enumTypes       []gqlEnum
	queryFields     []gqlField
	usedQueryFields map[string]bool
	diagnostics     []schemaexport.Diagnostic
}

func (b *builder) addTable(db *goschema.Database, table goschema.Table) {
	fields := schemaexport.FieldsFor(db, table)
	pk := toSet(schemaexport.EffectivePrimaryKey(table, fields))
	typeName := b.typeNames[table.Name]

	object := gqlType{name: typeName, desc: table.Comment}
	input := gqlType{name: b.reg.unique(typeName + "Input")}
	usedFieldNames := map[string]bool{}

	for _, field := range fields {
		// Column names are arbitrary annotation strings; a GraphQL field name
		// must be a legal identifier or the schema fails to build.
		name := schemaexport.SanitizeGraphQLName(field.Name)
		if name != field.Name {
			b.warn("type "+typeName+"."+field.Name, "column name is not a valid GraphQL name; exported as "+name)
		}
		if usedFieldNames[name] {
			b.warn("type "+typeName+"."+field.Name, "field name "+name+" collides with another column; omitted")
			continue
		}
		usedFieldNames[name] = true

		objectField := b.columnField(table, field, pk, name)
		object.fields = append(object.fields, objectField)

		// A server-generated column (serial / auto-increment) is not a create
		// input; everything else is.
		if !isServerGenerated(field) {
			inputField := objectField
			inputField.desc = ""
			input.fields = append(input.fields, inputField)
		}
	}

	// Foreign keys become object relations alongside the scalar id column.
	for _, field := range fields {
		if strings.TrimSpace(field.Foreign) == "" {
			continue
		}
		if _, ok := schemaexport.ParseForeignRef(field.Foreign); !ok {
			continue
		}
		relName, ok := schemaexport.RelationFieldName(field.Name)
		if !ok {
			continue
		}
		relName = schemaexport.SanitizeGraphQLName(relName)
		if usedFieldNames[relName] {
			continue // collides with a column or another relation
		}
		ref, _ := schemaexport.ParseForeignRef(field.Foreign)
		targetType, ok := b.typeNames[ref.Table]
		if !ok {
			b.warn("type "+typeName+"."+field.Name,
				fmt.Sprintf("foreign key references table %q which is not exported; relation field omitted", ref.Table))
			continue
		}
		object.fields = append(object.fields, gqlField{
			name: relName, typ: targetType, nonNull: !field.Nullable,
		})
		usedFieldNames[relName] = true
	}

	// A type with no fields is a GraphQL syntax error; skip the whole table.
	if len(object.fields) == 0 {
		b.warn("type "+typeName, "table has no exportable columns; type omitted")
		return
	}

	b.objectTypes = append(b.objectTypes, object)
	if len(input.fields) > 0 {
		b.inputTypes = append(b.inputTypes, input)
	}

	edgeName := b.reg.unique(typeName + "Edge")
	b.edgeTypes = append(b.edgeTypes, gqlType{
		name: edgeName,
		fields: []gqlField{
			{name: "node", typ: typeName, nonNull: true},
			{name: "cursor", typ: scalarString, nonNull: true},
		},
	})
	connName := b.reg.unique(typeName + "Connection")
	b.connTypes = append(b.connTypes, gqlType{
		name: connName,
		fields: []gqlField{
			{name: "edges", typ: edgeName, nonNull: true, list: true, listNonNull: true},
			{name: "pageInfo", typ: pageInfoType, nonNull: true},
		},
	})

	// Query root fields: a paginated connection for the table, and a by-id
	// lookup when the table has a single-column primary key.
	b.addQueryField(gqlField{
		name: lowerFirst(schemaexport.SanitizeGraphQLName(schemaexport.PascalCase(table.Name))),
		args: "(first: Int, after: String)",
		typ:  connName,
	})
	if pk := schemaexport.EffectivePrimaryKey(table, fields); len(pk) == 1 {
		b.addQueryField(gqlField{
			name: lowerFirst(typeName),
			args: "(" + schemaexport.SanitizeGraphQLName(pk[0]) + ": ID!)",
			typ:  typeName,
		})
	}
}

// columnField builds the GraphQL field for a column, mapping an array column to a
// list of the element type.
func (b *builder) columnField(table goschema.Table, field goschema.Field, pk map[string]bool, name string) gqlField {
	elementField := field
	list := false
	if element, isArray := schemaexport.ElementType(field.Type); isArray {
		elementField.Type = element
		list = true
	}
	gt := b.resolveColumnType(table, elementField, pk)
	nonNull := !field.Nullable || pk[field.Name]
	return gqlField{
		name:        name,
		typ:         gt,
		nonNull:     nonNull,
		list:        list,
		listNonNull: false, // SQL arrays may contain null elements
		desc:        field.Comment,
	}
}

func (b *builder) addQueryField(field gqlField) {
	if b.usedQueryFields[field.name] {
		b.warn("type Query."+field.name, "duplicate query field name; omitted")
		return
	}
	b.usedQueryFields[field.name] = true
	b.queryFields = append(b.queryFields, field)
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	runes := []rune(s)
	return strings.ToLower(string(runes[0])) + string(runes[1:])
}

// resolveColumnType returns the GraphQL type name for a column, resolving enums,
// applying the ID convention to primary keys, and recording custom scalars and
// diagnostics as a side effect.
func (b *builder) resolveColumnType(table goschema.Table, field goschema.Field, pk map[string]bool) string {
	if values, ok := schemaexport.ResolveEnumValues(field, b.enums); ok {
		if name, ok := b.enumType(table, field, values); ok {
			return name
		}
		// Values are not valid GraphQL enum names; fall back to a scalar.
		b.warn("type "+b.typeNames[table.Name]+"."+field.Name,
			"enum values are not valid GraphQL enum names; emitted as String")
		return scalarString
	}

	scalar := mapGraphQLScalar(field.Type)
	if scalar.Custom != "" {
		b.customScalars[scalar.Custom] = true
	}
	if !scalar.Known {
		b.warn("type "+b.typeNames[table.Name]+"."+field.Name,
			fmt.Sprintf("unknown column type %q mapped to String", field.Type))
	}
	if pk[field.Name] && scalar.Known {
		return scalarID
	}
	return scalar.Name
}

// enumType returns the GraphQL enum type name for a field, defining the enum once
// and deduplicating fields that share the same source enum. The second result is
// false when the values are not valid GraphQL enum names.
func (b *builder) enumType(table goschema.Table, field goschema.Field, values []string) (string, bool) {
	for _, value := range values {
		if !schemaexport.IsValidGraphQLName(value) {
			return "", false
		}
	}
	key := b.enumSourceKey(table, field)
	if name, ok := b.enumNameByKey[key]; ok {
		return name, true
	}
	name := b.reg.unique(b.desiredEnumName(table, field))
	b.enumNameByKey[key] = name
	b.enumTypes = append(b.enumTypes, gqlEnum{name: name, values: values})
	return name, true
}

func (b *builder) enumSourceKey(table goschema.Table, field goschema.Field) string {
	if !mapGraphQLScalar(field.Type).Known {
		return "type:" + field.Type // named enum type shared across columns
	}
	return "col:" + table.Name + "." + field.Name // inline enum, unique per column
}

func (b *builder) desiredEnumName(table goschema.Table, field goschema.Field) string {
	var raw string
	if !mapGraphQLScalar(field.Type).Known {
		raw = schemaexport.PascalCase(strings.TrimPrefix(field.Type, "enum_"))
	} else {
		raw = schemaexport.TypeName(table.Name) + schemaexport.PascalCase(field.Name)
	}
	return schemaexport.SanitizeGraphQLName(raw)
}

func (b *builder) warn(path, message string) {
	b.diagnostics = append(b.diagnostics, schemaexport.Diagnostic{
		Severity: schemaexport.SeverityWarning,
		Path:     path,
		Message:  message,
	})
}

// isServerGenerated reports whether a column's value is produced by the database
// (serial or auto-increment), and so does not belong in a create input.
func isServerGenerated(field goschema.Field) bool {
	if field.AutoInc {
		return true
	}
	base, _ := schemaexport.NormalizeType(field.Type)
	return strings.Contains(base, "SERIAL")
}

func toSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

// sortedKeys returns the keys of a set sorted, for deterministic emission.
func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
