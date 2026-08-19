// Package graphqlrender renders a Ptah schema as GraphQL SDL: one object type
// per table, enum types for enum columns and object relations for foreign keys.
// Operation shapes — inputs, Relay-style connections, and Query fields — are
// emitted only when Options.Operations names them, because Ptah generates no
// resolver, authorization, or data access to stand behind them. The output
// parses with a standard GraphQL parser.
package graphqlrender

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

// Options controls the GraphQL export.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
	// FieldPolicy decides what an undeclared column means. The zero value is
	// the historical behavior: every column of an exported table is exported.
	FieldPolicy schemaexport.FieldPolicy
	// Operations selects the operation shapes to emit. Its zero value emits
	// data types only.
	Operations Operations
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
	// Refused before anything is built: an alias that shadows another column
	// would drop it from the schema, and the reader of that schema has nothing
	// left to notice the loss with. This is the DECLARED collision; a collision
	// that only appears after GraphQL name sanitization is a naming-rules
	// artifact and stays a warning below.
	if err := schemaexport.ValidateTableAPINames(tables, schemaexport.TargetGraphQL); err != nil {
		return Result{}, err
	}
	enums := schemaexport.EnumIndex(db)
	policy := opts.FieldPolicy
	if policy == "" {
		policy = schemaexport.FieldPolicyAll
	}
	for _, table := range tables {
		// The validation pre-pass reads the EXPORTED set, so two columns
		// colliding on api_name where the policy publishes only one of them is
		// not a collision. It has to agree with the build pass below or a
		// refusal describes a schema nobody asked for.
		fields, _, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeRead, policy)
		if err != nil {
			return Result{}, err
		}
		if err := schemaexport.ValidateFieldAPINames(table, fields, schemaexport.TargetGraphQL); err != nil {
			return Result{}, err
		}
		// An explicit type override the scalar mapping cannot honor is refused
		// rather than defaulted to String. Defaulting is right for an
		// unrecognized COLUMN type, which is a fact about the schema; here it
		// would answer a request nobody made and hide that the declaration did
		// nothing.
		for _, field := range fields {
			if err := schemaexport.RefuseUnknownAPIType(
				table, field, schemaexport.TargetGraphQL, enums,
				func(t string) bool { return mapGraphQLScalar(t).Known },
			); err != nil {
				return Result{}, err
			}
		}
	}

	reg := newNameRegistry()
	// Reserve built-in and structural names so no generated type can shadow
	// them. They are reserved whatever the operation selection is, so a table
	// named "page_info" gets the same object-type name in every profile.
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
		typeNames[table.Name] = reg.unique(schemaexport.SanitizeGraphQLName(schemaexport.TypeName(schemaexport.TableAPIName(table, schemaexport.TargetGraphQL))))
	}

	b := &builder{
		reg:             reg,
		ops:             opts.Operations,
		enums:           enums,
		typeNames:       typeNames,
		enumNameByKey:   make(map[string]string),
		customScalars:   make(map[string]bool),
		usedQueryFields: make(map[string]bool),
	}
	for _, table := range tables {
		if err := b.addTable(db, table, policy); err != nil {
			return Result{}, err
		}
	}
	if len(b.objectTypes) == 0 {
		b.warn("schema", "no exportable tables; nothing was emitted for them")
	}

	return Result{Data: []byte(b.render()), Diagnostics: b.diagnostics}, nil
}

// builder accumulates the SDL model across tables before serialization.
type builder struct {
	reg           *nameRegistry
	ops           Operations
	enums         map[string][]string
	typeNames     map[string]string
	enumNameByKey map[string]string
	customScalars map[string]bool

	objectTypes     []gqlType
	createInputs    []gqlType
	updateInputs    []gqlType
	edgeTypes       []gqlType
	connTypes       []gqlType
	enumTypes       []gqlEnum
	queryFields     []gqlField
	usedQueryFields map[string]bool
	diagnostics     []schemaexport.Diagnostic
}

// column pairs an emitted object field with the source column it came from, so
// the input projections reuse the exact name the object type published rather
// than re-deriving it and drifting from a deduplicated one.
type column struct {
	source goschema.Field
	field  gqlField
}

// unionShapes returns every column that reaches either contract, and which of
// the two each one reaches.
//
// The order is the read shape's, with write-only columns appended, so a schema
// does not reshuffle when an exposure changes.
func unionShapes(read, write []goschema.Field) (all []goschema.Field, inObject, inInput map[string]bool) {
	inObject = make(map[string]bool, len(read))
	for _, field := range read {
		inObject[field.Name] = true
	}
	inInput = make(map[string]bool, len(write))
	for _, field := range write {
		inInput[field.Name] = true
	}
	all = append([]goschema.Field(nil), read...)
	for _, field := range write {
		if !inObject[field.Name] {
			all = append(all, field)
		}
	}
	return all, inObject, inInput
}

func (b *builder) addTable(db *goschema.Database, table goschema.Table, policy schemaexport.FieldPolicy) error {
	// The object type is the read shape and the input types are the write one,
	// which is the split GraphQL already had a word for. A column exposed for
	// write alone still needs a field built for it, so the build walks the
	// union and the object keeps only what the read shape published.
	readable, diagnostics, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeRead, policy)
	if err != nil {
		return err
	}
	writable, _, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeWrite, policy)
	if err != nil {
		return err
	}
	b.diagnostics = append(b.diagnostics, diagnostics...)
	fields, inObject, inInput := unionShapes(readable, writable)
	pk := toSet(schemaexport.EffectivePrimaryKey(table, fields))
	typeName := b.typeNames[table.Name]

	object := gqlType{name: typeName, desc: table.Comment}
	usedFieldNames := make(map[string]bool)
	var columns []column

	for _, field := range fields {
		// The API type is substituted once here, so the scalar mapping, the
		// array detection, the enum lookup and the input projections that reuse
		// `source` below all read one answer.
		field = schemaexport.ProjectedField(field)
		// The name to export is the field's API name, which is its column name
		// unless the schema declared a different one. Sanitization runs on the
		// result either way: an alias is an arbitrary annotation string too,
		// and a GraphQL field name must be a legal identifier or the schema
		// fails to build.
		//
		// The diagnostic paths keep naming the COLUMN. A warning about a name
		// the reader cannot find in their schema source is a warning they
		// cannot act on.
		exported := schemaexport.FieldAPIName(field, schemaexport.TargetGraphQL)
		name := schemaexport.SanitizeGraphQLName(exported)
		if name != exported {
			b.warn("type "+typeName+"."+field.Name, "column name is not a valid GraphQL name; exported as "+name)
		}
		if usedFieldNames[name] {
			b.warn("type "+typeName+"."+field.Name, "field name "+name+" collides with another column; omitted")
			continue
		}
		usedFieldNames[name] = true

		objectField := b.columnField(table, field, pk, name)
		if inObject[field.Name] {
			object.fields = append(object.fields, objectField)
		}
		if inInput[field.Name] {
			columns = append(columns, column{source: field, field: objectField})
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
	// Its operations go with it: they would all reference a type that does not
	// exist.
	if len(object.fields) == 0 {
		b.warn("type "+typeName, "table has no exportable columns; type omitted")
		return nil
	}

	b.objectTypes = append(b.objectTypes, object)
	b.addInputs(typeName, columns, pk)
	b.addQueries(table, typeName, columns, pk)
	return nil
}

// addInputs emits the requested operation inputs from the write projection: the
// columns a client may set, rather than every column that is not
// server-generated. An input whose projection is empty is omitted, because an
// input type with no fields does not parse.
func (b *builder) addInputs(typeName string, columns []column, pk map[string]bool) {
	if b.ops.CreateInput {
		fields := writeProjection(columns, pk, createShape)
		if len(fields) == 0 {
			b.warn("input "+typeName+createInputSuffix,
				"every column is server-owned, so the create projection is empty; input omitted")
		} else {
			b.createInputs = append(b.createInputs,
				gqlType{name: b.reg.unique(typeName + createInputSuffix), fields: fields})
		}
	}
	if b.ops.UpdateInput {
		fields := writeProjection(columns, pk, updateShape)
		if len(fields) == 0 {
			b.warn("input "+typeName+updateInputSuffix,
				"every column is server-owned or part of the primary key, so the update projection is empty; input omitted")
		} else {
			b.updateInputs = append(b.updateInputs,
				gqlType{name: b.reg.unique(typeName + updateInputSuffix), fields: fields})
		}
	}
}

// addQueries emits the requested Query root fields and, for the list shape, the
// connection and edge types they return.
func (b *builder) addQueries(table goschema.Table, typeName string, columns []column, pk map[string]bool) {
	if b.ops.List {
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
		b.addQueryField(gqlField{
			name: lowerFirst(schemaexport.SanitizeGraphQLName(schemaexport.PascalCase(table.Name))),
			args: "(first: Int, after: String)",
			typ:  connName,
		})
	}
	if !b.ops.ByID {
		return
	}
	key, ok := b.keyArgument(typeName, columns, pk)
	if !ok {
		return
	}
	b.addQueryField(gqlField{name: lowerFirst(typeName), args: "(" + key + ")", typ: typeName})
}

// keyArgument renders the by-key Query argument for a table. It reports false
// when the key is not usable as one: a composite or absent primary key has no
// single argument, and a key column that the object type did not publish (a
// name collision, for instance) would make the argument reference a field that
// is not there. Either way the operation is omitted rather than emitted broken.
func (b *builder) keyArgument(typeName string, columns []column, pk map[string]bool) (string, bool) {
	if len(pk) != 1 {
		// The shape was asked for by name, so a table that cannot supply it says
		// so rather than quietly contributing nothing.
		b.warn("type Query."+lowerFirst(typeName),
			fmt.Sprintf("by-id query needs a single-column primary key; this table declares %d key column(s), so it is omitted",
				len(pk)))
		return "", false
	}
	for _, col := range columns {
		if !pk[col.source.Name] {
			continue
		}
		// The argument repeats the published column's own type, so a key that
		// did not map to ID is not silently re-declared as one.
		arg := col.field
		arg.nonNull = true
		return col.field.name + ": " + arg.typeRef(), true
	}
	b.warn("type Query."+lowerFirst(typeName),
		"the primary key column is not present in the exported type; by-id query omitted")
	return "", false
}

// writeShape names which input projection a caller wants.
type writeShape int

const (
	createShape writeShape = iota
	updateShape
)

// writeProjection returns the input fields for a table's create or update
// shape: the published columns a client may assign, minus the ones the database
// owns. The update shape also drops the primary key, which identifies the row
// rather than being assigned to it, and makes every remaining field optional so
// an omitted field means "unchanged".
func writeProjection(columns []column, pk map[string]bool, shape writeShape) []gqlField {
	var fields []gqlField
	for _, col := range columns {
		if isServerOwned(col.source) {
			continue
		}
		if shape == updateShape && pk[col.source.Name] {
			continue
		}
		field := col.field
		field.desc = ""
		if shape == updateShape || hasDefault(col.source) {
			// A column the database fills in when it is not supplied, and every
			// column of a partial update, must not be mandatory on the way in.
			field.nonNull = false
		}
		fields = append(fields, field)
	}
	return fields
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

// isServerOwned reports whether the database, not the caller, produces a
// column's value: a serial or auto-increment column, a PostgreSQL
// GENERATED ALWAYS AS IDENTITY column, a generated/computed column, and a column
// the server rewrites on every update. None of them belongs in an input the
// caller fills in. A plain DEFAULT is not in this set — a caller may still
// supply that column — so those fields stay in the projection as optional.
func isServerOwned(field goschema.Field) bool {
	if field.AutoInc {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(field.IdentityGeneration), "ALWAYS") {
		return true
	}
	if strings.TrimSpace(field.GeneratedExpression) != "" {
		return true
	}
	if strings.TrimSpace(field.UpdateExpression) != "" {
		return true
	}
	base, _ := schemaexport.NormalizeType(field.Type)
	return strings.Contains(base, "SERIAL")
}

// hasDefault reports whether the database supplies a value when the column is
// omitted, which makes the column optional in a create input even when it is
// NOT NULL.
func hasDefault(field goschema.Field) bool {
	return field.DefaultSet || strings.TrimSpace(field.DefaultExpr) != "" ||
		strings.TrimSpace(field.Default) != ""
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
