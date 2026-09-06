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

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaexport"
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
func Render(db *schemamodel.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}

	tables := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: opts.IncludeTables,
		ExcludeTables: opts.ExcludeTables,
	})
	policy := opts.FieldPolicy
	if policy == "" {
		policy = schemaexport.FieldPolicyAll
	}
	enums := schemaexport.EnumIndex(db)
	if err := validateGraphQLExport(db, tables, policy, enums); err != nil {
		return Result{}, err
	}

	reg := newNameRegistry()
	// Reserve built-in and structural names so no generated type can shadow
	// them. They are reserved whatever the operation selection is, so a table
	// named "page_info" gets the same object-type name in every profile.
	for _, reserved := range reservedGraphQLTypeNames {
		reg.reserve(reserved)
	}

	// Pass 1: assign a unique object-type name per table, so relations can
	// reference targets defined later in the file.
	typeNames := make(map[string]string, len(tables))
	unqualifiedTypeNames := make(map[string]string, len(tables))
	ambiguousUnqualified := make(map[string]bool)
	for _, table := range tables {
		name := reg.uniqueOwned(
			schemaexport.SanitizeGraphQLName(schemaexport.TypeName(
				schemaexport.TableAPIName(table, schemaexport.TargetGraphQL),
			)),
			tableNameOwnership(table),
		)
		typeNames[table.QualifiedName()] = name
		if ambiguousUnqualified[table.Name] {
			continue
		}
		if _, exists := unqualifiedTypeNames[table.Name]; exists {
			delete(unqualifiedTypeNames, table.Name)
			ambiguousUnqualified[table.Name] = true
			continue
		}
		unqualifiedTypeNames[table.Name] = name
	}

	b := &builder{
		reg:              reg,
		ops:              opts.Operations,
		enums:            enums,
		typeNames:        typeNames,
		unqualifiedTypes: unqualifiedTypeNames,
		enumNameByKey:    make(map[string]string),
		customScalars:    make(map[string]bool),
		usedQueryFields:  make(map[string]bool),
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

// validateGraphQLExport refuses contract declarations that the renderer would
// otherwise have to rename, omit, or silently map to a different type. It runs
// before the builder emits any part of the document.
func validateGraphQLExport(
	db *schemamodel.Database,
	tables []schemamodel.Table,
	policy schemaexport.FieldPolicy,
	enums map[string][]string,
) error {
	// An alias that shadows another column would drop it from the schema, and
	// the reader of that schema has nothing left to notice the loss with.
	// Raw-name collisions keep this direct error; the final-name checks below
	// catch collisions introduced by GraphQL normalization.
	if err := schemaexport.ValidateTableAPINames(tables, schemaexport.TargetGraphQL); err != nil {
		return err
	}
	if err := validateDeclaredGraphQLTableNames(tables); err != nil {
		return err
	}
	for _, table := range tables {
		if err := validateGraphQLTableFields(db, table, policy, enums); err != nil {
			return err
		}
	}
	return nil
}

func validateGraphQLTableFields(
	db *schemamodel.Database,
	table schemamodel.Table,
	policy schemaexport.FieldPolicy,
	enums map[string][]string,
) error {
	// The validation pre-pass reads the EXPORTED set, so two columns colliding
	// on api_name where the policy publishes only one of them is not a
	// collision. It has to agree with the build pass below or a refusal would
	// describe a schema nobody asked for.
	readable, _, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeRead, policy)
	if err != nil {
		return err
	}
	writable, _, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeWrite, policy)
	if err != nil {
		return err
	}
	fields, _, _ := unionShapes(readable, writable)
	if err := schemaexport.ValidateFieldAPINames(table, fields, schemaexport.TargetGraphQL); err != nil {
		return err
	}
	for _, field := range fields {
		if err := validateDeclaredGraphQLFieldName(table, field); err != nil {
			return err
		}
	}
	if err := validateDeclaredGraphQLFieldNames(table, fields); err != nil {
		return err
	}
	// An explicit type override the scalar mapping cannot honor is refused
	// rather than defaulted to String. Defaulting is right for an unrecognized
	// COLUMN type; here it would hide that the declaration did nothing.
	for _, field := range fields {
		if err := schemaexport.RefuseUnknownAPIType(
			table, field, schemaexport.TargetGraphQL, enums,
			func(t string) bool { return mapGraphQLScalar(t).Known },
		); err != nil {
			return err
		}
	}
	return nil
}

var reservedGraphQLTypeNames = []string{
	scalarInt, scalarFloat, scalarString, scalarBoolean, scalarID, scalarDateTime, scalarJSON,
	pageInfoType, queryType, "Mutation", "Subscription",
}

// validateDeclaredGraphQLTableNames protects authored contract names from
// collision resolution after they are parsed. An ordinary persistence name may
// still take the renderer's historical collision suffix, but api_name and
// graphql_name are contract declarations: silently turning InvoiceRecord into
// InvoiceRecord2 would publish a different contract from the one requested.
func validateDeclaredGraphQLTableNames(tables []schemamodel.Table) error {
	type owner struct {
		table    schemamodel.Table
		authored bool
	}

	reserved := make(map[string]bool, len(reservedGraphQLTypeNames))
	for _, name := range reservedGraphQLTypeNames {
		reserved[name] = true
	}

	owners := make(map[string]owner, len(tables))
	for _, table := range tables {
		targetDeclared := table.APINames.GraphQL != ""
		authored := targetDeclared || table.APIName != ""
		name := schemaexport.TypeName(schemaexport.TableAPIName(table, schemaexport.TargetGraphQL))
		if targetDeclared {
			if !schemaexport.IsValidGraphQLName(name) || strings.HasPrefix(name, "__") {
				return fmt.Errorf(
					"table %q declares graphql_name %q, which does not produce a valid GraphQL type name",
					table.Name,
					table.APINames.GraphQL,
				)
			}
		} else {
			name = schemaexport.SanitizeGraphQLName(name)
		}
		if authored && reserved[name] {
			attribute, value := "api_name", table.APIName
			if targetDeclared {
				attribute, value = "graphql_name", table.APINames.GraphQL
			}
			return fmt.Errorf(
				"table %q declares %s %q, which produces reserved GraphQL type name %q; choose a different graphql_name",
				table.Name,
				attribute,
				value,
				name,
			)
		}

		if first, taken := owners[name]; taken && (authored || first.authored) {
			return fmt.Errorf(
				"tables %q and %q both produce GraphQL type name %q; give one of them a distinct graphql_name",
				first.table.Name,
				table.Name,
				name,
			)
		}
		owners[name] = owner{table: table, authored: authored}
	}
	return nil
}

func hasAuthoredGraphQLTableName(table schemamodel.Table) bool {
	return table.APIName != "" || table.APINames.GraphQL != ""
}

func tableNameOwnership(table schemamodel.Table) nameOwnership {
	if hasAuthoredGraphQLTableName(table) {
		return authoredName
	}
	return derivedName
}

func hasAuthoredGraphQLFieldName(field schemamodel.Field) bool {
	return field.APIName != "" || field.APINames.GraphQL != ""
}

// GraphQL table declarations are stems: every table name is singularized and
// PascalCased into a type. Field declarations, by contrast, are the exact
// published identifier and must already satisfy GraphQL's grammar.
func validateDeclaredGraphQLFieldName(table schemamodel.Table, field schemamodel.Field) error {
	declared := field.APINames.GraphQL
	if declared == "" {
		return nil
	}
	if schemaexport.IsValidGraphQLName(declared) && !strings.HasPrefix(declared, "__") {
		return nil
	}
	return fmt.Errorf(
		"column %q on table %q declares graphql_name %q, which is not a valid GraphQL field name",
		field.Name,
		table.Name,
		declared,
	)
}

func validateDeclaredGraphQLFieldNames(table schemamodel.Table, fields []schemamodel.Field) error {
	type owner struct {
		field    schemamodel.Field
		authored bool
	}
	owners := make(map[string]owner, len(fields))
	for _, field := range fields {
		targetDeclared := field.APINames.GraphQL != ""
		authored := targetDeclared || field.APIName != ""
		name := schemaexport.FieldAPIName(field, schemaexport.TargetGraphQL)
		if !targetDeclared {
			name = schemaexport.SanitizeGraphQLName(name)
		}
		if authored && strings.HasPrefix(name, "__") {
			attribute, value := "api_name", field.APIName
			if targetDeclared {
				attribute, value = "graphql_name", field.APINames.GraphQL
			}
			return fmt.Errorf(
				"column %q on table %q declares %s %q, which produces reserved GraphQL field name %q; choose a different graphql_name",
				field.Name,
				table.Name,
				attribute,
				value,
				name,
			)
		}
		if first, taken := owners[name]; taken && (authored || first.authored) {
			return fmt.Errorf(
				"columns %q and %q on table %q both produce GraphQL field name %q; give one of them a distinct graphql_name",
				first.field.Name,
				field.Name,
				table.Name,
				name,
			)
		}
		owners[name] = owner{field: field, authored: authored}
	}
	return nil
}

// builder accumulates the SDL model across tables before serialization.
type builder struct {
	reg              *nameRegistry
	ops              Operations
	enums            map[string][]string
	typeNames        map[string]string
	unqualifiedTypes map[string]string
	enumNameByKey    map[string]string
	customScalars    map[string]bool

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
	source schemamodel.Field
	field  gqlField
}

type tableColumns struct {
	object         gqlType
	inputs         []column
	usedFieldNames map[string]bool
}

// unionShapes returns every column that reaches either contract, and which of
// the two each one reaches.
//
// The order is the read shape's, with write-only columns appended, so a schema
// does not reshuffle when an exposure changes.
func unionShapes(read, write []schemamodel.Field) (all []schemamodel.Field, inObject, inInput map[string]bool) {
	inObject = make(map[string]bool, len(read))
	for _, field := range read {
		inObject[field.Name] = true
	}
	inInput = make(map[string]bool, len(write))
	for _, field := range write {
		inInput[field.Name] = true
	}
	all = append([]schemamodel.Field(nil), read...)
	for _, field := range write {
		if !inObject[field.Name] {
			all = append(all, field)
		}
	}
	return all, inObject, inInput
}

func (b *builder) addTable(db *schemamodel.Database, table schemamodel.Table, policy schemaexport.FieldPolicy) error {
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
	typeName := b.typeNames[table.QualifiedName()]

	built, err := b.buildTableColumns(
		table, fields, pk, inObject, inInput, typeName,
	)
	if err != nil {
		return err
	}
	if err := b.addTableRelations(table, fields, typeName, built.usedFieldNames, &built.object); err != nil {
		return err
	}

	// A type with no fields is a GraphQL syntax error; skip the whole table.
	// Its operations go with it: they would all reference a type that does not
	// exist.
	if len(built.object.fields) == 0 {
		b.warn("type "+typeName, "table has no exportable columns; type omitted")
		return nil
	}

	b.objectTypes = append(b.objectTypes, built.object)
	if err := b.addInputs(table, typeName, built.inputs, pk); err != nil {
		return err
	}
	return b.addQueries(table, typeName, built.inputs, pk)
}

func (b *builder) buildTableColumns(
	table schemamodel.Table,
	fields []schemamodel.Field,
	pk, inObject, inInput map[string]bool,
	typeName string,
) (tableColumns, error) {
	built := tableColumns{
		object:         gqlType{name: typeName, desc: table.Comment},
		usedFieldNames: make(map[string]bool),
	}

	for _, field := range fields {
		// Substitute the API type once, so scalar mapping, array detection, enum
		// lookup, and the input projections all read the same answer.
		field = schemaexport.ProjectedField(field)
		exported := schemaexport.FieldAPIName(field, schemaexport.TargetGraphQL)
		name := schemaexport.SanitizeGraphQLName(exported)
		if name != exported {
			b.warn("type "+typeName+"."+field.Name, "column name is not a valid GraphQL name; exported as "+name)
		}
		if _, used := built.usedFieldNames[name]; used {
			b.warn("type "+typeName+"."+field.Name, "field name "+name+" collides with another column; omitted")
			continue
		}
		built.usedFieldNames[name] = hasAuthoredGraphQLFieldName(field)

		objectField, err := b.columnField(table, field, pk, name)
		if err != nil {
			return tableColumns{}, err
		}
		if inObject[field.Name] {
			built.object.fields = append(built.object.fields, objectField)
		}
		if inInput[field.Name] {
			built.inputs = append(built.inputs, column{source: field, field: objectField})
		}
	}
	return built, nil
}

// addTableRelations adds object relations alongside the scalar foreign-key
// columns. Derived-only collisions keep the historical omission behavior;
// authored names are refused rather than silently changed.
func (b *builder) addTableRelations(
	table schemamodel.Table,
	fields []schemamodel.Field,
	typeName string,
	usedFieldNames map[string]bool,
	object *gqlType,
) error {
	for _, field := range fields {
		ref, ok := schemaexport.ParseForeignRef(field.Foreign)
		if strings.TrimSpace(field.Foreign) == "" || !ok {
			continue
		}
		relName, ok := schemaexport.RelationFieldName(
			schemaexport.FieldAPIName(field, schemaexport.TargetGraphQL),
		)
		if !ok {
			// A published scalar name without an id suffix cannot name the
			// relation by itself. Preserve the relation derived from storage.
			relName, ok = schemaexport.RelationFieldName(field.Name)
		}
		if !ok {
			continue
		}
		relName = schemaexport.SanitizeGraphQLName(relName)
		relationAuthored := hasAuthoredGraphQLFieldName(field)
		if firstAuthored, used := usedFieldNames[relName]; used {
			if relationAuthored || firstAuthored {
				return fmt.Errorf(
					"foreign-key column %q on table %q produces GraphQL relation field name %q, which collides with another field; choose a distinct graphql_name",
					field.Name,
					table.Name,
					relName,
				)
			}
			continue
		}
		targetType, ok := b.resolveTableTypeName(table, ref.Table)
		if !ok {
			b.warn("type "+typeName+"."+field.Name,
				fmt.Sprintf("foreign key references table %q which is not exported; relation field omitted", ref.Table))
			continue
		}
		object.fields = append(object.fields, gqlField{
			name: relName, typ: targetType, nonNull: !field.Nullable,
		})
		usedFieldNames[relName] = relationAuthored
	}
	return nil
}

func (b *builder) resolveTableTypeName(source schemamodel.Table, reference string) (string, bool) {
	if name, ok := b.typeNames[reference]; ok {
		return name, true
	}
	if source.Schema != "" && !strings.Contains(reference, ".") {
		if name, ok := b.typeNames[schemamodel.QualifyTableName(source.Schema, reference)]; ok {
			return name, true
		}
	}
	name, ok := b.unqualifiedTypes[reference]
	return name, ok
}

// addInputs emits the requested operation inputs from the write projection: the
// columns a client may set, rather than every column that is not
// server-generated. An input whose projection is empty is omitted, because an
// input type with no fields does not parse.
func (b *builder) addInputs(table schemamodel.Table, typeName string, columns []column, pk map[string]bool) error {
	if b.ops.CreateInput {
		fields := writeProjection(columns, pk, createShape)
		if len(fields) == 0 {
			b.warn("input "+typeName+createInputSuffix,
				"every column is server-owned, so the create projection is empty; input omitted")
		} else {
			desired := typeName + createInputSuffix
			name, collision := b.reg.claim(desired, tableNameOwnership(table))
			if collision {
				return fmt.Errorf(
					"table %q produces GraphQL operation type name %q, which collides with another type; choose a distinct graphql_name",
					table.Name,
					desired,
				)
			}
			b.createInputs = append(b.createInputs,
				gqlType{name: name, fields: fields})
		}
	}
	if b.ops.UpdateInput {
		fields := writeProjection(columns, pk, updateShape)
		if len(fields) == 0 {
			b.warn("input "+typeName+updateInputSuffix,
				"every column is server-owned or part of the primary key, so the update projection is empty; input omitted")
		} else {
			desired := typeName + updateInputSuffix
			name, collision := b.reg.claim(desired, tableNameOwnership(table))
			if collision {
				return fmt.Errorf(
					"table %q produces GraphQL operation type name %q, which collides with another type; choose a distinct graphql_name",
					table.Name,
					desired,
				)
			}
			b.updateInputs = append(b.updateInputs,
				gqlType{name: name, fields: fields})
		}
	}
	return nil
}

// addQueries emits the requested Query root fields and, for the list shape, the
// connection and edge types they return.
func (b *builder) addQueries(table schemamodel.Table, typeName string, columns []column, pk map[string]bool) error {
	if b.ops.List {
		desiredEdgeName := typeName + "Edge"
		edgeName, collision := b.reg.claim(desiredEdgeName, tableNameOwnership(table))
		if collision {
			return fmt.Errorf(
				"table %q produces GraphQL operation type name %q, which collides with another type; choose a distinct graphql_name",
				table.Name,
				desiredEdgeName,
			)
		}
		b.edgeTypes = append(b.edgeTypes, gqlType{
			name: edgeName,
			fields: []gqlField{
				{name: "node", typ: typeName, nonNull: true},
				{name: "cursor", typ: scalarString, nonNull: true},
			},
		})
		desiredConnectionName := typeName + "Connection"
		connName, collision := b.reg.claim(
			desiredConnectionName,
			tableNameOwnership(table),
		)
		if collision {
			return fmt.Errorf(
				"table %q produces GraphQL operation type name %q, which collides with another type; choose a distinct graphql_name",
				table.Name,
				desiredConnectionName,
			)
		}
		b.connTypes = append(b.connTypes, gqlType{
			name: connName,
			fields: []gqlField{
				{name: "edges", typ: edgeName, nonNull: true, list: true, listNonNull: true},
				{name: "pageInfo", typ: pageInfoType, nonNull: true},
			},
		})
		if err := b.addQueryField(table, gqlField{
			name: lowerFirst(schemaexport.SanitizeGraphQLName(schemaexport.PascalCase(
				schemaexport.TableAPIName(table, schemaexport.TargetGraphQL),
			))),
			args: "(first: Int, after: String)",
			typ:  connName,
		}); err != nil {
			return err
		}
	}
	if !b.ops.ByID {
		return nil
	}
	key, ok := b.keyArgument(typeName, columns, pk)
	if !ok {
		return nil
	}
	return b.addQueryField(table, gqlField{name: lowerFirst(typeName), args: "(" + key + ")", typ: typeName})
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
func (b *builder) columnField(
	table schemamodel.Table,
	field schemamodel.Field,
	pk map[string]bool,
	name string,
) (gqlField, error) {
	elementField := field
	list := false
	if element, isArray := schemaexport.ElementType(field.Type); isArray {
		elementField.Type = element
		list = true
	}
	gt, err := b.resolveColumnType(table, elementField, pk)
	if err != nil {
		return gqlField{}, err
	}
	nonNull := !field.Nullable || pk[field.Name]
	return gqlField{
		name:        name,
		typ:         gt,
		nonNull:     nonNull,
		list:        list,
		listNonNull: false, // SQL arrays may contain null elements
		desc:        field.Comment,
	}, nil
}

func (b *builder) addQueryField(table schemamodel.Table, field gqlField) error {
	authored := hasAuthoredGraphQLTableName(table)
	if firstAuthored, used := b.usedQueryFields[field.name]; used {
		if authored || firstAuthored {
			return fmt.Errorf(
				"table %q produces GraphQL Query field name %q, which collides with another table; choose a distinct graphql_name",
				table.Name,
				field.name,
			)
		}
		b.warn("type Query."+field.name, "duplicate query field name; omitted")
		return nil
	}
	b.usedQueryFields[field.name] = authored
	b.queryFields = append(b.queryFields, field)
	return nil
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
func (b *builder) resolveColumnType(
	table schemamodel.Table,
	field schemamodel.Field,
	pk map[string]bool,
) (string, error) {
	if values, ok := schemaexport.ResolveEnumValues(field, b.enums); ok {
		name, valid, err := b.enumType(table, field, values)
		if err != nil {
			return "", err
		}
		if valid {
			return name, nil
		}
		// Values are not valid GraphQL enum names; fall back to a scalar.
		b.warn("type "+b.typeNames[table.QualifiedName()]+"."+field.Name,
			"enum values are not valid GraphQL enum names; emitted as String")
		return scalarString, nil
	}

	scalar := mapGraphQLScalar(field.Type)
	if scalar.Custom != "" {
		b.customScalars[scalar.Custom] = true
	}
	if !scalar.Known {
		b.warn("type "+b.typeNames[table.QualifiedName()]+"."+field.Name,
			fmt.Sprintf("unknown column type %q mapped to String", field.Type))
	}
	if pk[field.Name] && scalar.Known {
		return scalarID, nil
	}
	return scalar.Name, nil
}

// enumType returns the GraphQL enum type name for a field, defining the enum
// once and deduplicating fields that share the same source enum. The second
// result is false when the values are not valid GraphQL enum names. A type-name
// collision is returned separately so it cannot be mistaken for that lossy
// fallback.
func (b *builder) enumType(
	table schemamodel.Table,
	field schemamodel.Field,
	values []string,
) (string, bool, error) {
	for _, value := range values {
		if !schemaexport.IsValidGraphQLName(value) {
			return "", false, nil
		}
	}
	key := b.enumSourceKey(table, field)
	if name, ok := b.enumNameByKey[key]; ok {
		return name, true, nil
	}
	desired := b.desiredEnumName(table, field)
	ownership := derivedName
	if hasAuthoredGraphQLTableName(table) ||
		hasAuthoredGraphQLFieldName(field) || field.APIType != "" {
		ownership = authoredName
	}
	name, collision := b.reg.claim(desired, ownership)
	if collision {
		return "", false, fmt.Errorf(
			"column %q on table %q produces GraphQL enum type name %q, which collides with another type; choose distinct API names or api_type",
			field.Name,
			table.Name,
			desired,
		)
	}
	b.enumNameByKey[key] = name
	b.enumTypes = append(b.enumTypes, gqlEnum{name: name, values: values})
	return name, true, nil
}

func (b *builder) enumSourceKey(table schemamodel.Table, field schemamodel.Field) string {
	if !mapGraphQLScalar(field.Type).Known {
		return "type:" + field.Type // named enum type shared across columns
	}
	return "col:" + table.QualifiedName() + "." + field.Name // inline enum, unique per column
}

func (b *builder) desiredEnumName(table schemamodel.Table, field schemamodel.Field) string {
	var raw string
	if !mapGraphQLScalar(field.Type).Known {
		raw = schemaexport.PascalCase(strings.TrimPrefix(field.Type, "enum_"))
	} else {
		raw = schemaexport.TypeName(schemaexport.TableAPIName(table, schemaexport.TargetGraphQL)) +
			schemaexport.PascalCase(schemaexport.FieldAPIName(field, schemaexport.TargetGraphQL))
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
func isServerOwned(field schemamodel.Field) bool {
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
func hasDefault(field schemamodel.Field) bool {
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
