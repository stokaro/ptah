package schemaprep

import (
	"fmt"
	"maps"
	"regexp"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnsequence"
	"ptah.run/internal/tableref"
)

// PlaceUnqualified returns a copy of database in which every object and
// reference written without a schema names schema instead, spelled as a source
// that wrote the schema would spell it. database is not modified, and an empty
// schema returns it as it is. It fails only when two feature objects become one
// identity, one written without a schema and one naming schema.
//
// A desired schema read offline keeps an unqualified name relative, so the
// comparison resolves it in the connection's current schema and the plan writes
// it without a schema. On a connection that describes every schema of its
// database, the community binary instead reads the name where its dev database
// resolves it and writes every statement qualified. Placed here, the name means
// the same schema in the comparison, the plan and the rehearsal, whatever the
// session that runs the plan resolves an unqualified name to.
//
// What is placed: tables, sequences, enums, functions, views, materialized
// views, a column type that names a user type the database declares, and the
// identity of a feature object, such as a domain or a policy, whose schema the
// source left to the target's default. A reference written without a schema is
// placed when it names one of those objects the database declares without a
// schema: the tables of columns, indexes, constraints, triggers, row-level
// security records, grants and declared rows, the tables a foreign key or a
// relation embedding references, the parent a partition names, the function a
// trigger executes, the sequence or routine a grant names, the dependency maps
// and the self-referencing foreign keys. A table's DependsOn is matched by its
// bare name, so it needs no schema. A reference to an object the database does
// not declare, such as a built-in trigger function or a table another tool
// manages, keeps the name it was written with and is resolved where the
// session finds it. Bodies, expressions and defaults are
// SQL text and keep the names they were written with, and so does the table of
// a sequence's OWNED BY, which the comparison reads as the server prints it.
func PlaceUnqualified(database *schemamodel.Database, schema, targetPlatform string) (*schemamodel.Database, error) {
	schema = strings.TrimSpace(schema)
	if database == nil || schema == "" {
		return database, nil
	}
	objects, err := placedFeatureObjects(database.FeatureObjects, schema, targetPlatform)
	if err != nil {
		return nil, err
	}
	placed := *database
	placed.FeatureObjects = objects
	place := func(name string) string { return placedName(name, schema) }
	declared := declaredUnqualified(database, identifier.ForDialect(targetPlatform))
	relation := func(name string) string { return declared.place(name, schema, declared.relations) }
	routine := func(name string) string { return declared.placeRoutine(name, schema) }
	sequence := func(name string) string { return declared.place(name, schema, declared.sequences) }
	anyDeclared := func(name string) string { return declared.place(name, schema, declared.relations, declared.functions) }

	placed.Tables, err = placedTables(database.Tables, schema, declared)
	if err != nil {
		return nil, err
	}
	placed.Sequences = mapSlice(database.Sequences, func(sequence schemamodel.Sequence) schemamodel.Sequence {
		if strings.TrimSpace(sequence.Schema) == "" && unqualified(sequence.Name) {
			sequence.Schema = schema
		}
		return sequence
	})
	placed.Enums = mapSlice(database.Enums, func(enum schemamodel.Enum) schemamodel.Enum {
		if strings.TrimSpace(enum.Schema) == "" {
			enum.Name = place(enum.Name)
		}
		return enum
	})
	placed.Functions = mapSlice(database.Functions, func(function schemamodel.Function) schemamodel.Function {
		function.Name = place(function.Name)
		return function
	})
	placed.Views = mapSlice(database.Views, func(view schemamodel.View) schemamodel.View {
		view.Name = place(view.Name)
		return view
	})
	placed.MaterializedViews = mapSlice(database.MaterializedViews, func(view schemamodel.MaterializedView) schemamodel.MaterializedView {
		view.Name = place(view.Name)
		return view
	})
	enums := placedEnumTypes(database.Enums, schema, targetPlatform)
	placeReferences(&placed, referenceMaps{
		relation: relation, sequence: sequence, routine: routine, anyDeclared: anyDeclared,
		database: unchanged, object: unchanged,
	})
	placed.Fields = mapSlice(placed.Fields, func(field schemamodel.Field) schemamodel.Field {
		field.Type = qualifyUserTypeReference(field.Type, enums, enums)
		return field
	})
	placed.ManagedData = mapSlice(database.ManagedData, func(data schemamodel.ManagedData) schemamodel.ManagedData {
		if strings.TrimSpace(data.Schema) == "" && unqualified(data.Table) && declared.has(data.Table, declared.relations) {
			data.Schema = schema
		}
		return data
	})
	return QualifyDeclaredUserTypes(&placed, targetPlatform), nil
}

// referenceMaps are the functions a placement maps each kind of reference by:
// a table or view, a sequence, a routine written as `name` or
// `name(arguments)`, a routine dependency that may name either, the database a
// grant names as its target, and the other object a grant names.
type referenceMaps struct {
	relation    func(string) string
	sequence    func(string) string
	routine     func(string) string
	anyDeclared func(string) string
	database    func(string) string
	object      func(string) string
}

// unchanged is a reference map that leaves a reference as written.
func unchanged(name string) string { return name }

// placeReferences maps every reference the objects of placed make to another
// object, each kind by its function in by: the tables of columns, relation
// embeddings, indexes, constraints, triggers, row-level security records and
// grants, the tables a foreign key references, the function a trigger
// executes, the sequence, routine, database or object a grant or a revoke
// names, the dependency maps and the self-referencing foreign keys.
//
// It is the one list of reference sites [PlaceUnqualified] and
// [RespellReadDatabases] read, so a site added for one is a site the other
// maps too.
func placeReferences(placed *schemamodel.Database, by referenceMaps) {
	placed.Fields = mapSlice(placed.Fields, func(field schemamodel.Field) schemamodel.Field {
		field.Foreign = placedReference(field.Foreign, by.relation)
		return field
	})
	placed.EmbeddedFields = mapSlice(placed.EmbeddedFields, func(field schemamodel.EmbeddedField) schemamodel.EmbeddedField {
		field.Ref = placedReference(field.Ref, by.relation)
		return field
	})
	placed.Indexes = mapSlice(placed.Indexes, func(index schemamodel.Index) schemamodel.Index {
		index.TableName = by.relation(index.TableName)
		return index
	})
	placed.Constraints = mapSlice(placed.Constraints, func(constraint schemamodel.Constraint) schemamodel.Constraint {
		constraint.Table = by.relation(constraint.Table)
		constraint.ForeignTable = by.relation(constraint.ForeignTable)
		return constraint
	})
	placed.Triggers = mapSlice(placed.Triggers, func(trigger schemamodel.Trigger) schemamodel.Trigger {
		trigger.Table = by.relation(trigger.Table)
		trigger.ExecuteFunction = by.routine(trigger.ExecuteFunction)
		return trigger
	})
	placed.RLSPolicies = mapSlice(placed.RLSPolicies, func(policy schemamodel.RLSPolicy) schemamodel.RLSPolicy {
		policy.Table = by.relation(policy.Table)
		return policy
	})
	placed.RLSEnabledTables = mapSlice(placed.RLSEnabledTables, func(table schemamodel.RLSEnabledTable) schemamodel.RLSEnabledTable {
		table.Table = by.relation(table.Table)
		return table
	})
	placeGrant := func(grant schemamodel.Grant) schemamodel.Grant {
		grant.OnTable = by.relation(grant.OnTable)
		grant.OnSchema = by.database(grant.OnSchema)
		grant.OnSequence = by.sequence(grant.OnSequence)
		grant.OnRoutine = by.routine(grant.OnRoutine)
		grant.OnObject = by.object(grant.OnObject)
		return grant
	}
	placed.Grants = mapSlice(placed.Grants, placeGrant)
	placed.RevokedGrants = mapSlice(placed.RevokedGrants, placeGrant)
	placed.Dependencies = placedDependencies(placed.Dependencies, by.relation)
	placed.FunctionDependencies = placedDependencies(placed.FunctionDependencies, by.anyDeclared)
	placed.SelfReferencingForeignKeys = placedSelfReferences(placed.SelfReferencingForeignKeys, by.relation)
}

// placedTables is tables with each one written without a schema placed in
// schema, and the parent each one names without a schema placed there too when
// the database declares that parent without a schema; see
// [schemaext.PlacedTableParent]. A partition is created in the placed schema,
// and its statement names the parent's.
func placedTables(tables []schemamodel.Table, schema string, declared declaredNames) ([]schemamodel.Table, error) {
	if tables == nil {
		return nil, nil
	}
	placed := make([]schemamodel.Table, len(tables))
	for i, table := range tables {
		if strings.TrimSpace(table.Schema) == "" {
			table.Schema = schema
		}
		facets, err := placedTableParent(table.Facets, schema, declared)
		if err != nil {
			return nil, fmt.Errorf("place the parent of table %q in %q: %w", table.Name, schema, err)
		}
		table.Facets = facets
		placed[i] = table
	}
	return placed, nil
}

// placedTableParent is facets with the parent a value names without a schema
// placed in schema, when the database declares that parent without a schema.
func placedTableParent(facets schemaext.Facets, schema string, declared declaredNames) (schemaext.Facets, error) {
	values, err := facets.Values()
	if err != nil {
		return schemaext.Facets{}, err
	}
	for _, value := range values {
		parent, ok := value.(schemaext.PlacedTableParent)
		if !ok {
			continue
		}
		parentSchema, name := parent.ParentTable()
		if strings.TrimSpace(parentSchema) != "" || !declared.has(name, declared.relations) {
			continue
		}
		if facets, err = facets.Replace(parent.WithParentSchema(schema)); err != nil {
			return schemaext.Facets{}, err
		}
	}
	return facets, nil
}

// placedSelfReferences is the self-referencing foreign keys of a database,
// keyed by table, with each table and each reference placed by place. The map
// is keyed by the table's qualified name, so a key left as written stops
// naming the table once the table is placed.
func placedSelfReferences(
	references map[string][]schemamodel.SelfReferencingFK,
	place func(string) string,
) map[string][]schemamodel.SelfReferencingFK {
	if references == nil {
		return nil
	}
	placed := make(map[string][]schemamodel.SelfReferencingFK, len(references))
	for table, keys := range maps.All(references) {
		placedKeys := make([]schemamodel.SelfReferencingFK, len(keys))
		for i, key := range keys {
			key.Foreign = placedReference(key.Foreign, place)
			placedKeys[i] = key
		}
		placed[place(table)] = placedKeys
	}
	return placed
}

// placedFeatureObjects is objects with every identity whose schema the source
// left to the target's default naming schema instead. The comparison binds a
// defaulted schema to the connection's default, so a policy on a placed table
// would otherwise name a table of another schema, and a domain would be
// created where the connection's search path puts it. Only the identity
// changes: the value keeps what the source wrote, as a binding by
// [tableidentity] keeps it.
func placedFeatureObjects(objects schemaext.Objects, schema, targetPlatform string) (schemaext.Objects, error) {
	if objects.IsZero() {
		return objects, nil
	}
	all, err := objects.All()
	if err != nil {
		return schemaext.Objects{}, err
	}
	part := objectidentity.NewBuilder(identifier.ForDialect(targetPlatform)).TableParts(schema, "placed").Schema
	changed := false
	for i, object := range all {
		ref := object.Ref
		if !ref.Schema.Defaulted || !ref.Database.Empty() || !ref.Namespace.IsZero() {
			continue
		}
		ref.Schema = part
		all[i].Ref, changed = ref, true
	}
	if !changed {
		return objects, nil
	}
	placed, err := schemaext.NewObjects(all...)
	if err != nil {
		return schemaext.Objects{}, fmt.Errorf("place feature objects written without a schema in %q: %w", schema, err)
	}
	return placed, nil
}

// placedEnumTypes maps the name of each enum written without a schema to its
// placed name, the spelling a column type takes when its source names the
// enum's schema. A name declared twice, or one that names a built-in type,
// maps to nothing, as [QualifyDeclaredUserTypes] leaves it.
func placedEnumTypes(enums []schemamodel.Enum, schema, targetPlatform string) map[string]string {
	placed := make(map[string]string)
	for _, enum := range enums {
		name := strings.TrimSpace(enum.Name)
		if strings.TrimSpace(enum.Schema) != "" || !unqualified(name) {
			continue
		}
		if _, seen := placed[name]; seen || namesABuiltInType(targetPlatform, name) {
			placed[name] = ""
			continue
		}
		placed[name] = placedName(name, schema)
	}
	return placed
}

// placedName is name in schema when name names no schema, and name otherwise.
// An empty name stays empty: the object it belongs to names its table through
// its Go struct instead.
func placedName(name, schema string) string {
	if !unqualified(name) {
		return name
	}
	return tableref.Canonical("", schema) + "." + strings.TrimSpace(name)
}

// unqualified reports whether name is one name with no schema in front of it.
// sequenceNameOption finds the SEQUENCE NAME option in an identity column's
// options.
var sequenceNameOption = regexp.MustCompile(`(?i)\bSEQUENCE\s+NAME\b`)

// namesItsSequence reports whether field is an identity column whose options
// name the sequence it creates.
func namesItsSequence(field schemamodel.Field) bool {
	return sequenceNameOption.MatchString(field.IdentityOptions)
}

func unqualified(name string) bool {
	ref, ok := tableref.Parse(name)
	return ok && !ref.Qualified
}

// declaredNames are the objects a database declares without a schema, by
// identity key, which [PlaceUnqualified] places. A reference is placed only
// when it names one of them.
type declaredNames struct {
	semantics identifier.Semantics
	// relations are the tables, views and materialized views.
	relations map[string]bool
	sequences map[string]bool
	functions map[string]bool
}

// declaredUnqualified collects the objects database declares without a
// schema.
func declaredUnqualified(database *schemamodel.Database, semantics identifier.Semantics) declaredNames {
	names := declaredNames{
		semantics: semantics,
		relations: make(map[string]bool),
		sequences: make(map[string]bool),
		functions: make(map[string]bool),
	}
	add := func(set map[string]bool, name string) {
		if unqualified(name) {
			set[semantics.TableIdentityKey(strings.TrimSpace(name))] = true
		}
	}
	for _, table := range database.Tables {
		if strings.TrimSpace(table.Schema) != "" {
			continue
		}
		add(names.relations, table.Name)
		// A serial or identity column creates its sequence beside the table,
		// so a grant on that sequence names a declared object too. An identity
		// column that names its sequence is left out: the name it gives is in
		// options this reads no further, and the name PostgreSQL would
		// otherwise give is not the sequence's, so claiming it could place a
		// grant on another object.
		for _, field := range database.Fields {
			if field.StructName != table.StructName || namesItsSequence(field) {
				continue
			}
			if sequence, ok := columnsequence.Declared(table.Name, field); ok {
				add(names.sequences, sequence)
			}
		}
	}
	for _, view := range database.Views {
		add(names.relations, view.Name)
	}
	for _, view := range database.MaterializedViews {
		add(names.relations, view.Name)
	}
	for _, sequence := range database.Sequences {
		if strings.TrimSpace(sequence.Schema) == "" {
			add(names.sequences, sequence.Name)
		}
	}
	for _, function := range database.Functions {
		add(names.functions, function.Name)
	}
	return names
}

// has reports whether name, written without a schema, names an object of one
// of sets.
func (d declaredNames) has(name string, sets ...map[string]bool) bool {
	if !unqualified(name) {
		return false
	}
	key := d.semantics.TableIdentityKey(strings.TrimSpace(name))
	for _, set := range sets {
		if set[key] {
			return true
		}
	}
	return false
}

// place is name in schema when it names, without a schema, an object of one
// of sets, and name otherwise.
func (d declaredNames) place(name, schema string, sets ...map[string]bool) string {
	if !d.has(name, sets...) {
		return name
	}
	return placedName(name, schema)
}

// placeRoutine is a routine reference, `name` or `name(arguments)`, placed in
// schema when it names a function the database declares without a schema.
func (d declaredNames) placeRoutine(reference, schema string) string {
	trimmed := strings.TrimSpace(reference)
	name, arguments, _ := strings.Cut(trimmed, "(")
	name = strings.TrimSpace(name)
	if name == "" || !d.has(name, d.functions) {
		return reference
	}
	if trimmed == name {
		return placedName(name, schema)
	}
	return placedName(name, schema) + "(" + arguments
}

// placedReference is a foreign key reference, `table(column)` or `table`, with
// its table placed by place.
func placedReference(reference string, place func(string) string) string {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return reference
	}
	open := strings.IndexByte(trimmed, '(')
	if open < 0 {
		return place(trimmed)
	}
	table := strings.TrimSpace(trimmed[:open])
	if table == "" {
		return reference
	}
	return place(table) + trimmed[open:]
}

// placedDependencies is a dependency map with every key and every value placed.
func placedDependencies(dependencies map[string][]string, place func(string) string) map[string][]string {
	if dependencies == nil {
		return nil
	}
	placed := make(map[string][]string, len(dependencies))
	for name, needs := range maps.All(dependencies) {
		placedNeeds := make([]string, len(needs))
		for i, need := range needs {
			placedNeeds[i] = place(need)
		}
		placed[place(name)] = placedNeeds
	}
	return placed
}

// mapSlice is values with each element replaced by convert's answer, nil for
// nil.
func mapSlice[T any](values []T, convert func(T) T) []T {
	if values == nil {
		return nil
	}
	out := make([]T, len(values))
	for i, value := range values {
		out[i] = convert(value)
	}
	return out
}
