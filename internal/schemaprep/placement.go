package schemaprep

import (
	"fmt"
	"maps"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// PlaceUnqualified returns a copy of database in which every object and
// reference written without a schema names schema instead, spelled as a source
// that wrote the schema would spell it. database is not modified, and an empty
// schema returns it as it is. It fails only when two feature objects become one
// identity, one written without a schema and one naming schema.
//
// A desired state read offline keeps an unqualified name relative, so the
// comparison resolves it in the connection's current schema and the plan writes
// it without a schema. On a connection that describes every schema of its
// database, the community binary instead reads the name where its dev database
// resolves it and writes every statement qualified. Placed here, the name means
// the same schema in the comparison, the plan and the rehearsal, whatever the
// session that runs the plan resolves an unqualified name to.
//
// What is placed: tables, sequences, enums, functions, views, materialized
// views, the tables of columns, indexes, constraints, triggers, row-level
// security records, grants and declared rows, the tables a foreign key or a
// relation embedding references, the function a trigger executes, the dependency
// maps, a column type that names a user type the database declares, and the
// identity of a feature object, such as a domain or a policy, whose schema the
// source left to the target's default. Bodies, expressions and defaults are SQL
// text and keep the names they were written with.
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

	placed.Tables = mapSlice(database.Tables, func(table schemamodel.Table) schemamodel.Table {
		if strings.TrimSpace(table.Schema) == "" {
			table.Schema = schema
		}
		return table
	})
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
	placed.Fields = mapSlice(database.Fields, func(field schemamodel.Field) schemamodel.Field {
		field.Foreign = placedReference(field.Foreign, schema)
		field.Type = qualifyUserTypeReference(field.Type, enums, enums)
		return field
	})
	placed.EmbeddedFields = mapSlice(database.EmbeddedFields, func(field schemamodel.EmbeddedField) schemamodel.EmbeddedField {
		field.Ref = placedReference(field.Ref, schema)
		return field
	})
	placed.Indexes = mapSlice(database.Indexes, func(index schemamodel.Index) schemamodel.Index {
		index.TableName = place(index.TableName)
		return index
	})
	placed.Constraints = mapSlice(database.Constraints, func(constraint schemamodel.Constraint) schemamodel.Constraint {
		constraint.Table = place(constraint.Table)
		constraint.ForeignTable = place(constraint.ForeignTable)
		return constraint
	})
	placed.Triggers = mapSlice(database.Triggers, func(trigger schemamodel.Trigger) schemamodel.Trigger {
		trigger.Table = place(trigger.Table)
		trigger.ExecuteFunction = place(trigger.ExecuteFunction)
		return trigger
	})
	placed.RLSPolicies = mapSlice(database.RLSPolicies, func(policy schemamodel.RLSPolicy) schemamodel.RLSPolicy {
		policy.Table = place(policy.Table)
		return policy
	})
	placed.RLSEnabledTables = mapSlice(database.RLSEnabledTables, func(table schemamodel.RLSEnabledTable) schemamodel.RLSEnabledTable {
		table.Table = place(table.Table)
		return table
	})
	placeGrant := func(grant schemamodel.Grant) schemamodel.Grant {
		grant.OnTable = place(grant.OnTable)
		grant.OnSequence = place(grant.OnSequence)
		grant.OnRoutine = place(grant.OnRoutine)
		return grant
	}
	placed.Grants = mapSlice(database.Grants, placeGrant)
	placed.RevokedGrants = mapSlice(database.RevokedGrants, placeGrant)
	placed.ManagedData = mapSlice(database.ManagedData, func(data schemamodel.ManagedData) schemamodel.ManagedData {
		if strings.TrimSpace(data.Schema) == "" && unqualified(data.Table) {
			data.Schema = schema
		}
		return data
	})
	placed.Dependencies = placedDependencies(database.Dependencies, place)
	placed.FunctionDependencies = placedDependencies(database.FunctionDependencies, place)
	return QualifyDeclaredUserTypes(&placed, targetPlatform), nil
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
func unqualified(name string) bool {
	ref, ok := tableref.Parse(name)
	return ok && !ref.Qualified
}

// placedReference is a foreign key reference, `table(column)` or `table`, with
// its table placed in schema.
func placedReference(reference, schema string) string {
	trimmed := strings.TrimSpace(reference)
	if trimmed == "" {
		return reference
	}
	open := strings.IndexByte(trimmed, '(')
	if open < 0 {
		return placedName(trimmed, schema)
	}
	table := strings.TrimSpace(trimmed[:open])
	if table == "" {
		return reference
	}
	return placedName(table, schema) + trimmed[open:]
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
