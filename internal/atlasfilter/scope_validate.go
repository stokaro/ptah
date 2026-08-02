package atlasfilter

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/deporder"
)

// scopeDiagnostics collects deduplicated cross-scope dependency violations.
type scopeDiagnostics map[string]struct{}

func (d scopeDiagnostics) addf(format string, args ...any) {
	d[fmt.Sprintf(format, args...)] = struct{}{}
}

func (d scopeDiagnostics) err() error {
	if len(d) == 0 {
		return nil
	}
	return &CrossScopeError{Diagnostics: slices.Sorted(maps.Keys(d))}
}

// validateGeneratedScope refuses projections where a selected desired-state
// object depends on an object the selection dropped: foreign keys to
// unselected tables, functions depending on unselected functions, view,
// materialized view, and trigger bodies referencing unselected relations, and
// column types whose type object was dropped. Only dependencies the selection
// dropped are diagnosed: a reference to an object that was never part of the
// same state behaves exactly as it does without a selection.
func validateGeneratedScope(original, final *goschema.Database, selection *scopeSelection) error {
	diagnostics := scopeDiagnostics{}
	keptByStruct := generatedTableByStruct(final.Tables)

	validateGeneratedForeignKeys(original, final, keptByStruct, diagnostics)
	validateGeneratedFunctions(original, final, diagnostics)
	validateGeneratedBodies(original, final, diagnostics)
	validateGeneratedTypes(original, final, selection, diagnostics)
	return diagnostics.err()
}

func validateGeneratedForeignKeys(
	original, final *goschema.Database,
	keptByStruct map[string]goschema.Table,
	diagnostics scopeDiagnostics,
) {
	for _, field := range original.Fields {
		owner, ok := keptByStruct[field.StructName]
		if !ok || field.Foreign == "" {
			continue
		}
		reportGeneratedTableDependency(original, final, owner, foreignReferenceTableName(field.Foreign), diagnostics)
	}
	for _, embedded := range original.EmbeddedFields {
		owner, ok := keptByStruct[embedded.StructName]
		if !ok || embedded.Mode != "relation" || embedded.Ref == "" {
			continue
		}
		reportGeneratedTableDependency(original, final, owner, foreignReferenceTableName(embedded.Ref), diagnostics)
	}
	for _, constraint := range original.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") || constraint.ForeignTable == "" {
			continue
		}
		owner, ok := generatedConstraintTable(keptByStruct, constraint)
		if !ok {
			continue
		}
		reportGeneratedTableDependency(original, final, owner, constraint.ForeignTable, diagnostics)
	}
}

// reportGeneratedTableDependency records a diagnostic when reference names a
// table that exists in the original schema but was dropped by the selection.
func reportGeneratedTableDependency(
	original, final *goschema.Database,
	owner goschema.Table,
	reference string,
	diagnostics scopeDiagnostics,
) {
	reference = strings.TrimSpace(reference)
	if reference == "" || generatedTableNameKept(final.Tables, reference) {
		return
	}
	for _, table := range original.Tables {
		if tableMatchesName(table, reference) {
			diagnostics.addf("table %q depends on table %q via a foreign key, but %q is not selected",
				owner.QualifiedName(), table.QualifiedName(), table.QualifiedName())
			return
		}
	}
}

func validateGeneratedFunctions(original, final *goschema.Database, diagnostics scopeDiagnostics) {
	keptFunctions := make(map[string]struct{}, len(final.Functions))
	for _, function := range final.Functions {
		keptFunctions[function.Name] = struct{}{}
	}
	originalFunctions := make(map[string]struct{}, len(original.Functions))
	for _, function := range original.Functions {
		originalFunctions[function.Name] = struct{}{}
	}
	for _, function := range final.Functions {
		for _, dependency := range original.FunctionDependencies[function.Name] {
			_, existed := originalFunctions[dependency]
			_, kept := keptFunctions[dependency]
			if existed && !kept {
				diagnostics.addf("function %q depends on function %q, but %q is not selected",
					function.Name, dependency, dependency)
			}
		}
	}
}

// validateGeneratedBodies checks view, materialized view, and trigger bodies
// of the selection for references to relations the selection dropped. Bodies
// are free SQL, so the check is a conservative identifier scan.
func validateGeneratedBodies(original, final *goschema.Database, diagnostics scopeDiagnostics) {
	dropped := droppedGeneratedRelations(original, final)
	for _, view := range final.Views {
		reportBodyReferences("view", view.Name, view.Body, dropped, diagnostics)
	}
	for _, view := range final.MaterializedViews {
		reportBodyReferences("materialized view", view.Name, view.Body, dropped, diagnostics)
	}
	for _, trigger := range final.Triggers {
		reportBodyReferences("trigger", trigger.Name, trigger.Body, dropped, diagnostics)
	}
}

// droppedGeneratedRelations maps display names of tables, views, and
// materialized views that the selection dropped, keyed by the bare name the
// identifier scan looks for.
func droppedGeneratedRelations(original, final *goschema.Database) map[string]string {
	kept := make(map[string]struct{})
	for _, table := range final.Tables {
		kept[table.Name] = struct{}{}
	}
	for _, view := range final.Views {
		_, name := splitQualified(view.Name)
		kept[name] = struct{}{}
	}
	for _, view := range final.MaterializedViews {
		_, name := splitQualified(view.Name)
		kept[name] = struct{}{}
	}

	dropped := make(map[string]string)
	record := func(bare, display string) {
		if _, ok := kept[bare]; !ok && bare != "" {
			dropped[bare] = display
		}
	}
	for _, table := range original.Tables {
		record(table.Name, table.QualifiedName())
	}
	for _, view := range original.Views {
		_, name := splitQualified(view.Name)
		record(name, view.Name)
	}
	for _, view := range original.MaterializedViews {
		_, name := splitQualified(view.Name)
		record(name, view.Name)
	}
	return dropped
}

func reportBodyReferences(
	kind, name, body string,
	dropped map[string]string,
	diagnostics scopeDiagnostics,
) {
	for bare, display := range dropped {
		if deporder.ReferencesIdentifier(body, bare) {
			diagnostics.addf("%s %q references %q, but %q is not selected", kind, name, display, display)
		}
	}
}

// validateGeneratedTypes refuses projections whose kept columns use a type
// object (enum, domain, composite type, range) that the selection dropped.
func validateGeneratedTypes(
	original, final *goschema.Database,
	selection *scopeSelection,
	diagnostics scopeDiagnostics,
) {
	referenced := generatedFieldTypeSet(final.Fields)
	report := func(kind, schema, name string) {
		if typeNameReferenced(referenced, schema, name) {
			display := dbschematypes.QualifyTableName(selection.effectiveSchema(schema), name)
			diagnostics.addf("selected tables use %s %q, but %q is not selected", kind, display, display)
		}
	}
	for _, enum := range original.Enums {
		if !generatedEnumKept(final.Enums, enum.Name) {
			report("enum", "", enum.Name)
		}
	}
	for _, domain := range original.Domains {
		if !typeObjectKept(final.Domains, domain.Schema, domain.Name, func(d goschema.Domain) (string, string) {
			return d.Schema, d.Name
		}) {
			report("domain", domain.Schema, domain.Name)
		}
	}
	for _, composite := range original.CompositeTypes {
		if !typeObjectKept(final.CompositeTypes, composite.Schema, composite.Name, func(c goschema.CompositeType) (string, string) {
			return c.Schema, c.Name
		}) {
			report("composite type", composite.Schema, composite.Name)
		}
	}
	for _, item := range original.Ranges {
		if !typeObjectKept(final.Ranges, item.Schema, item.Name, func(r goschema.Range) (string, string) {
			return r.Schema, r.Name
		}) {
			report("range type", item.Schema, item.Name)
		}
	}
}

func generatedEnumKept(enums []goschema.Enum, name string) bool {
	for _, enum := range enums {
		if enum.Name == name {
			return true
		}
	}
	return false
}

// typeObjectKept reports whether a type object identified by schema and name
// is still present in the kept slice.
func typeObjectKept[T any](kept []T, schema, name string, key func(T) (schema, name string)) bool {
	for _, item := range kept {
		keptSchema, keptName := key(item)
		if keptSchema == schema && keptName == name {
			return true
		}
	}
	return false
}

// validateDatabaseScope refuses projections where a selected introspected
// table has a foreign key to a table the selection dropped, or where kept
// columns use a type object the selection dropped.
func validateDatabaseScope(original, final *dbschematypes.DBSchema, selection *scopeSelection) error {
	diagnostics := scopeDiagnostics{}
	kept := make(map[tableIdentity]struct{}, len(final.Tables))
	for _, table := range final.Tables {
		kept[selection.tableIdentity(table.Schema, table.Name)] = struct{}{}
	}
	existed := make(map[tableIdentity]struct{}, len(original.Tables))
	for _, table := range original.Tables {
		existed[selection.tableIdentity(table.Schema, table.Name)] = struct{}{}
	}

	for _, constraint := range original.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		owner := selection.tableIdentity(constraint.Schema, constraint.TableName)
		foreign := selection.tableIdentity(foreignSchemaOrLocal(constraint), derefString(constraint.ForeignTable))
		if foreign.table == "" {
			continue
		}
		_, ownerKept := kept[owner]
		_, foreignKept := kept[foreign]
		_, foreignExisted := existed[foreign]
		if ownerKept && foreignExisted && !foreignKept {
			ownerName := dbschematypes.QualifyTableName(owner.schema, owner.table)
			foreignName := dbschematypes.QualifyTableName(foreign.schema, foreign.table)
			diagnostics.addf("table %q depends on table %q via a foreign key, but %q is not selected",
				ownerName, foreignName, foreignName)
		}
	}
	validateDatabaseTypes(original, final, selection, diagnostics)
	return diagnostics.err()
}

// validateDatabaseTypes refuses projections whose kept columns reference an
// introspected type object the selection dropped.
func validateDatabaseTypes(
	original, final *dbschematypes.DBSchema,
	selection *scopeSelection,
	diagnostics scopeDiagnostics,
) {
	referenced := databaseColumnTypeSet(final.Tables)
	report := func(kind, schema, name string) {
		if typeNameReferenced(referenced, schema, name) {
			display := dbschematypes.QualifyTableName(selection.effectiveSchema(schema), name)
			diagnostics.addf("selected tables use %s %q, but %q is not selected", kind, display, display)
		}
	}
	for _, enum := range original.Enums {
		if !databaseEnumKept(final.Enums, enum.Name) {
			report("enum", "", enum.Name)
		}
	}
	for _, domain := range original.Domains {
		if !typeObjectKept(final.Domains, domain.Schema, domain.Name, func(d dbschematypes.DBDomain) (string, string) {
			return d.Schema, d.Name
		}) {
			report("domain", domain.Schema, domain.Name)
		}
	}
}

func databaseEnumKept(enums []dbschematypes.DBEnum, name string) bool {
	for _, enum := range enums {
		if enum.Name == name {
			return true
		}
	}
	return false
}
