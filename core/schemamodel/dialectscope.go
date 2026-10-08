package schemamodel

import (
	"slices"
	"sort"

	"ptah.run/core/schemaext"
)

// ScopeToTarget projects db onto an explicitly resolved target. Every declared
// object whose `dialects=` scope excludes that target is absent from the result.
// An unresolved target is an error, never an empty or unchanged declaration.
//
// An excluded declaration does not describe this target. Comparison must also
// suppress the corresponding observed identity rather than treating exclusion
// as deletion intent. An empty declaration scope includes every resolved target.
// The result retains unfiltered nested values as read-only data. Filtered slices
// and derived dependency graphs belong to the result; the input is unchanged.
func ScopeToTarget(db *Database, target schemaext.TargetSelection) (*Database, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	if db == nil {
		return nil, nil
	}
	if !hasDialectScope(db) {
		// Nothing is scoped, so the projection is the identity. Returning the
		// original pointer keeps an unscoped schema out of the clone-and-
		// finalize path entirely, which is where every behavior difference
		// between a scoped and an unscoped run could otherwise creep in.
		return db, nil
	}

	scoped := *db
	scoped.Extensions = keepScoped(db.Extensions, target, func(v Extension) []string { return v.Dialects })
	scoped.Functions = keepScoped(db.Functions, target, func(v Function) []string { return v.Dialects })
	scoped.Sequences = keepScoped(db.Sequences, target, func(v Sequence) []string { return v.Dialects })
	scoped.Domains = keepScoped(db.Domains, target, func(v Domain) []string { return v.Dialects })
	scoped.CompositeTypes = keepScoped(db.CompositeTypes, target, func(v CompositeType) []string { return v.Dialects })
	scoped.Ranges = keepScoped(db.Ranges, target, func(v Range) []string { return v.Dialects })
	scoped.Views = keepScoped(db.Views, target, func(v View) []string { return v.Dialects })
	scoped.MaterializedViews = keepScoped(db.MaterializedViews, target, func(v MaterializedView) []string { return v.Dialects })
	scoped.Triggers = keepScoped(db.Triggers, target, func(v Trigger) []string { return v.Dialects })
	scoped.RLSPolicies = keepScoped(db.RLSPolicies, target, func(v RLSPolicy) []string { return v.Dialects })
	scoped.RLSEnabledTables = keepScoped(db.RLSEnabledTables, target, func(v RLSEnabledTable) []string { return v.Dialects })
	scoped.Roles = keepScoped(db.Roles, target, func(v Role) []string { return v.Dialects })
	scoped.Grants = keepScoped(db.Grants, target, func(v Grant) []string { return v.Dialects })
	scoped.DefaultPrivileges = keepScoped(db.DefaultPrivileges, target, func(v DefaultPrivilege) []string { return v.Dialects })
	scoped.RevokedGrants = keepScoped(db.RevokedGrants, target, func(v Grant) []string { return v.Dialects })

	// Everything the projection does not filter is still shared with the
	// caller's database by value, so the slices it can reorder are cloned
	// before Finalize runs over them.
	scoped.Tables = slices.Clone(db.Tables)
	scoped.Fields = slices.Clone(db.Fields)
	scoped.Indexes = slices.Clone(db.Indexes)
	scoped.Constraints = slices.Clone(db.Constraints)
	scoped.Enums = slices.Clone(db.Enums)
	scoped.EmbeddedFields = slices.Clone(db.EmbeddedFields)
	scoped.Schemas = slices.Clone(db.Schemas)

	// The derived graphs name objects the projection may have removed, so they
	// are dropped and recomputed rather than carried across. This is the same
	// discipline the exclude filter follows for the same reason: a dependency
	// edge pointing at an object that is no longer there orders a creation that
	// never happens.
	scoped.Dependencies = nil
	scoped.FunctionDependencies = nil
	scoped.SelfReferencingForeignKeys = nil
	Finalize(&scoped)
	return &scoped, nil
}

// ScopedObject names one declared object and the dialect scope it carries.
type ScopedObject struct {
	// Kind is the directive-facing object kind, such as "function" or "role".
	Kind string
	// Name identifies the object within its kind, as the declaration spells it.
	Name string
	// Dialects is the canonical scope the declaration carries.
	Dialects []string
}

// ScopedObjects returns every object in db that carries a `dialects=` scope,
// sorted by kind and then name so anything built from it is stable across runs.
//
// Callers that must not silently drop a scope use this: the HCL exporter has no
// place to write one, and reporting each scoped object as a loss is what makes
// destructive annotation cleanup refuse rather than delete the only place the
// scope was written down.
func ScopedObjects(db *Database) []ScopedObject {
	return collectScopedObjects(db, func([]string) bool { return true })
}

// OmissionsForTarget names every object ScopeToTarget would remove from db
// for the selected target, in the same order as [ScopedObjects].
//
// It answers the question a projection alone cannot: an object that is absent
// looks exactly like an object that was never declared. Reporting the omission
// is what turns "this target quietly does nothing with your declaration" into a
// statement the author wrote on purpose.
func OmissionsForTarget(db *Database, target schemaext.TargetSelection) ([]ScopedObject, error) {
	if err := target.Validate(); err != nil {
		return nil, err
	}
	return collectScopedObjects(db, func(scope []string) bool {
		return !target.Includes(scope)
	}), nil
}

func collectScopedObjects(db *Database, want func(scope []string) bool) []ScopedObject {
	if db == nil {
		return nil
	}
	var found []ScopedObject
	collect := func(kind, name string, scope []string) {
		if len(scope) == 0 || !want(scope) {
			return
		}
		found = append(found, ScopedObject{
			Kind:     kind,
			Name:     name,
			Dialects: slices.Clone(scope),
		})
	}
	for _, v := range db.Extensions {
		collect("extension", v.Name, v.Dialects)
	}
	for _, v := range db.Functions {
		collect("function", v.Name, v.Dialects)
	}
	for _, v := range db.Sequences {
		collect("sequence", v.QualifiedName(), v.Dialects)
	}
	for _, v := range db.Domains {
		collect("domain", v.QualifiedName(), v.Dialects)
	}
	for _, v := range db.CompositeTypes {
		collect("composite", v.QualifiedName(), v.Dialects)
	}
	for _, v := range db.Ranges {
		collect("range", v.QualifiedName(), v.Dialects)
	}
	for _, v := range db.Views {
		collect("view", v.Name, v.Dialects)
	}
	for _, v := range db.MaterializedViews {
		collect("matview", v.Name, v.Dialects)
	}
	for _, v := range db.Triggers {
		collect("trigger", v.Name, v.Dialects)
	}
	for _, v := range db.RLSPolicies {
		collect("rls policy", v.Name, v.Dialects)
	}
	for _, v := range db.RLSEnabledTables {
		collect("rls enable", v.Table, v.Dialects)
	}
	for _, v := range db.Roles {
		collect("role", v.Name, v.Dialects)
	}
	for _, v := range db.Grants {
		collect("grant", v.Role, v.Dialects)
	}
	for _, v := range db.RevokedGrants {
		collect("revoked grant", v.Role, v.Dialects)
	}
	for _, v := range db.DefaultPrivileges {
		// The collected name has to separate two declarations that differ only
		// in grantor or object type, because the suppression that consumes it
		// keeps an object by name alone.
		collect("default privilege", defaultPrivilegeScopeName(v), v.Dialects)
	}
	sort.SliceStable(found, func(i, j int) bool {
		if found[i].Kind != found[j].Kind {
			return found[i].Kind < found[j].Kind
		}
		return found[i].Name < found[j].Name
	})
	return found
}

// hasDialectScope reports whether any object in db carries a scope at all.
func hasDialectScope(db *Database) bool {
	return anyScoped(db.Extensions, func(v Extension) []string { return v.Dialects }) ||
		anyScoped(db.Functions, func(v Function) []string { return v.Dialects }) ||
		anyScoped(db.Sequences, func(v Sequence) []string { return v.Dialects }) ||
		anyScoped(db.Domains, func(v Domain) []string { return v.Dialects }) ||
		anyScoped(db.CompositeTypes, func(v CompositeType) []string { return v.Dialects }) ||
		anyScoped(db.Ranges, func(v Range) []string { return v.Dialects }) ||
		anyScoped(db.Views, func(v View) []string { return v.Dialects }) ||
		anyScoped(db.MaterializedViews, func(v MaterializedView) []string { return v.Dialects }) ||
		anyScoped(db.Triggers, func(v Trigger) []string { return v.Dialects }) ||
		anyScoped(db.RLSPolicies, func(v RLSPolicy) []string { return v.Dialects }) ||
		anyScoped(db.RLSEnabledTables, func(v RLSEnabledTable) []string { return v.Dialects }) ||
		anyScoped(db.Roles, func(v Role) []string { return v.Dialects }) ||
		anyScoped(db.Grants, func(v Grant) []string { return v.Dialects }) ||
		anyScoped(db.DefaultPrivileges, func(v DefaultPrivilege) []string { return v.Dialects }) ||
		anyScoped(db.RevokedGrants, func(v Grant) []string { return v.Dialects })
}

func anyScoped[T any](values []T, scopeOf func(T) []string) bool {
	for _, value := range values {
		if len(scopeOf(value)) > 0 {
			return true
		}
	}
	return false
}

func keepScoped[T any](values []T, target schemaext.TargetSelection, scopeOf func(T) []string) []T {
	kept := make([]T, 0, len(values))
	for _, value := range values {
		if target.Includes(scopeOf(value)) {
			kept = append(kept, value)
		}
	}
	return kept
}

// defaultPrivilegeScopeName names one default privilege for the scope report.
//
// The four components are the object's identity. A bare grantee would make two
// declarations differing only in object type one name, and the suppression that
// reads this report keeps an object by name alone -- so one of them would be
// suppressed on behalf of the other.
func defaultPrivilegeScopeName(d DefaultPrivilege) string {
	return d.ObjectType + " in " + d.Schema + " for " + d.Grantor + " to " + d.Grantee
}
