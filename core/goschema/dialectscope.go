package goschema

import (
	"slices"
	"sort"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/dialectscope"
)

// ScopeToDialect returns db projected onto dialect: every declared object whose
// `dialects=` scope excludes dialect is absent from the result.
//
// # Absent, not skipped and not refused
//
// A target that cannot host an object a schema declares used to leave Ptah two
// choices, and both are dishonest. Skipping the object with a named comment
// keeps a multi-dialect schema working but never converges: the comparator
// keeps the object in its added list, `schema apply` exits 0 having created
// nothing, and the next run plans the same creation forever. Refusing the
// object converges but makes one schema across postgres, mysql and mariadb
// impossible. Measured on MariaDB 12.3.2 before this existed, one schema
// declaring a plpgsql function, an extension, a sequence, a domain and an RLS
// policy applied cleanly (exit 0) and then reported four permanently
// unreconcilable categories on every later comparison, while a declared role
// refused the apply outright.
//
// A scope makes the third answer expressible. The object is simply not part of
// the desired state for a target it was not declared for, so nothing compares
// it, nothing plans it, and nothing has to apologize for it.
//
// # An empty scope belongs to every dialect
//
// Objects declared before the attribute existed carry no scope, and they must
// keep reaching every target. The projection can therefore only narrow a
// schema, never widen one: with no scope anywhere, ScopeToDialect returns a
// database equal to its input.
//
// # The JSON tag on every Dialects field
//
// [Database]'s JSON encoding is the desired-state fingerprint that plan files
// record and verify. A `Dialects` field without `json:",omitempty"` would
// encode as `null` on every object of every schema and change the fingerprint
// of every plan anyone has already saved. The tag is what keeps an unscoped
// schema encoding exactly as it did before this field existed.
func ScopeToDialect(db *Database, dialect string) *Database {
	if db == nil {
		return nil
	}
	if platform.NormalizeDialect(dialect) == "" {
		// An unrecognized target is refused by the renderer and by the
		// connection with a message that names what is wrong. Projecting it
		// first would empty the desired state and report a synced schema
		// instead.
		return db
	}
	if !hasDialectScope(db) {
		// Nothing is scoped, so the projection is the identity. Returning the
		// original pointer keeps an unscoped schema out of the clone-and-
		// finalize path entirely, which is where every behavior difference
		// between a scoped and an unscoped run could otherwise creep in.
		return db
	}

	scoped := *db
	scoped.Extensions = keepScoped(db.Extensions, dialect, func(v Extension) []string { return v.Dialects })
	scoped.Functions = keepScoped(db.Functions, dialect, func(v Function) []string { return v.Dialects })
	scoped.Sequences = keepScoped(db.Sequences, dialect, func(v Sequence) []string { return v.Dialects })
	scoped.Domains = keepScoped(db.Domains, dialect, func(v Domain) []string { return v.Dialects })
	scoped.CompositeTypes = keepScoped(db.CompositeTypes, dialect, func(v CompositeType) []string { return v.Dialects })
	scoped.Ranges = keepScoped(db.Ranges, dialect, func(v Range) []string { return v.Dialects })
	scoped.Views = keepScoped(db.Views, dialect, func(v View) []string { return v.Dialects })
	scoped.MaterializedViews = keepScoped(db.MaterializedViews, dialect, func(v MaterializedView) []string { return v.Dialects })
	scoped.Triggers = keepScoped(db.Triggers, dialect, func(v Trigger) []string { return v.Dialects })
	scoped.RLSPolicies = keepScoped(db.RLSPolicies, dialect, func(v RLSPolicy) []string { return v.Dialects })
	scoped.RLSEnabledTables = keepScoped(db.RLSEnabledTables, dialect, func(v RLSEnabledTable) []string { return v.Dialects })
	scoped.Roles = keepScoped(db.Roles, dialect, func(v Role) []string { return v.Dialects })
	scoped.Grants = keepScoped(db.Grants, dialect, func(v Grant) []string { return v.Dialects })

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
	return &scoped
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

// OmissionsForDialect names every object ScopeToDialect would remove from db
// for dialect, in the same order as [ScopedObjects].
//
// It answers the question a projection alone cannot: an object that is absent
// looks exactly like an object that was never declared. Reporting the omission
// is what turns "this target quietly does nothing with your declaration" into a
// statement the author wrote on purpose.
func OmissionsForDialect(db *Database, dialect string) []ScopedObject {
	if platform.NormalizeDialect(dialect) == "" {
		return nil
	}
	return collectScopedObjects(db, func(scope []string) bool {
		return !dialectscope.Includes(scope, dialect)
	})
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
		anyScoped(db.Grants, func(v Grant) []string { return v.Dialects })
}

func anyScoped[T any](values []T, scopeOf func(T) []string) bool {
	for _, value := range values {
		if len(scopeOf(value)) > 0 {
			return true
		}
	}
	return false
}

func keepScoped[T any](values []T, dialect string, scopeOf func(T) []string) []T {
	kept := make([]T, 0, len(values))
	for _, value := range values {
		if dialectscope.Includes(scopeOf(value), dialect) {
			kept = append(kept, value)
		}
	}
	return kept
}
