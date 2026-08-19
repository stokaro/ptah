package schemadiff

import (
	"slices"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
)

// suppressScopedAway removes from the current schema every object a scope kept
// out of the desired one.
//
// The projection alone is only half the guarantee. It removes the declaration,
// and the database is left still holding the object, so the comparison reads it
// as present in the target and absent from the declaration -- which is exactly
// the shape of a drop. A function declared `dialects="mysql"` that already
// exists in a PostgreSQL target lands in FunctionsRemoved, and `schema apply`
// plans a DROP FUNCTION for an object the feature promised not to compare.
//
// The scope says the declaration does not describe that target. It does not say
// the target should not have the object. So an object outside the scope is not
// compared in either direction, and an object no declaration mentions at all is
// still an ordinary removal -- which is what keeps this from being read as
// "never drop anything".
//
// The current schema is cloned rather than filtered in place. Callers hand over
// a snapshot they may still be holding, and a comparison is not entitled to
// edit it.
func suppressScopedAway(current *types.DBSchema, omitted []goschema.ScopedObject) *types.DBSchema {
	if current == nil || len(omitted) == 0 {
		return current
	}

	names := make(map[string]map[string]bool, len(omitted))
	for _, object := range omitted {
		if names[object.Kind] == nil {
			names[object.Kind] = make(map[string]bool)
		}
		names[object.Kind][object.Name] = true
	}

	filtered := *current
	filtered.Extensions = keepUnscoped(current.Extensions, names["extension"], func(v types.DBExtension) (string, string) { return v.Schema, v.Name })
	filtered.Functions = keepUnscoped(current.Functions, names["function"], func(v types.DBFunction) (string, string) { return v.Schema, v.Name })
	filtered.Sequences = keepUnscoped(current.Sequences, names["sequence"], func(v types.DBSequence) (string, string) { return v.Schema, v.Name })
	filtered.Domains = keepUnscoped(current.Domains, names["domain"], func(v types.DBDomain) (string, string) { return v.Schema, v.Name })
	filtered.Composites = keepUnscoped(current.Composites, names["composite"], func(v types.DBComposite) (string, string) { return v.Schema, v.Name })
	filtered.Ranges = keepUnscoped(current.Ranges, names["range"], func(v types.DBRange) (string, string) { return v.Schema, v.Name })
	filtered.Views = keepUnscoped(current.Views, names["view"], func(v types.DBView) (string, string) { return v.Schema, v.Name })
	filtered.MatViews = keepUnscoped(current.MatViews, names["matview"], func(v types.DBMatView) (string, string) { return v.Schema, v.Name })
	filtered.Triggers = keepUnscoped(current.Triggers, names["trigger"], func(v types.DBTrigger) (string, string) { return v.Schema, v.Name })
	filtered.Roles = keepUnscoped(current.Roles, names["role"], func(v types.DBRole) (string, string) { return "", v.Name })
	filtered.Grants = keepUnscoped(current.Grants, names["grant"], func(v types.DBGrant) (string, string) { return "", v.Role })
	return &filtered
}

// keepUnscoped returns the values whose identity is not among the scoped-away
// names, sharing the original slice when nothing is dropped.
func keepUnscoped[T any](values []T, scopedAway map[string]bool, identityOf func(T) (schema, name string)) []T {
	if len(scopedAway) == 0 || len(values) == 0 {
		return values
	}
	kept := make([]T, 0, len(values))
	for _, value := range values {
		schema, name := identityOf(value)
		if !matchesScopedAway(schema, name, scopedAway) {
			kept = append(kept, value)
		}
	}
	if len(kept) == len(values) {
		return values
	}
	return slices.Clip(kept)
}

// matchesScopedAway reports whether an object identifies one that was scoped
// away, across the two spellings the sides use for one object.
//
// The declaration spells a sequence, domain, composite or range as schema.name,
// while a reader blanks the schema for the connection's own -- so comparing the
// strings whole would suppress nothing for exactly the objects most likely to
// be present. The unqualified halves must match, and a qualifier only has to
// agree when both sides carry one. Matching on the tail alone would let a
// scoped-away app.seq suppress an unrelated other.seq, which is a drop this
// function exists to prevent going missing.
func matchesScopedAway(schema, name string, scopedAway map[string]bool) bool {
	for away := range scopedAway {
		awaySchema, awayName := splitQualified(away)
		if awayName != name {
			continue
		}
		if awaySchema == "" || schema == "" || awaySchema == schema {
			return true
		}
	}
	return false
}

// splitQualified separates a schema.name spelling into its two halves, leaving
// the schema empty when the name carries no qualifier.
func splitQualified(qualified string) (schema, name string) {
	for i := len(qualified) - 1; i >= 0; i-- {
		if qualified[i] == '.' {
			return qualified[:i], qualified[i+1:]
		}
	}
	return "", qualified
}
