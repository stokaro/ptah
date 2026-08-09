package fromschema

import (
	"maps"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
)

// QualifyDeclaredUserTypes returns a clone of database whose column types name
// the schema of the user type the same schema declares.
//
// A user type is created in a schema, and a column declared against it has to
// name the same one: PostgreSQL resolves a bare type name through search_path,
// and a plan that touches more than one schema cannot have the right
// search_path for all of them. Measured on PostgreSQL 17.10, one schema
// `wf1138s` holding one user type of each kind, inspected and planned back into
// a fresh database:
//
//	CREATE TYPE "wf1138s"."mood" AS ENUM ('sad', 'ok', 'happy');
//	CREATE TABLE "wf1138s"."t" (..., "c6" mood[], ...);
//	  -> ERROR:  type "mood[]" does not exist
//
// [handleEnumTypes] already does this for the SCALAR spelling of an enum, and
// carries the same measurement for `CREATE TYPE "extra"."mood"` followed by a
// bare `mood` (stokaro/ptah#1276). That repair reached one of the eight cells
// this boundary has -- four user-type kinds (enum, domain, composite, range)
// times two spellings (scalar and array) -- because [declaredEnum] matches a
// declared name exactly and `mood[]` is not `mood`. This closes the other seven
// (stokaro/ptah#1138).
//
// It runs over the whole schema rather than inside [FromField] because a column
// does not carry its type's schema; only the declaration does. Both callers are
// AST-generation entry points, never the comparison in
// [go.5x5.cz/ptah/migration/schemadiff]: a database read reports the column type
// as the catalog spells it, so qualifying the generated side before a comparison
// would invent a difference rather than remove one.
//
// Names deliberately left alone:
//
//   - a type that already carries a qualifier, which the author spelled
//   - a type whose bare name is declared more than once, where nothing in the
//     column says which was meant and guessing would type it against the wrong
//     declaration
//   - the SCALAR spelling of an enum, because [declaredEnum] is the only test
//     for "is this column an enum" and it matches the bare name; rewriting it
//     here would hide the enum from the standalone-versus-inline decision
//     [handleEnumTypes] makes
//   - every enum on a dialect that models an enum on the column instead of as a
//     standalone type, for the same reason
func QualifyDeclaredUserTypes(database *goschema.Database, targetPlatform string) *goschema.Database {
	if database == nil {
		return nil
	}
	clone := *database
	scalars, arrays := declaredUserTypeQualifiers(database, targetPlatform)
	if len(scalars) == 0 && len(arrays) == 0 {
		return &clone
	}
	clone.Fields = make([]goschema.Field, len(database.Fields))
	copy(clone.Fields, database.Fields)
	for i := range clone.Fields {
		clone.Fields[i].Type = qualifyUserTypeReference(clone.Fields[i].Type, scalars, arrays)
	}
	return &clone
}

// declaredUserTypeQualifiers maps the bare name of every unambiguously declared
// user type to its qualified spelling, once for the scalar spelling and once for
// the array spelling.
//
// The two maps differ only in the enums, which the array map carries and the
// scalar map does not; see [QualifyDeclaredUserTypes] for why. A name declared
// twice is mapped to the empty string rather than dropped, so that a later
// declaration of the same name cannot re-add it.
func declaredUserTypeQualifiers(
	database *goschema.Database,
	targetPlatform string,
) (scalars, arrays map[string]string) {
	declare := func(into map[string]string, name, schema, qualified string) {
		name = strings.TrimSpace(name)
		if name == "" || strings.TrimSpace(schema) == "" {
			return
		}
		if _, seen := into[name]; seen {
			into[name] = ""
			return
		}
		into[name] = qualified
	}
	base := make(map[string]string)
	for _, domain := range database.Domains {
		declare(base, domain.Name, domain.Schema, domain.QualifiedName())
	}
	for _, composite := range database.CompositeTypes {
		declare(base, composite.Name, composite.Schema, composite.QualifiedName())
	}
	for _, rangeType := range database.Ranges {
		declare(base, rangeType.Name, rangeType.Schema, rangeType.QualifiedName())
	}

	scalars = make(map[string]string, len(base))
	maps.Copy(scalars, base)
	// An enum name never participates in the scalar map, even when no enum
	// block claims a schema: a scalar column typed with it belongs to
	// [handleEnumTypes], which finds it by the bare name.
	for _, enum := range database.Enums {
		if name := strings.TrimSpace(enum.Name); name != "" {
			scalars[name] = ""
		}
	}
	if !emitsStandaloneEnumDefinitions(targetPlatform) {
		return scalars, scalars
	}

	arrays = make(map[string]string, len(base)+len(database.Enums))
	maps.Copy(arrays, base)
	for _, enum := range database.Enums {
		name := strings.TrimSpace(enum.Name)
		if strings.TrimSpace(enum.Schema) == "" {
			// A schemaless enum still shadows a same-named declaration of
			// another kind, because `mood[]` where both answer to `mood` names
			// neither unambiguously.
			if name != "" {
				arrays[name] = ""
			}
			continue
		}
		declare(arrays, name, enum.Schema, enum.QualifiedName())
	}
	return scalars, arrays
}

// qualifyUserTypeReference rewrites a column type that names a declared user
// type, keeping any array brackets.
func qualifyUserTypeReference(columnType string, scalars, arrays map[string]string) string {
	trimmed := strings.TrimSpace(columnType)
	name, brackets := splitArraySuffix(trimmed)
	if name == "" || strings.Contains(name, ".") {
		return columnType
	}
	qualifiers := scalars
	if brackets != "" {
		qualifiers = arrays
	}
	qualified := qualifiers[name]
	if qualified == "" {
		return columnType
	}
	return qualified + brackets
}

// splitArraySuffix splits a column type into the type it is an array of and the
// bracket run that says so, returning an empty suffix for a scalar.
//
// Every bracket group PostgreSQL accepts is consumed, empty or dimensioned, so
// `mood[]`, `mood[3]` and `mood[][1]` all reduce to `mood`. The dimensions are
// kept verbatim rather than normalized: PostgreSQL discards them itself, and
// rewriting them here would change a spelling this function has no reason to
// touch.
func splitArraySuffix(columnType string) (name, brackets string) {
	end := len(columnType)
	for end > 0 && columnType[end-1] == ']' {
		open := strings.LastIndexByte(columnType[:end-1], '[')
		if open < 0 {
			break
		}
		if !isArrayDimension(columnType[open+1 : end-1]) {
			break
		}
		end = open
	}
	return strings.TrimRight(columnType[:end], " "), columnType[end:]
}

// isArrayDimension reports whether the text between one pair of brackets is an
// array dimension rather than part of the type itself.
func isArrayDimension(text string) bool {
	for _, r := range strings.TrimSpace(text) {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
