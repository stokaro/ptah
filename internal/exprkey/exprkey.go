// Package exprkey builds the keys the expression-resolution maps in package
// config are read by: [config.CompareOptions.CheckExpressions] and
// [config.CompareOptions.GeneratedExpressions].
//
// It exists because those keys were built in three places -- the resolver that
// fills each map and the comparison that reads it -- by three hand-written
// functions that each said, in their own doc comment, that they had to agree
// with the others. They agreed on a fold no target uses: `strings.ToLower`
// applied to the components joined with a dot.
//
// Two things were wrong with that, and they fail differently:
//
//   - The fold was hard-coded rather than the target's. PostgreSQL reports `t`
//     and `T` as two tables and each may carry a constraint of the same name,
//     because a constraint name is unique within its table. Lower-casing spells
//     one key for both, so the expression a server resolved for one table
//     answers the lookup for the other and a real difference on the second is
//     reported as no change -- silently, and on every run.
//   - The components were joined. `orders.2024` with constraint `c` and
//     `orders` with constraint `2024.c` are two objects that render as one
//     string under any separator.
//
// The identity itself is [objectidentity.Key], which is a struct precisely so
// that no component's content can forge a boundary. The maps above are
// `map[string]` on a public type, so the identity has to become a string
// somewhere; this package is that somewhere, and it does it once. Each
// component is written with its byte length in front, so the boundaries are
// read from the lengths and never from the content -- the property the struct
// key has, carried across.
//
// The strings are opaque and internal. Nothing parses them back, and no
// artifact contains them.
package exprkey

import (
	"strconv"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/objectidentity"
)

// Check is the key for a table CHECK whose table arrives as one possibly
// qualified string, which is how a declaration spells it.
func Check(semantics identifier.Semantics, qualifiedTable, constraint string) string {
	return encode(objectidentity.NewBuilder(semantics).Constraint(qualifiedTable, constraint))
}

// CheckParts is [Check] for a catalog, which reports the schema, the table and
// the constraint as three separate values and each one bare.
func CheckParts(semantics identifier.Semantics, schema, table, constraint string) string {
	return encode(objectidentity.NewBuilder(semantics).ConstraintParts(schema, table, constraint))
}

// Policy is the key for a row-level security policy whose table arrives as one
// possibly qualified string.
func Policy(semantics identifier.Semantics, qualifiedTable, policy string) string {
	return encode(objectidentity.NewBuilder(semantics).Policy(qualifiedTable, policy))
}

// PolicyParts is [Policy] for a catalog.
func PolicyParts(semantics identifier.Semantics, schema, table, policy string) string {
	return encode(objectidentity.NewBuilder(semantics).PolicyParts(schema, table, policy))
}

// Index is the key for an index expression whose owning table arrives as one
// possibly qualified string, which is how a declaration spells it.
//
// The table is carried rather than dropped because whether an index name is
// unique within its table or across the schema is the target's rule, and
// [objectidentity.Builder.Index] is where that rule lives.
func Index(semantics identifier.Semantics, qualifiedTable, index string) string {
	return encode(objectidentity.NewBuilder(semantics).Index(qualifiedTable, index))
}

// IndexParts is [Index] for a catalog, which reports the schema, the table and
// the index as three separate values.
func IndexParts(semantics identifier.Semantics, schema, table, index string) string {
	return encode(objectidentity.NewBuilder(semantics).IndexParts(schema, table, index))
}

// Generated is the key for one generated column, whose schema may be empty
// because the declaration did not qualify the table.
//
// It takes the DIALECT rather than resolved semantics, which the other families
// take, and the difference is not an oversight. The two sides of this key are
// built from two different connections: the map is filled by asking a DEV
// database to spell each declaration, and it is read while comparing against
// the TARGET. Semantics resolved from either connection can carry that
// server's catalog collation and its resolved names, so a key built from one
// would not be found by the other. The dialect is the part both sides agree on
// by construction.
func Generated(dialect, schema, table, column string) string {
	semantics := identifier.ForDialect(dialect)
	return encode(objectidentity.NewBuilder(semantics).ColumnParts(schema, table, column))
}

// Table is the key a probe-column map is held under, from a possibly qualified
// declaration spelling.
func Table(semantics identifier.Semantics, qualifiedTable string) string {
	return encode(objectidentity.NewBuilder(semantics).Table(qualifiedTable))
}

// TableParts is [Table] for a catalog, which reports the schema separately.
func TableParts(semantics identifier.Semantics, schema, table string) string {
	return encode(objectidentity.NewBuilder(semantics).TableParts(schema, table))
}

// encode renders an identity as an opaque string whose component boundaries
// come from the lengths written in front of each component, never from a
// separator a component could contain.
func encode(id objectidentity.ID) string {
	var b strings.Builder
	for _, part := range []string{
		string(id.Kind),
		id.Catalog.Normalized,
		id.Schema.Normalized,
		id.Parent.Normalized,
		id.Name.Normalized,
		id.Signature,
	} {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}
