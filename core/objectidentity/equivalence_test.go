package objectidentity_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
)

// TestBuilder_MatchesTheSemanticsKeysItReplaces is what makes migrating a
// consumer onto this model a refactor rather than a behavior change.
//
// Every per-family key in the tree folded through identifier.Semantics, and a
// model that folds even slightly differently -- unquoting first, trimming
// differently, defaulting a schema in another place -- splits two objects those
// keys hold together, or merges two they keep apart. So the normalized
// component
// is asserted to equal the Semantics call it stands in for, over the spellings
// that make those choices visible.
func TestBuilder_MatchesTheSemanticsKeysItReplaces(t *testing.T) {
	semanticsByName := map[string]identifier.Semantics{
		"folding": postgresSemantics(),
		"exact": {
			DefaultSchema: "main",
			TableNames:    identifier.ComparisonExact,
			ColumnNames:   identifier.ComparisonExact,
			IndexNames:    identifier.ComparisonExact,
		},
	}
	spellings := []string{"users", "Users", `"Users"`, "USERS", `"users"`, "Ünïcode", " padded "}

	for name, semantics := range semanticsByName {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			builder := objectidentity.NewBuilder(semantics)

			for _, spelling := range spellings {
				table := builder.Table("public." + spelling)
				c.Assert(table.Name.Normalized, qt.Equals, semantics.TableIdentityKey(trimmed(spelling)),
					qt.Commentf("table spelling %q", spelling))

				column := builder.Column("public.t", spelling)
				c.Assert(column.Name.Normalized, qt.Equals, semantics.ColumnIdentityKey(trimmed(spelling)),
					qt.Commentf("column spelling %q", spelling))

				index := builder.Index("public.t", spelling)
				c.Assert(index.Name.Normalized, qt.Equals, semantics.IndexIdentityKey(trimmed(spelling)),
					qt.Commentf("index spelling %q", spelling))

				// A table's name given as a component keeps its bytes.
				parts := builder.TableParts("public", spelling)
				c.Assert(parts.Name.Normalized, qt.Equals, semantics.TableIdentityKey(spelling),
					qt.Commentf("table parts spelling %q", spelling))
			}
		})
	}
}

// TestBuilder_DefaultsTheSchemaLikeQualifiedTableIdentityKey pins the other
// half: an unqualified name takes the default schema, in the same place and by
// the same rule as the Semantics helper that already did it.
func TestBuilder_DefaultsTheSchemaLikeQualifiedTableIdentityKey(t *testing.T) {
	c := qt.New(t)
	semantics := postgresSemantics()
	builder := objectidentity.NewBuilder(semantics)

	unqualified := builder.Table("orders")
	qualified := builder.Table("public.orders")

	c.Assert(unqualified.Schema.Normalized, qt.Equals, semantics.TableIdentityKey("public"))
	c.Assert(unqualified.Equal(qualified), qt.IsTrue)
}

// trimmed mirrors the trimming the builder applies before folding, so the
// expectation is about the FOLDING rule rather than about whitespace.
func trimmed(value string) string {
	return strings.TrimSpace(value)
}

// A table's name given as a component keeps its bytes, and so does the table
// a column, an index, a constraint, a policy or a grant given as components
// belongs to: a catalog holds " t" and t as two tables. The name of every
// other object is trimmed, and so is a part split off one qualified string.
func TestBuilder_KeepsTheBytesOfATableNameOnly(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())
	tests := []struct {
		name string
		id   objectidentity.ID
		want string
	}{
		{name: "table parts", id: builder.TableParts("public", " t"), want: " t"},
		{name: "verbatim table parts", id: builder.TablePartsVerbatim("public", " t"), want: " t"},
		{name: "a split table", id: builder.Table("public. t"), want: "t"},
		{name: "column", id: builder.ColumnParts("public", "t", " c"), want: "c"},
		{name: "a column of a split table", id: builder.Column("public.t", " c"), want: "c"},
		{name: "index", id: builder.IndexParts("public", "t", " i"), want: "i"},
		{name: "constraint", id: builder.ConstraintParts("public", "t", " k"), want: "k"},
		{name: "check constraint", id: builder.CheckConstraintParts("public", "t", " k", "x > 0"), want: "k"},
		{name: "policy", id: builder.PolicyParts("public", "t", " p"), want: "p"},
		{name: "trigger", id: builder.Trigger("public.t", " g"), want: "g"},
		{name: "grant", id: builder.GrantParts("public", "t", " r", "SELECT"), want: "r"},
		{name: "role", id: builder.Role(" r"), want: "r"},
		{name: "sequence", id: builder.SchemaScopedParts(objectidentity.KindSequence, "public", " s"), want: "s"},
		{name: "domain", id: builder.Domain("public. d"), want: "d"},
		{name: "function", id: builder.Function("public. f", ""), want: "f"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.id.Name.Source, qt.Equals, test.want)
		})
	}
}

// The owner of an object given as components keeps the bytes of the table's
// name, so a column of " t" is not a column of t.
func TestBuilder_KeepsTheBytesOfTheOwningTable(t *testing.T) {
	builder := objectidentity.NewBuilder(postgresSemantics())
	tests := []struct {
		name string
		id   objectidentity.ID
	}{
		{name: "column", id: builder.ColumnParts("public", " t", "c")},
		{name: "index", id: builder.IndexParts("public", " t", "i")},
		{name: "constraint", id: builder.ConstraintParts("public", " t", "k")},
		{name: "policy", id: builder.PolicyParts("public", " t", "p")},
		{name: "grant", id: builder.GrantParts("public", " t", "r", "SELECT")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.id.Parent.Source, qt.Equals, " t")
		})
	}
}
