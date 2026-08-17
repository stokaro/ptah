package objectidentity_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// TestBuilder_MatchesTheSemanticsKeysItReplaces is what makes migrating a
// consumer onto this model a refactor rather than a behavior change.
//
// Every per-family key in the tree folded through identifier.Semantics, and a
// model that folded even slightly differently -- unquoting first, trimming
// differently, defaulting a schema in another place -- would split two objects
// that used to be one, or merge two that were not. So the normalized component
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
