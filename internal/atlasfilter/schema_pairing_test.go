package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/atlasfilter"
)

func schemaName(schema catalog.Schema) string { return schema.Name }

func schemasNamed(names ...string) []catalog.Schema {
	schemas := make([]catalog.Schema, 0, len(names))
	for _, name := range names {
		schemas = append(schemas, catalog.Schema{Name: name})
	}
	return schemas
}

func TestPairSchemas_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		all, kept []catalog.Schema
		other     []string
		want      []catalog.Schema
	}{
		{
			name: "restores a schema the other side kept",
			all:  schemasNamed("app", "db", "old"), kept: schemasNamed("app"), other: []string{"app", "db"},
			want: schemasNamed("app", "db"),
		},
		{
			name: "leaves out a schema neither side kept",
			all:  schemasNamed("app", "db", "old"), kept: schemasNamed("app"), other: []string{"app"},
			want: schemasNamed("app"),
		},
		{
			name: "restores nothing the side does not have",
			all:  schemasNamed("app"), kept: schemasNamed("app"), other: []string{"app", "db"},
			want: schemasNamed("app"),
		},
		{
			name: "lists the schemas in the order of the side",
			all:  schemasNamed("z", "app", "db"), kept: schemasNamed("app"), other: []string{"db", "z"},
			want: schemasNamed("z", "app", "db"),
		},
		{
			name: "keeps the projected entry of a kept schema",
			all:  []catalog.Schema{{Name: "app", Comment: "read"}, {Name: "db"}}, kept: []catalog.Schema{{Name: "app", Comment: "projected"}},
			other: []string{"db"},
			want:  []catalog.Schema{{Name: "app", Comment: "projected"}, {Name: "db"}},
		},
		{
			name: "keeps a kept schema the side did not list",
			all:  schemasNamed("db"), kept: schemasNamed("support"), other: []string{"db"},
			want: schemasNamed("db", "support"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasfilter.PairSchemas(test.all, test.kept, test.other, schemaName)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// With nothing to restore, the side's own slice comes back, so a projection
// that kept every schema is not copied.
func TestPairSchemas_ReturnsKeptWhenNothingIsRestored(t *testing.T) {
	c := qt.New(t)
	kept := schemasNamed("app")

	got := atlasfilter.PairSchemas(schemasNamed("app", "db"), kept, []string{"app"}, schemaName)

	c.Assert(got, qt.HasLen, 1)
	c.Assert(&got[0], qt.Equals, &kept[0])
}

// A restore builds a new slice, so appending to the result never writes into
// the backing array the projection shares with its read.
func TestPairSchemas_DoesNotWriteIntoTheKeptSlice(t *testing.T) {
	c := qt.New(t)
	backing := schemasNamed("app", "spare")
	kept := backing[:1]

	got := atlasfilter.PairSchemas(schemasNamed("app", "db"), kept, []string{"db"}, schemaName)

	c.Assert(got, qt.DeepEquals, schemasNamed("app", "db"))
	c.Assert(backing, qt.DeepEquals, schemasNamed("app", "spare"))
}
