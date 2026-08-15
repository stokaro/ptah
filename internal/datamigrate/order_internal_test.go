package datamigrate

// White-box testing required: exercises orderByDependency, the package-local
// helper that maps managed-data diffs to the schema's dependency-sorted tables.
// It has no exported entry point, and the mapping edge cases (bare-name fallback,
// ambiguity guard) are not observable from the black-box Generate output.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/datadiff"
)

// TestOrderByDependency covers how managed-data diffs are matched to the
// schema's dependency-sorted tables: by qualified name, by a bare-name fallback
// when the data annotation omits the schema, and the ambiguity guard that
// refuses to guess when a bare name is shared across schemas.
func TestOrderByDependency(t *testing.T) {
	names := func(diffs []*datadiff.DataDiff) []string {
		out := make([]string, len(diffs))
		for i, d := range diffs {
			out[i] = qualifiedName(d.Schema, d.Table)
		}
		return out
	}

	// db.Tables is already dependency-sorted parents-first (authors before
	// articles); note "articles" < "authors" alphabetically, so any path that
	// falls back to alphabetical order would wrongly put the child first.
	depSorted := func() *goschema.Database {
		return &goschema.Database{Tables: []goschema.Table{
			{Name: "authors", Schema: "app"},
			{Name: "articles", Schema: "app"},
		}}
	}

	t.Run("qualified match follows dependency order", func(t *testing.T) {
		c := qt.New(t)
		diffs := []*datadiff.DataDiff{
			{Schema: "app", Table: "articles"},
			{Schema: "app", Table: "authors"},
		}
		orderByDependency(depSorted(), diffs)
		c.Assert(names(diffs), qt.DeepEquals, []string{"app.authors", "app.articles"})
	})

	t.Run("bare-name fallback when the data annotation omits the schema", func(t *testing.T) {
		c := qt.New(t)
		diffs := []*datadiff.DataDiff{
			{Table: "articles"},
			{Table: "authors"},
		}
		orderByDependency(depSorted(), diffs)
		c.Assert(names(diffs), qt.DeepEquals, []string{"authors", "articles"})
	})

	t.Run("ambiguous bare name is not guessed", func(t *testing.T) {
		// The same bare name in two schemas must not resolve via the fallback;
		// with no qualified match either, both rank last and sort alphabetically
		// by qualified name ("t" < "z.t").
		c := qt.New(t)
		db := &goschema.Database{Tables: []goschema.Table{
			{Name: "t", Schema: "b"},
			{Name: "t", Schema: "a"},
		}}
		diffs := []*datadiff.DataDiff{
			{Schema: "z", Table: "t"},
			{Table: "t"},
		}
		orderByDependency(db, diffs)
		c.Assert(names(diffs), qt.DeepEquals, []string{"t", "z.t"})
	})
}

func TestMergeByTable_PreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	changes := []tableChange{
		{table: "tenant.data", updates: 1},
		{schema: "tenant", table: "data", updates: 2},
	}

	got := mergeByTable(changes)

	c.Assert(got, qt.HasLen, 2)
	c.Assert(got[0].qualified(), qt.Equals, `"tenant.data"`)
	c.Assert(got[1].qualified(), qt.Equals, "tenant.data")
}
