package dataorder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/dataorder"
)

// depSorted is a schema as the parser hands it over: dependency-sorted, parents
// first. "articles" sorts before "authors" alphabetically, so a caller that
// ranked by name would put the child first — which is the defect this package
// exists to remove, and the reason the fixture is named this way round.
func depSorted() *schemamodel.Database {
	return &schemamodel.Database{Tables: []schemamodel.Table{
		{Name: "authors", Schema: "app"},
		{Name: "articles", Schema: "app"},
	}}
}

func TestRank_HappyPath(t *testing.T) {
	t.Run("qualified name gives the table's position", func(t *testing.T) {
		c := qt.New(t)
		ranker := dataorder.New(depSorted())
		c.Assert(ranker.Rank("app", "authors"), qt.Equals, 0)
		c.Assert(ranker.Rank("app", "articles"), qt.Equals, 1)
	})

	t.Run("bare name resolves when the schema is unambiguous", func(t *testing.T) {
		// A //ptah:schema:data annotation may omit the schema its
		// //ptah:schema:table sets. Without the fallback the lookup misses and
		// the caller orders by whatever it falls back on.
		c := qt.New(t)
		ranker := dataorder.New(depSorted())
		c.Assert(ranker.Rank("", "authors"), qt.Equals, 0)
		c.Assert(ranker.Rank("", "articles"), qt.Equals, 1)
	})

	t.Run("a one-table schema ranks its table first", func(t *testing.T) {
		c := qt.New(t)
		ranker := dataorder.New(&schemamodel.Database{
			Tables: []schemamodel.Table{{Name: "regions"}},
		})
		c.Assert(ranker.Rank("", "regions"), qt.Equals, 0)
	})
}

func TestRank_FailurePath(t *testing.T) {
	t.Run("a table the schema does not define ranks last", func(t *testing.T) {
		c := qt.New(t)
		ranker := dataorder.New(depSorted())
		c.Assert(ranker.Rank("app", "comments"), qt.Equals, dataorder.Unknown)
	})

	t.Run("a bare name two schemas share is not guessed", func(t *testing.T) {
		// Resolving it would name one of the two tables, and the wrong one half
		// the time. Ranking it last costs the ordering and invents nothing.
		c := qt.New(t)
		ranker := dataorder.New(&schemamodel.Database{Tables: []schemamodel.Table{
			{Name: "t", Schema: "b"},
			{Name: "t", Schema: "a"},
		}})
		c.Assert(ranker.Rank("", "t"), qt.Equals, dataorder.Unknown)
		c.Assert(ranker.Rank("z", "t"), qt.Equals, dataorder.Unknown)
		// The qualified names still resolve: the guard is about the fallback.
		c.Assert(ranker.Rank("a", "t"), qt.Equals, 1)
		c.Assert(ranker.Rank("b", "t"), qt.Equals, 0)
	})

	t.Run("the zero ranker and a nil schema rank everything last", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(dataorder.Ranker{}.Rank("app", "authors"), qt.Equals, dataorder.Unknown)
		c.Assert(dataorder.New(nil).Rank("app", "authors"), qt.Equals, dataorder.Unknown)
	})
}
