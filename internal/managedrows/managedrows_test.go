package managedrows_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/managedrows"
)

func liveSettings() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name:   "settings",
		Schema: "public",
		Columns: []catalog.Column{
			{Name: "code"},
			{Name: "label"},
		},
	}}}
}

func TestColumns_HappyPath(t *testing.T) {
	t.Run("keys and every column a row carries, sorted", func(t *testing.T) {
		c := qt.New(t)
		rows := []map[string]any{{"label": "a"}, {"note": "b"}}
		c.Assert(managedrows.Columns(rows, []string{"code"}), qt.DeepEquals, []string{"code", "label", "note"})
	})

	t.Run("a declaration with no rows still names its keys", func(t *testing.T) {
		// An emptied desired set reads the live table by its keys; asking for
		// no column at all would return no rows and read as converged.
		c := qt.New(t)
		c.Assert(managedrows.Columns(nil, []string{"tenant", "code"}), qt.DeepEquals, []string{"code", "tenant"})
	})
}

func TestProjectOntoLive_HappyPath(t *testing.T) {
	t.Run("a column the table does not have is dropped", func(t *testing.T) {
		// The plan may be about to add it. Asking the server for it first is
		// what answers 42703 (stokaro/ptah#3260); the declared value still
		// reaches the plan as an insert or an update.
		c := qt.New(t)
		live := liveSettings()
		got := managedrows.ProjectOntoLive([]string{"code", "label", "note"}, &live.Tables[0])
		c.Assert(got, qt.DeepEquals, []string{"code", "label"})
	})

	t.Run("a caller with no introspected table keeps the whole projection", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.ProjectOntoLive([]string{"code", "note"}, nil), qt.DeepEquals, []string{"code", "note"})
	})
}

func TestLiveTable_HappyPath(t *testing.T) {
	t.Run("a declaration with no schema matches wherever the reader put it", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "", "settings"), qt.Not(qt.IsNil))
	})

	t.Run("a declared schema matches the reader's", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "public", "settings"), qt.Not(qt.IsNil))
	})
}

func TestLiveTable_FailurePath(t *testing.T) {
	t.Run("a table the catalog does not hold", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "", "regions"), qt.IsNil)
	})

	t.Run("a table of that name in another schema", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "reference", "settings"), qt.IsNil)
	})

	t.Run("no catalog at all", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(nil, "", "settings"), qt.IsNil)
	})
}
