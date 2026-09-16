package managedrows_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/managedrows"
)

var postgresNames = identifier.ForDialect("postgres")

var oracleNames = identifier.ForDialect("oracle")

var sqliteNames = identifier.ForDialect("sqlite")

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

// liveOracleSettings is the catalog Oracle reports for a table the renderer
// created as `settings (code, label)`: the bare names folded to upper case.
func liveOracleSettings() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name:   "SETTINGS",
		Schema: "PTAH_USER",
		Columns: []catalog.Column{
			{Name: "CODE"},
			{Name: "LABEL"},
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
		got := managedrows.ProjectOntoLive([]string{"code", "label", "note"}, &live.Tables[0], postgresNames)
		c.Assert(got, qt.DeepEquals, []string{"code", "label"})
	})

	t.Run("a caller with no introspected table keeps the whole projection", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.ProjectOntoLive([]string{"code", "note"}, nil, postgresNames), qt.DeepEquals, []string{"code", "note"})
	})

	t.Run("oracle matches a folded column and keeps the declared spelling", func(t *testing.T) {
		// Matched exactly, CODE and LABEL are not code and label, every column
		// is dropped, and the read has nothing left to ask for.
		c := qt.New(t)
		live := liveOracleSettings()
		got := managedrows.ProjectOntoLive([]string{"code", "label", "note"}, &live.Tables[0], oracleNames)
		c.Assert(got, qt.DeepEquals, []string{"code", "label"})
	})
}

// TestProjectOntoLive_FailurePath is the control for the Oracle row above: under
// exact rules a column spelled in another case is a different column.
func TestProjectOntoLive_FailurePath(t *testing.T) {
	t.Run("postgres keeps case, so an upper-case column is another column", func(t *testing.T) {
		c := qt.New(t)
		live := liveOracleSettings()
		got := managedrows.ProjectOntoLive([]string{"code", "label"}, &live.Tables[0], postgresNames)
		c.Assert(got, qt.HasLen, 0)
	})
}

func TestLiveTable_HappyPath(t *testing.T) {
	t.Run("a declaration with no schema matches wherever the reader put it", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "", "settings", postgresNames), qt.IsNotNil)
	})

	t.Run("a declared schema matches the reader's", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "public", "settings", postgresNames), qt.IsNotNil)
	})

	t.Run("oracle finds the table under the name it folded", func(t *testing.T) {
		// Compared exactly, the table is never found, its rows are never read,
		// and every apply plans the same INSERTs again.
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveOracleSettings(), "", "settings", oracleNames), qt.IsNotNil)
	})

	t.Run("oracle folds a declared schema too", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveOracleSettings(), "ptah_user", "settings", oracleNames), qt.IsNotNil)
	})

	t.Run("a declared default schema matches a schema the reader blanked", func(t *testing.T) {
		// Readers blank the schema of default-schema tables, so a declaration
		// that spells the default out names the same table.
		c := qt.New(t)
		blanked := &catalog.Database{Tables: []catalog.Table{{Name: "settings"}}}
		c.Assert(managedrows.LiveTable(blanked, "main", "settings", sqliteNames), qt.IsNotNil)
	})

	t.Run("an omitted schema prefers the table in the default schema", func(t *testing.T) {
		c := qt.New(t)
		shared := &catalog.Database{Tables: []catalog.Table{
			{Name: "settings", Schema: "reference"},
			{Name: "settings", Schema: "public"},
		}}
		got := managedrows.LiveTable(shared, "", "settings", postgresNames)
		c.Assert(got, qt.IsNotNil)
		c.Assert(got.Schema, qt.Equals, "public")
	})
}

func TestLiveTable_FailurePath(t *testing.T) {
	t.Run("a table the catalog does not hold", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "", "regions", postgresNames), qt.IsNil)
	})

	t.Run("a table of that name in another schema", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveSettings(), "reference", "settings", postgresNames), qt.IsNil)
	})

	t.Run("no catalog at all", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(nil, "", "settings", postgresNames), qt.IsNil)
	})

	t.Run("postgres keeps case, so an upper-case table is another table", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(managedrows.LiveTable(liveOracleSettings(), "", "settings", postgresNames), qt.IsNil)
	})

	t.Run("a bare name two other schemas share resolves to neither", func(t *testing.T) {
		// The reference-data page promises this: with no declared schema and no
		// candidate in the default one, there is no answer to pick.
		c := qt.New(t)
		shared := &catalog.Database{Tables: []catalog.Table{
			{Name: "settings", Schema: "reference"},
			{Name: "settings", Schema: "staging"},
		}}
		c.Assert(managedrows.LiveTable(shared, "", "settings", postgresNames), qt.IsNil)
	})

	t.Run("a schema the reader blanked is not every declared schema", func(t *testing.T) {
		// A blank introspected schema is the default schema left out, so it
		// answers the declaration naming that default and no other.
		c := qt.New(t)
		blanked := &catalog.Database{Tables: []catalog.Table{{Name: "settings"}}}
		c.Assert(managedrows.LiveTable(blanked, "reference", "settings", postgresNames), qt.IsNil)
	})
}
