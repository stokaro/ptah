package migrateclean

// White-box testing required: a clean check reaches a writer's scope catalog
// only through a *dbschema.DatabaseConnection, which only the built-in
// connector registry builds, and an engine registered with the process
// registry has no connector, so no black-box test can hand the check the
// writer of an engine Ptah does not ship. This file drives the
// connection-free seams the connection path calls, with the writer of an
// invented engine.

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
)

// acmeScope is the scope catalog of an engine nobody ships: it answers the
// tables and the realm it was given, and records the schemas it was asked
// about.
type acmeScope struct {
	tables []dbreset.ScopeTable
	realm  []dbreset.ScopeTable
	err    error
	asked  []string
}

func (a *acmeScope) Tables(_ context.Context, _ dbreset.Querier, _ capability.Capabilities, schema string) ([]dbreset.ScopeTable, error) {
	a.asked = append(a.asked, schema)
	return a.tables, a.err
}

func (a *acmeScope) Realm(context.Context, dbreset.Querier, capability.Capabilities) ([]dbreset.ScopeTable, error) {
	return a.realm, a.err
}

func (a *acmeScope) RealmSchemas(context.Context, dbreset.Querier, capability.Capabilities) ([]string, error) {
	return nil, a.err
}

// acmeWriter is that engine's writer, which offers its scope catalog. The
// embedded writer is never called.
type acmeWriter struct {
	catalog.SchemaWriter
	scope dbreset.ScopeCatalog
}

func (w acmeWriter) ScopeCatalog() dbreset.ScopeCatalog { return w.scope }

// acmeEngine selects a schema by search path and states no tables query, as
// an engine whose writer reads its scope does.
var acmeEngine = descriptor.Dialect{
	Name:    "acme",
	Schemas: descriptor.Schemas{Selection: descriptor.SchemaSelectionSearchPath},
}

// noRows answers every query with nothing.
func noRows(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{}, nil
}

func TestWriterScopeCatalog(t *testing.T) {
	scope := &acmeScope{}
	tests := []struct {
		name   string
		writer catalog.SchemaWriter
		want   dbreset.ScopeCatalog
	}{
		{name: "a writer that offers a catalog", writer: acmeWriter{scope: scope}, want: scope},
		{name: "a writer that offers none", writer: acmeWriter{}, want: nil},
		{name: "a writer with no scope catalog", writer: struct{ catalog.SchemaWriter }{}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(writerScopeCatalog(test.writer), qt.Equals, test.want)
		})
	}
}

// TestScopeTables_ReadsTheWritersCatalog reads the tables of the engine's
// scope through its writer's catalog, with the schema the URL pinned, and
// sends no query of its own.
func TestScopeTables_ReadsTheWritersCatalog(t *testing.T) {
	c := qt.New(t)
	scope := &acmeScope{tables: []dbreset.ScopeTable{{Schema: "app", Name: "orders"}}}
	db := dbtest.Open(c, noRows)

	tables, err := scopeTables(c.Context(), acmeWriter{scope: scope}, db.SQL, capability.Capabilities{}, "acme", acmeEngine, "app")

	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.DeepEquals, []dbreset.ScopeTable{{Schema: "app", Name: "orders"}})
	c.Assert(scope.asked, qt.DeepEquals, []string{"app"})
	c.Assert(db.QueryCount(), qt.Equals, 0)
}

// TestScopeTables_ReadsTheDescribedQuery reads, where the writer offers no
// catalog, the tables query the dialect's descriptor states: names alone on
// an engine with one namespace, and schemas with them otherwise.
func TestScopeTables_ReadsTheDescribedQuery(t *testing.T) {
	tests := []struct {
		name      string
		selection descriptor.SchemaSelection
		columns   []string
		rows      [][]driver.Value
		want      []dbreset.ScopeTable
	}{
		{
			name:      "one namespace",
			selection: descriptor.SchemaSelectionSingle,
			columns:   []string{"name"},
			rows:      [][]driver.Value{{"t2"}, {"t1"}},
			want:      []dbreset.ScopeTable{{Name: "t2"}, {Name: "t1"}},
		},
		{
			name:      "schemas the URL does not select",
			selection: "",
			columns:   []string{"schema", "name"},
			rows:      [][]driver.Value{{"dbo", "t1"}},
			want:      []dbreset.ScopeTable{{Schema: "dbo", Name: "t1"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			engine := descriptor.Dialect{
				Name:       "acme",
				Schemas:    descriptor.Schemas{Selection: test.selection},
				CleanCheck: descriptor.CleanCheck{Dev: true, Tables: "SELECT name FROM acme_tables"},
			}
			var queries []string
			db := dbtest.Open(c, func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
				queries = append(queries, query)
				return dbtest.QueryResult{Columns: test.columns, Rows: test.rows}, nil
			})

			tables, err := scopeTables(c.Context(), struct{ catalog.SchemaWriter }{}, db.SQL, capability.Capabilities{}, "acme", engine, "app")

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.DeepEquals, test.want)
			c.Assert(queries, qt.DeepEquals, []string{"SELECT name FROM acme_tables"})
		})
	}
}

func TestScopeTables_FailurePath(t *testing.T) {
	refused := errors.New("catalog refused")
	t.Run("neither a catalog nor a described query", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, noRows)

		tables, err := scopeTables(c.Context(), acmeWriter{}, db.SQL, capability.Capabilities{}, "acme", acmeEngine, "app")

		c.Assert(err, qt.ErrorMatches, `clean check has no catalog probe for dialect "acme"`)
		c.Assert(tables, qt.IsNil)
		c.Assert(db.QueryCount(), qt.Equals, 0)
	})
	t.Run("the writer's catalog fails", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, noRows)

		tables, err := scopeTables(c.Context(), acmeWriter{scope: &acmeScope{err: refused}}, db.SQL,
			capability.Capabilities{}, "acme", acmeEngine, "app")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `clean check: catalog refused`)
		c.Assert(tables, qt.IsNil)
	})
	t.Run("the described query fails", func(t *testing.T) {
		c := qt.New(t)
		engine := descriptor.Dialect{Name: "acme", CleanCheck: descriptor.CleanCheck{Tables: "SELECT 1"}}
		db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
			return dbtest.QueryResult{}, refused
		})

		tables, err := scopeTables(c.Context(), acmeWriter{}, db.SQL, capability.Capabilities{}, "acme", engine, "")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `clean check: catalog refused`)
		c.Assert(tables, qt.IsNil)
	})
}

// TestRealmSchemas_HappyPath assembles the realm the writer's catalog lists:
// every schema once, in byte order, with its tables in byte order, and a
// schema the catalog lists with no table kept with none.
func TestRealmSchemas_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		realm []dbreset.ScopeTable
		want  []RealmSchema
	}{
		{
			name: "schemas with and without tables",
			realm: []dbreset.ScopeTable{
				{Schema: "b"}, {Schema: "a", Name: "z"}, {Schema: "a", Name: "y"}, {Schema: "Zed"}, {Schema: "c"}, {Schema: "c", Name: "t"},
			},
			want: []RealmSchema{{Name: "Zed"}, {Name: "a", Tables: []string{"y", "z"}}, {Name: "b"}, {Name: "c", Tables: []string{"t"}}},
		},
		{name: "a realm with no schema", realm: nil, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, noRows)

			schemas, err := realmSchemas(c.Context(), acmeWriter{scope: &acmeScope{realm: test.realm}}, db.SQL,
				capability.Capabilities{}, "acme")

			c.Assert(err, qt.IsNil)
			c.Assert(schemas, qt.DeepEquals, test.want)
		})
	}
}

func TestRealmSchemas_FailurePath(t *testing.T) {
	t.Run("a writer that offers no catalog", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, noRows)

		schemas, err := realmSchemas(c.Context(), acmeWriter{}, db.SQL, capability.Capabilities{}, "acme")

		c.Assert(err, qt.ErrorMatches, `clean check has no realm-scope catalog probe for dialect "acme"`)
		c.Assert(schemas, qt.IsNil)
	})
	t.Run("the writer's catalog fails", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("catalog refused")
		db := dbtest.Open(c, noRows)

		schemas, err := realmSchemas(c.Context(), acmeWriter{scope: &acmeScope{err: refused}}, db.SQL,
			capability.Capabilities{}, "acme")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `clean check: catalog refused`)
		c.Assert(schemas, qt.IsNil)
	})
}
