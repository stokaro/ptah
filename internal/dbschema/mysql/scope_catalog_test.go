package mysql_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/mysqlsystemdb"
)

// TestScopeCatalog_ServesEveryMemberOfTheFamily pins where the MySQL family's
// writer offers its scope catalog: on every dialect it serves, in any
// spelling. None of those dialects states a CleanCheck.Tables query, which
// the clean check would otherwise read as a second answer about the same
// tables.
func TestScopeCatalog_ServesEveryMemberOfTheFamily(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB, "maria"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			described, _ := platform.Lookup(dialect)

			c.Assert(mysql.NewMySQLWriter(nil, "", dialect).ScopeCatalog(), qt.IsNotNil)
			c.Assert(described.CleanCheck.Tables, qt.Equals, "")
		})
	}
}

// scopeReads answers every read with the same columns and rows, and records
// each query and its arguments.
type scopeReads struct {
	columns []string
	rows    [][]driver.Value
	err     error
	queries []string
	args    [][]any
}

func (r *scopeReads) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	r.queries = append(r.queries, query)
	values := make([]any, 0, len(args))
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	r.args = append(r.args, values)
	return dbtest.QueryResult{Columns: r.columns, Rows: r.rows}, r.err
}

// TestScopeCatalog_Tables_HappyPath reads the base tables of the database the
// URL named, or of the session's DATABASE() for the empty name. A view is not
// a table.
func TestScopeCatalog_Tables_HappyPath(t *testing.T) {
	for _, schema := range []string{"app", ""} {
		t.Run(schema, func(t *testing.T) {
			c := qt.New(t)
			reads := &scopeReads{
				columns: []string{"table_schema", "table_name"},
				rows:    [][]driver.Value{{"app", "orders"}, {"app", "customers"}},
			}
			db := dbtest.Open(c, reads.answer)

			tables, err := mysql.NewMySQLWriter(db.SQL, "", platform.MySQL).ScopeCatalog().Tables(
				c.Context(), db.SQL, capability.ForDialect(platform.MySQL), schema)

			c.Assert(err, qt.IsNil)
			c.Assert(tables, qt.DeepEquals, []dbreset.ScopeTable{{Schema: "app", Name: "orders"}, {Schema: "app", Name: "customers"}})
			c.Assert(reads.args, qt.DeepEquals, [][]any{{schema}})
			c.Assert(reads.queries, qt.HasLen, 1)
			c.Assert(reads.queries[0], qt.Contains, "WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE())")
			c.Assert(reads.queries[0], qt.Contains, "AND table_type = 'BASE TABLE'")
		})
	}
}

// TestScopeCatalog_Realm_HappyPath lists every user database of the server
// with its base tables, a database holding none once with no table, and
// leaves the server's own databases out.
func TestScopeCatalog_Realm_HappyPath(t *testing.T) {
	c := qt.New(t)
	reads := &scopeReads{
		columns: []string{"SCHEMA_NAME", "TABLE_NAME"},
		rows:    [][]driver.Value{{"empty", ""}, {"shop", "orders"}},
	}
	db := dbtest.Open(c, reads.answer)

	tables, err := mysql.NewMySQLWriter(db.SQL, "", platform.MariaDB).ScopeCatalog().Realm(
		c.Context(), db.SQL, capability.ForDialect(platform.MariaDB))

	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.DeepEquals, []dbreset.ScopeTable{{Schema: "empty"}, {Schema: "shop", Name: "orders"}})
	c.Assert(reads.args, qt.DeepEquals, [][]any{{}})
	c.Assert(reads.queries, qt.HasLen, 1)
	c.Assert(reads.queries[0], qt.Contains, "LEFT JOIN information_schema.TABLES t")
	c.Assert(reads.queries[0], qt.Contains, "AND t.TABLE_TYPE = 'BASE TABLE'")
	c.Assert(reads.queries[0], qt.Contains, "WHERE "+mysqlsystemdb.MySQLUserDatabasesPredicate("s.SCHEMA_NAME"))
}

// TestScopeCatalog_RealmSchemas_HappyPath lists every user database of the
// server in the order the catalog returns them.
func TestScopeCatalog_RealmSchemas_HappyPath(t *testing.T) {
	c := qt.New(t)
	reads := &scopeReads{columns: []string{"SCHEMA_NAME"}, rows: [][]driver.Value{{"r2"}, {"R1"}}}
	db := dbtest.Open(c, reads.answer)

	names, err := mysql.NewMySQLWriter(db.SQL, "", platform.MySQL).ScopeCatalog().RealmSchemas(
		c.Context(), db.SQL, capability.ForDialect(platform.MySQL))

	c.Assert(err, qt.IsNil)
	c.Assert(names, qt.DeepEquals, []string{"r2", "R1"})
	c.Assert(reads.queries, qt.HasLen, 1)
	c.Assert(reads.queries[0], qt.Contains, "FROM information_schema.SCHEMATA")
	c.Assert(reads.queries[0], qt.Contains, "WHERE "+mysqlsystemdb.MySQLUserDatabasesPredicate("SCHEMA_NAME"))
}

// TestScopeCatalog_FailurePath hands back the error a read failed with,
// unwrapped, and no answer.
func TestScopeCatalog_FailurePath(t *testing.T) {
	refused := errors.New("SELECT command denied to user")
	caps := capability.ForDialect(platform.MySQL)

	t.Run("tables", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, (&scopeReads{err: refused}).answer)

		tables, err := mysql.NewMySQLWriter(db.SQL, "", platform.MySQL).ScopeCatalog().Tables(c.Context(), db.SQL, caps, "app")

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(tables, qt.IsNil)
	})
	t.Run("realm", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, (&scopeReads{err: refused}).answer)

		tables, err := mysql.NewMySQLWriter(db.SQL, "", platform.MySQL).ScopeCatalog().Realm(c.Context(), db.SQL, caps)

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(tables, qt.IsNil)
	})
	t.Run("realm schemas", func(t *testing.T) {
		c := qt.New(t)
		db := dbtest.Open(c, (&scopeReads{err: refused}).answer)

		names, err := mysql.NewMySQLWriter(db.SQL, "", platform.MySQL).ScopeCatalog().RealmSchemas(c.Context(), db.SQL, caps)

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(names, qt.IsNil)
	})
}
