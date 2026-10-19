package mysql_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
)

// TestCleanupCatalog_OffersTheStoredObjectsWhereTheyAreListed pins where the
// MySQL family's writer offers its catalog strategy: on the engines whose
// descriptor states the cleanup lists stored programs, in any spelling, and
// on no writer built without a dialect.
func TestCleanupCatalog_OffersTheStoredObjectsWhereTheyAreListed(t *testing.T) {
	tests := []struct {
		dialect string
		offered bool
	}{
		{dialect: platform.MySQL, offered: true},
		{dialect: platform.MariaDB, offered: true},
		{dialect: "maria", offered: true},
		{dialect: "", offered: false},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mysql.NewMySQLWriter(nil, "", test.dialect).CleanupCatalog() != nil, qt.Equals, test.offered)
		})
	}
}

// storedRows answers the stored-object read with rows.
func storedRows(rows ...[]driver.Value) dbtest.QueryHandler {
	return func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"object_name", "object_kind"}, Rows: rows}, nil
	}
}

func TestCleanupCatalog_Objects_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, storedRows(
		[]driver.Value{"e_tick", "EVENT"},
		[]driver.Value{"f_one", "FUNCTION"},
		[]driver.Value{"p_touch", " procedure "},
		[]driver.Value{"s_counter", "SEQUENCE"},
	))

	objects, err := mysql.NewMySQLWriter(nil, "", platform.MariaDB).CleanupCatalog().Objects(c.Context(), db.SQL, "app")

	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []dbreset.CleanupObject{
		{Kind: "event", Name: "e_tick"},
		{Kind: "function", Name: "f_one"},
		{Kind: "procedure", Name: "p_touch"},
		{Kind: "sequence", Name: "s_counter"},
	})
}

func TestCleanupCatalog_Objects_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, storedRows([]driver.Value{"t_audit", "TRIGGER"}))

	objects, err := mysql.NewMySQLWriter(nil, "", platform.MySQL).CleanupCatalog().Objects(c.Context(), db.SQL, "app")

	c.Assert(err, qt.ErrorMatches, `inspect stored objects: unsupported object kind "TRIGGER"`)
	c.Assert(objects, qt.IsNil)
}

// TestCleanupCatalog_OrdersAndLocksNothing pins the two answers the MySQL
// family leaves to the writer's own order: no view depth and no lock.
func TestCleanupCatalog_OrdersAndLocksNothing(t *testing.T) {
	c := qt.New(t)
	strategy := mysql.NewMySQLWriter(nil, "", platform.MySQL).CleanupCatalog()

	depths, err := strategy.ViewDepths(c.Context(), nil, "app")

	c.Assert(err, qt.IsNil)
	c.Assert(depths, qt.IsNil)
	c.Assert(strategy.LockRelations(c.Context(), nil, []dbreset.Relation{{Name: "users"}}), qt.IsNil)
}

// serverLister is what a whole-server cleanup asks of the catalog strategy,
// beside the reads every strategy answers.
type serverLister interface {
	ServerObjects(ctx context.Context, q dbreset.Querier) ([]dbreset.CleanupObject, error)
}

// serverRows answers the user-database read with databases and the
// cross-database foreign key read with keys, failing the read whose query
// holds failOn.
func serverRows(failOn string, databases, keys [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case failOn != "" && strings.Contains(query, failOn):
			return dbtest.QueryResult{}, errors.New("access denied")
		case strings.Contains(query, "information_schema.SCHEMATA"):
			return dbtest.QueryResult{Columns: []string{"SCHEMA_NAME"}, Rows: databases}, nil
		default:
			return dbtest.QueryResult{Columns: []string{"TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME"}, Rows: keys}, nil
		}
	}
}

// TestCleanupCatalog_ServerObjects_HappyPath pins what a cleanup of a whole
// server drops: the foreign keys between databases first, then each user
// database as a schema.
func TestCleanupCatalog_ServerObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, serverRows("",
		[][]driver.Value{{"r1"}, {"r4"}},
		[][]driver.Value{{"r4", "z", "zfk"}},
	))
	server, ok := mysql.NewMySQLWriter(nil, "", platform.MySQL).CleanupCatalog().(serverLister)
	c.Assert(ok, qt.IsTrue)

	objects, err := server.ServerObjects(c.Context(), db.SQL)

	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []dbreset.CleanupObject{
		{Kind: "foreign_key", Schema: "r4", Table: "z", Name: "zfk"},
		{Kind: "schema", Schema: "r1", Name: "r1"},
		{Kind: "schema", Schema: "r4", Name: "r4"},
	})
}

// TestCleanupCatalog_ServerObjects_FailurePath names the read that failed.
// The databases are read first, so when both reads fail the databases are
// named.
func TestCleanupCatalog_ServerObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		failOn  string
		wantErr string
	}{
		{name: "the databases", failOn: "information_schema.SCHEMATA", wantErr: `inspect cleanup databases: .*access denied`},
		{name: "the foreign keys", failOn: "information_schema.KEY_COLUMN_USAGE",
			wantErr: `inspect cleanup foreign keys between databases: .*access denied`},
		{name: "both, the databases read first", failOn: "information_schema.", wantErr: `inspect cleanup databases: .*access denied`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, serverRows(test.failOn, [][]driver.Value{{"r1"}}, nil))
			server, ok := mysql.NewMySQLWriter(nil, "", platform.MariaDB).CleanupCatalog().(serverLister)
			c.Assert(ok, qt.IsTrue)

			objects, err := server.ServerObjects(c.Context(), db.SQL)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects, qt.IsNil)
		})
	}
}
