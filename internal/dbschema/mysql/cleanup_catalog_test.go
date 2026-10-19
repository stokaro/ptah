package mysql_test

import (
	"database/sql/driver"
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
