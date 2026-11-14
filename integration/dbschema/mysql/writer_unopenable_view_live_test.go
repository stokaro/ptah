//go:build integration

package mysql_test

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
)

// A view whose table is gone cannot be opened, and LOCK TABLES opens every
// view it names, so the server refuses the lock the cleanup protects its views
// with (error 1356). The cleanup still empties the database, the view that
// opens included, which is what a dev database replayed past a DROP TABLE of a
// table a view reads holds.
func TestWriterDropAllTables_LiveDropsAViewWhoseTableIsGone(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
	}{
		{
			name:    "mysql",
			engine:  dbtarget.MySQLAdmin,
			dialect: platform.MySQL,
		},
		{
			name:    "mariadb",
			engine:  dbtarget.MariaDBAdmin,
			dialect: platform.MariaDB,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := openMySQLWriterLiveDatabase(c, test.engine)
			c.Cleanup(func() {
				c.Check(db.Close(), qt.IsNil)
			})
			target := fmt.Sprintf("ptah_unopenable_view_%d", time.Now().UnixNano())
			createMySQLWriterLiveDatabase(c, db, target)
			c.Cleanup(func() {
				dropMySQLWriterLiveDatabase(c, db, target)
			})
			qualified := func(name string) string { return sqlident.Qualified(test.dialect, target, name) }
			for _, statement := range []string{
				"CREATE TABLE " + qualified("gone") + " (id BIGINT PRIMARY KEY)",
				"CREATE TABLE " + qualified("kept") + " (id BIGINT PRIMARY KEY)",
				"CREATE VIEW " + qualified("reads_gone") + " AS SELECT id FROM " + qualified("gone"),
				"CREATE VIEW " + qualified("reads_kept") + " AS SELECT id FROM " + qualified("kept"),
				"DROP TABLE " + qualified("gone"),
			} {
				_, err := db.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}
			_, err := db.ExecContext(c.Context(), "LOCK TABLES "+qualified("reads_gone")+" WRITE")
			c.Assert(err, qt.ErrorMatches, `Error 1356 \(HY000\): View '.*reads_gone' references invalid table.*`)
			writer := mysql.NewMySQLWriter(db, target, test.dialect)

			err = writer.DropAllTables(c.Context())

			c.Assert(err, qt.IsNil)
			var objects int
			err = db.QueryRowContext(
				c.Context(),
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ?",
				target,
			).Scan(&objects)
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.Equals, 0)
		})
	}
}
