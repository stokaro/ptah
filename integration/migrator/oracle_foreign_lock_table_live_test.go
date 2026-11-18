//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// foreignLockTable is how the owner's schema stands before the run, and what
// the run's account may do there.
type foreignLockTable struct {
	name string
	// grants are the run's privileges beyond CREATE SESSION and CREATE TABLE.
	grants string
	// prepare runs as the DBA in the owner's schema, {owner} and {login}
	// standing for the two accounts.
	prepare []string
	// rows is how many rows the lock table holds after the run.
	rows string
}

var foreignLockTables = []foreignLockTable{
	{
		// A table a DBA created and granted: the refusal comes before the
		// lock writes its row.
		name: "existing",
		prepare: []string{
			`CREATE TABLE "{owner}".ptah_locks (name VARCHAR2(1000) PRIMARY KEY)`,
			`GRANT SELECT, INSERT ON "{owner}".ptah_locks TO {login}`,
		},
		rows: "0",
	},
	{
		// No table: the lock creates it, owned by the schema's account, and
		// the refusal comes after.
		name:   "created by the lock",
		grants: ", CREATE ANY TABLE, CREATE ANY INDEX, INSERT ANY TABLE, SELECT ANY TABLE",
		rows:   "1",
	},
}

// A migration lock that keeps its table in another account's schema writes to
// a table that account owns, which Ptah refuses for every other metadata table
// unless PTAH_ALLOW_FOREIGN_METADATA_TABLE accepts it. So it refuses the lock
// table too: before the lock writes to a table already there, and again after
// the lock created it, since the check and the creation are two round trips.
// Either way the run runs nothing, and the error names the lock table.
func TestOracleForeignLockTableLive_FailurePath(t *testing.T) {
	for _, test := range foreignLockTables {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_ALLOW_FOREIGN_METADATA_TABLE", "0")
			adminURL := dbtarget.URL(c, dbtarget.OracleAdmin)
			admin := connectOracleLock(c, adminURL)
			stamp := time.Now().UnixNano() % 1_000_000_000
			owner := oracleAccount(c, admin, fmt.Sprintf("ptah_fl_o_%d", stamp), fmt.Sprintf("GRANT CREATE SESSION TO ptah_fl_o_%d", stamp))
			login := oracleAccount(c, admin, fmt.Sprintf("ptah_fl_a_%d", stamp),
				fmt.Sprintf("GRANT CREATE SESSION, CREATE TABLE%s TO ptah_fl_a_%d", test.grants, stamp))
			for _, statement := range test.prepare {
				statement = regexp.MustCompile(`\{owner\}`).ReplaceAllString(statement, owner)
				statement = regexp.MustCompile(`\{login\}`).ReplaceAllString(statement, login)
				_, err := admin.ExecContext(context.Background(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
			}
			run, err := migrator.NewFSMigrator(connectOracleLock(c, oracleAccountURL(c, adminURL, login)), oracleWidgets())
			c.Assert(err, qt.IsNil)

			err = run.WithMigrationsTable(owner, "").MigrateUp(context.Background())

			c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
			c.Assert(err, qt.ErrorMatches, `(?s).*refusing to use metadata table "`+owner+`"\."PTAH_LOCKS": it is owned by "`+owner+`".*`)
			c.Assert(readOne(c, admin, `SELECT COUNT(*) FROM "`+owner+`".ptah_locks`), qt.Equals, test.rows)
			c.Assert(readOne(c, admin, `SELECT COUNT(*) FROM all_tables WHERE owner IN ('`+owner+`', '`+login+`') AND table_name <> 'PTAH_LOCKS'`), qt.Equals, "0")
		})
	}
}
