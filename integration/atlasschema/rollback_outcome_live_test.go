//go:build integration

package atlasschema_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the PostgreSQL driver

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/dispatchwitness"
	"ptah.run/migration/migrator"
)

func TestApplyStatements_PostgresConfirmedRollbackReportsFailedLive(t *testing.T) {
	url := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })

	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	table := fmt.Sprintf("ptah_apply_rollback_%d", time.Now().UnixNano())
	c.Cleanup(func() {
		_, dropErr := db.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
		c.Check(dropErr, qt.IsNil)
	})
	statement := `CREATE TABLE "` + table + `" (id integer)`
	evidence := atlasschema.ApplyEvidence{}
	evidence.Err = atlasschema.ApplyStatements(dispatchwitness.With(c.Context(), func() {
		evidence.Dispatched = true
	}), conn, migrator.MigrationTxModeAll, []string{statement, statement})

	c.Assert(evidence.Err, qt.IsNotNil)
	c.Assert(evidence.Dispatched, qt.IsTrue)
	report := atlasschema.NewApplyReport(evidence)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
	c.Assert(report.Failure, qt.DeepEquals, &atlasschema.ApplyFailure{Statement: 2, Statements: 2, SQLState: "42P07"})
	var exists bool
	c.Assert(db.QueryRowContext(c.Context(), `SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists), qt.IsNil)
	c.Assert(exists, qt.IsFalse)
}
