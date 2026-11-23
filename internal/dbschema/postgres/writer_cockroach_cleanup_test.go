package postgres_test

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// cockroachCleanupQuery answers a CockroachDB v26.4.1 cleanup of objects, with
// the session's autocommit_before_ddl set to setting and dependents as the
// views that read the cleanup's relations.
func cockroachCleanupQuery(setting string, objects, dependents [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, autocommitBeforeDDLMarker):
			return autocommitBeforeDDLResult(setting), nil
		case strings.Contains(query, externalRelationDependentsMarker):
			return externalRelationDependentsResult(dependents), nil
		default:
			return postgresCleanupCatalogQuery(query, "CockroachDB CCL v26.4.1", objects)
		}
	}
}

// cockroachSharedTableObjects are a schema of three tables, in the order the
// cleanup lists them.
var cockroachSharedTableObjects = [][]driver.Value{
	{"table", "public", "a_keep", nil, `DROP TABLE IF EXISTS "public"."a_keep" RESTRICT`},
	{"table", "public", "m_shared", nil, `DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`},
	{"table", "public", "z_data", nil, `DROP TABLE IF EXISTS "public"."z_data" RESTRICT`},
}

// cockroachUsersObjects are a table and the view that reads it, table first.
var cockroachUsersObjects = [][]driver.Value{
	{"table", "public", "users", nil, `DROP TABLE IF EXISTS "public"."users" RESTRICT`},
	{"view", "public", "active_users", nil, `DROP VIEW IF EXISTS "public"."active_users" RESTRICT`},
}

// recordingExec records each statement and answers it with execute.
func recordingExec(statements *[]string, execute dbtest.ExecHandler) dbtest.ExecHandler {
	return func(query string, args []driver.NamedValue) (driver.Result, error) {
		*statements = append(*statements, query)
		return execute(query, args)
	}
}

// refusingDrop refuses every statement that names table, as a server does for
// a table something outside the cleanup still depends on.
func refusingDrop(table string) dbtest.ExecHandler {
	return func(query string, _ []driver.NamedValue) (driver.Result, error) {
		if strings.Contains(query, `"`+table+`"`) {
			return nil, errors.New("cannot drop table " + table + " because other objects depend on it")
		}
		return driver.RowsAffected(0), nil
	}
}

// A view in another schema that reads a table the cleanup drops is refused
// before the first drop on a server that commits each drop as it runs:
// refused at the table's drop instead, the cleanup would keep every drop
// before it.
func TestWriterDropAllTables_CockroachRefusesAnOutsideView_FailurePath(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t,
		cockroachCleanupQuery("on", cockroachSharedTableObjects, [][]driver.Value{
			{"public", "m_shared", "other", "v", "v"},
		}),
		recordingExec(&statements, refusingDrop("m_shared")),
	)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, `refusing to clean schema "public": a view the cleanup does not drop reads a `+
		`relation it drops: view "other"."v" reads table "public"."m_shared"; the server commits each drop as it `+
		`runs, so the cleanup refuses before it drops anything`)
	c.Assert(statements, qt.IsNil)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
}

// A view that reads a table the cleanup drops is no refusal when the cleanup
// drops the view too: the drops run, and the table is retried after the view.
func TestWriterDropAllTables_CockroachDropsAViewItListsWithItsTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t,
		cockroachCleanupQuery("on", cockroachUsersObjects, [][]driver.Value{
			{"public", "users", "public", "active_users", "v"},
		}),
		recordingExec(&statements, new(postgresInternalDependencyExec).execute),
	)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`DROP TABLE IF EXISTS "public"."users" RESTRICT`,
		`DROP VIEW IF EXISTS "public"."active_users" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."users" RESTRICT`,
	})
	c.Assert(db.CommitCount(), qt.Equals, 1)
}

// A drop the server keeps refusing, for a dependent the catalog does not
// report, ends the cleanup after the drops around it have run and committed.
// The error names each of them.
func TestWriterDropAllTables_CockroachNamesWhatItDroppedBeforeTheFailure_FailurePath(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t,
		cockroachCleanupQuery("on", cockroachSharedTableObjects, nil),
		recordingExec(&statements, refusingDrop("m_shared")),
	)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, `refusing to clean schema "public": failed to drop table m_shared: `+
		`SQL execution failed: cannot drop table m_shared because other objects depend on it`+"\n"+
		`SQL: DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`+"\n"+
		`the server commits each drop as it runs; dropped before the failure: table "public"."a_keep", `+
		`table "public"."z_data"`)
	c.Assert(statements, qt.DeepEquals, []string{
		`DROP TABLE IF EXISTS "public"."a_keep" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."z_data" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`,
	})
}

// A CockroachDB session that holds its schema statements in the transaction,
// with autocommit_before_ddl off or on a server without the setting, makes one
// pass: the first refused drop aborts the transaction, so a retry would answer
// 25P02 in place of the refusal. The refusal names the setting where the
// session has it.
func TestWriterDropAllTables_CockroachHoldsTheDropsInOneTransaction_FailurePath(t *testing.T) {
	const refusal = `refusing to clean schema "public": failed to drop table users: SQL execution failed: ` +
		`cannot drop table because view depends on it` + "\n" +
		`SQL: DROP TABLE IF EXISTS "public"."users" RESTRICT`
	tests := []struct {
		name    string
		setting string
		wantErr string
	}{
		{
			name:    "the setting off",
			setting: "off",
			wantErr: refusal + "\n" + "the session runs with autocommit_before_ddl off, so the drops share one " +
				"transaction and a refused one cannot be retried; with the setting on, the default from v25, the " +
				"cleanup retries a refused drop once the dependent that holds it up is dropped",
		},
		{name: "no such setting", setting: "", wantErr: refusal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				cockroachCleanupQuery(test.setting, cockroachUsersObjects, nil),
				recordingExec(&statements, new(postgresInternalDependencyExec).execute),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.wantErr)
			c.Assert(statements, qt.DeepEquals, []string{`DROP TABLE IF EXISTS "public"."users" RESTRICT`})
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}
