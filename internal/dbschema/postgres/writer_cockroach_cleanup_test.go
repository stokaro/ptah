package postgres_test

import (
	"database/sql/driver"
	"errors"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/postgres"
)

// cockroachCleanupQuery answers a CockroachDB v26.4.1 cleanup of objects, with
// the session's autocommit_before_ddl set to setting, dependents as the
// catalog rows that record a dependency on the cleanup's objects, and texts as
// the defaults, bodies, OIDs and database name it matches as text.
func cockroachCleanupQuery(setting string, objects, dependents, texts [][]driver.Value) dbtest.QueryHandler {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.Contains(query, autocommitBeforeDDLMarker):
			return autocommitBeforeDDLResult(setting), nil
		case strings.Contains(query, outsideTextDependentsMarker):
			return outsideTextDependentsResult(texts), nil
		case strings.Contains(query, outsideStructuredDependentsMarker):
			return outsideStructuredDependentsResult(dependents), nil
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
			{"rule", "relation", "public", "m_shared", "relation", "other", "v", "v", ""},
		}, nil),
		recordingExec(&statements, refusingDrop("m_shared")),
	)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, `refusing to clean schema "public": an object the cleanup does not drop `+
		`depends on one it drops: view "other"."v" reads table "public"."m_shared"; the server commits each drop as `+
		`it runs, so the cleanup refuses before it drops anything`)
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
			{"rule", "relation", "public", "users", "relation", "public", "active_users", "v", ""},
		}, nil),
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
		cockroachCleanupQuery("on", cockroachSharedTableObjects, nil, nil),
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
				cockroachCleanupQuery(test.setting, cockroachUsersObjects, nil, nil),
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

// cockroachOutsideObjects are a schema that holds one object of each family
// something outside it can depend on, and z_data, in the order the cleanup
// lists them.
var cockroachOutsideObjects = [][]driver.Value{
	{"table", "public", "t", nil, `DROP TABLE IF EXISTS "public"."t" RESTRICT`},
	{"table", "public", "z_data", nil, `DROP TABLE IF EXISTS "public"."z_data" RESTRICT`},
	{"sequence", "public", "s", nil, `DROP SEQUENCE IF EXISTS "public"."s" RESTRICT`},
	{"function", "public", "fn", nil, `DROP FUNCTION IF EXISTS "public"."fn"() RESTRICT`},
	{"type", "public", "e", nil, `DROP TYPE IF EXISTS "public"."e" RESTRICT`},
}

// cockroachOutsideSpellings are the OIDs of the sequence and the type of
// cockroachOutsideObjects and the database name, as the text read returns
// them.
var cockroachOutsideSpellings = [][]driver.Value{
	{"sequence", "public", "s", "", "4208", ""},
	{"type", "public", "e", "", "104221", ""},
	{"database", "", "app", "", "", ""},
}

// acceptingDrop accepts every statement.
func acceptingDrop(string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

// Each dependency an object outside the cleanup holds on one in it is refused
// before the first drop, and named: the ones the catalog records, and the
// defaults and routine bodies CockroachDB spells them in.
func TestWriterDropAllTables_CockroachRefusesEachOutsideDependent_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		dependents [][]driver.Value
		texts      [][]driver.Value
		want       string
	}{
		{
			name:       "a foreign key from another schema",
			dependents: [][]driver.Value{{"foreign key", "relation", "public", "t", "relation", "other", "c", "r", "c_tid_fkey"}},
			want:       `foreign key "c_tid_fkey" on table "other"."c" references table "public"."t"`,
		},
		{
			name:       "a function that calls one",
			dependents: [][]driver.Value{{"call", "routine", "public", "fn", "routine", "other", "g", "f", ""}},
			want:       `function "other"."g" calls function "public"."fn"`,
		},
		{
			name:       "a trigger that runs one",
			dependents: [][]driver.Value{{"trigger", "routine", "public", "fn", "relation", "other", "t2", "r", "tr"}},
			want:       `trigger "tr" on table "other"."t2" runs function "public"."fn"`,
		},
		{
			name:       "a column of the type",
			dependents: [][]driver.Value{{"column type", "type", "public", "e", "relation", "other", "t4", "r", "c"}},
			want:       `column "c" of table "other"."t4" has type "public"."e"`,
		},
		{
			name:       "an attribute of a composite type",
			dependents: [][]driver.Value{{"column type", "type", "public", "e", "relation", "other", "ct", "c", "f"}},
			want:       `column "f" of type "other"."ct" has type "public"."e"`,
		},
		{
			name:       "a function that returns the type",
			dependents: [][]driver.Value{{"signature type", "type", "public", "e", "routine", "other", "h", "f", ""}},
			want:       `function "other"."h" takes or returns type "public"."e"`,
		},
		{
			name:       "a function that returns the table's rows",
			dependents: [][]driver.Value{{"signature type", "relation", "public", "t", "routine", "other", "f", "f", ""}},
			want:       `function "other"."f" takes or returns table "public"."t"`,
		},
		{
			name:       "a trigger whose function reads the table",
			dependents: [][]driver.Value{{"rule", "relation", "public", "t", "relation", "other", "ot", "r", ""}},
			want:       `table "other"."ot" depends on table "public"."t"`,
		},
		{
			name:  "a default that calls the function",
			texts: [][]driver.Value{{"default", "other", "dt", "r", "c", "public.fn()"}},
			want:  `the default of column "c" of table "other"."dt" names function "public"."fn"`,
		},
		{
			name:  "a computed column that calls the function",
			texts: [][]driver.Value{{"computed", "other", "dt", "r", "c", "public.fn ( ) + 1"}},
			want:  `the expression of computed column "c" of table "other"."dt" names function "public"."fn"`,
		},
		{
			name:  "an ON UPDATE expression that calls the function",
			texts: [][]driver.Value{{"on update", "other", "ou", "r", "f", "public.fn()"}},
			want:  `the ON UPDATE expression of column "f" of table "other"."ou" names function "public"."fn"`,
		},
		{
			name:  "a check constraint that calls the function",
			texts: [][]driver.Value{{"check", "other", "dt", "r", "check_d", "CHECK ((d < public.fn()))"}},
			want:  `check constraint "check_d" on table "other"."dt" names function "public"."fn"`,
		},
		{
			name:  "a check constraint that casts to the type",
			texts: [][]driver.Value{{"check", "other", "et", "r", "check_d", "CHECK ((d::public.e = 'a'::public.e))"}},
			want:  `check constraint "check_d" on table "other"."et" names type "public"."e"`,
		},
		{
			name:  "a default that names the table as a regclass",
			texts: [][]driver.Value{{"default", "other", "rt", "r", "c", "'public.t'::REGCLASS"}},
			want:  `the default of column "c" of table "other"."rt" names table "public"."t"`,
		},
		{
			name: "two dependencies, named in order",
			dependents: [][]driver.Value{
				{"trigger", "routine", "public", "fn", "relation", "other", "t2", "r", "tr"},
				{"rule", "relation", "public", "t", "relation", "other", "t2", "r", ""},
			},
			want: `table "other"."t2" depends on table "public"."t"; trigger "tr" on table "other"."t2" runs function ` +
				`"public"."fn"`,
		},
		{
			name:  "a default that names the sequence",
			texts: [][]driver.Value{{"default", "other", "t3", "r", "id", "nextval('public.s'::regclass)"}},
			want:  `the default of column "id" of table "other"."t3" names sequence "public"."s"`,
		},
		{
			name:  "a default that casts the sequence's OID",
			texts: [][]driver.Value{{"default", "other", "t3", "r", "id", "nextval(4208:::REGCLASS)"}},
			want:  `the default of column "id" of table "other"."t3" names sequence "public"."s"`,
		},
		{
			name:  "a body that reads the table",
			texts: [][]driver.Value{{"body", "other", "f", "f", "", "SELECT count(*)::INT8 FROM app.public.t;"}},
			want:  `the body of function "other"."f" names table "public"."t"`,
		},
		{
			name:  "a body that quotes the table's name",
			texts: [][]driver.Value{{"body", "other", "f", "f", "", `SELECT 1 FROM "app".public."t";`}},
			want:  `the body of function "other"."f" names table "public"."t"`,
		},
		{
			name:  "a procedure body that writes the table",
			texts: [][]driver.Value{{"body", "other", "pc", "p", "", "BEGIN INSERT INTO app.public.t VALUES (1); END;"}},
			want:  `the body of procedure "other"."pc" names table "public"."t"`,
		},
		{
			name:  "a body that casts the sequence's OID",
			texts: [][]driver.Value{{"body", "other", "k", "f", "", "SELECT nextval(4208:::REGCLASS)::INT8;"}},
			want:  `the body of function "other"."k" names sequence "public"."s"`,
		},
		{
			name:  "a body that names the type by OID",
			texts: [][]driver.Value{{"body", "other", "h", "f", "", "SELECT b'@':::@104221;"}},
			want:  `the body of function "other"."h" names type "public"."e"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				cockroachCleanupQuery("on", cockroachOutsideObjects, test.dependents,
					slices.Concat(cockroachOutsideSpellings, test.texts)),
				recordingExec(&statements, acceptingDrop),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, `refusing to clean schema "public": an object the cleanup does not drop `+
				`depends on one it drops: `+test.want+`; the server commits each drop as it runs, so the cleanup `+
				`refuses before it drops anything`)
			c.Assert(statements, qt.IsNil)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

// A dependency whose holder the cleanup drops too, as every holder in a dev or
// shadow database cleanup is, is no refusal, and neither is a text that names
// another object: the drops run.
func TestWriterDropAllTables_CockroachDropsWhatOnlyItsOwnObjectsDependOn_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dependents [][]driver.Value
		texts      [][]driver.Value
	}{
		{
			name:       "a foreign key between two of its tables",
			dependents: [][]driver.Value{{"foreign key", "relation", "public", "t", "relation", "public", "z_data", "r", "fk"}},
		},
		{
			name:       "a call from a function outside to one it does not list",
			dependents: [][]driver.Value{{"call", "routine", "public", "other_fn", "routine", "other", "g", "f", ""}},
		},
		{
			name:  "a body of a function it drops",
			texts: [][]driver.Value{{"body", "public", "fn", "f", "", "SELECT count(*)::INT8 FROM app.public.t;"}},
		},
		{
			name:  "a default of a table it drops",
			texts: [][]driver.Value{{"default", "public", "z_data", "r", "id", "nextval('public.s'::regclass)"}},
		},
		{
			name:  "a default that names a sequence of another schema",
			texts: [][]driver.Value{{"default", "other", "t3", "r", "id", "nextval('other.s'::regclass)"}},
		},
		{
			name:  "a body that names a longer table name",
			texts: [][]driver.Value{{"body", "other", "f", "f", "", "SELECT 1 FROM app.public.tx;"}},
		},
		{
			name:  "a body that names the table in another database",
			texts: [][]driver.Value{{"body", "other", "f", "f", "", "SELECT 1 FROM other_app.public.t;"}},
		},
		{
			name:  "a default that calls a function of a longer name",
			texts: [][]driver.Value{{"default", "other", "dt", "r", "c", "public.fn2()"}},
		},
		{
			name:  "a default that calls a function of another schema",
			texts: [][]driver.Value{{"default", "other", "dt", "r", "c", "other.fn()"}},
		},
		{
			name:  "a check constraint on a column named like the function",
			texts: [][]driver.Value{{"check", "other", "dt", "r", "check_fn", "CHECK ((fn > 0))"}},
		},
		{
			name:  "a check constraint that casts to a longer type name",
			texts: [][]driver.Value{{"check", "other", "et", "r", "check_d", "CHECK ((d::public.ex = 'a'::public.ex))"}},
		},
		{
			name:  "a check constraint of a table it drops",
			texts: [][]driver.Value{{"check", "public", "z_data", "r", "check_d", "CHECK ((d < public.fn()))"}},
		},
		{
			name:  "a body that names a longer type OID",
			texts: [][]driver.Value{{"body", "other", "h", "f", "", "SELECT b'@':::@1042210;"}},
		},
		{
			name:       "a function that returns the rows of a table it does not list",
			dependents: [][]driver.Value{{"signature type", "relation", "public", "kept", "routine", "other", "f", "f", ""}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				cockroachCleanupQuery("on", cockroachOutsideObjects, test.dependents,
					slices.Concat(cockroachOutsideSpellings, test.texts)),
				recordingExec(&statements, acceptingDrop),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				`DROP TABLE IF EXISTS "public"."t" RESTRICT`,
				`DROP TABLE IF EXISTS "public"."z_data" RESTRICT`,
				`DROP SEQUENCE IF EXISTS "public"."s" RESTRICT`,
				`DROP FUNCTION IF EXISTS "public"."fn"() RESTRICT`,
				`DROP TYPE IF EXISTS "public"."e" RESTRICT`,
			})
			c.Assert(db.CommitCount(), qt.Equals, 1)
		})
	}
}

// cleanupIndexTablesMarker is what the CockroachDB cleanup's read of the table
// of each index it lists selects, and no other statement does.
const cleanupIndexTablesMarker = "x.indexrelid"

// withIndexTables answers the read of the table of each index with rows, one
// per index: its schema, its name and its table's name, and every other query
// with next.
func withIndexTables(next dbtest.QueryHandler, rows [][]driver.Value) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(query, cleanupIndexTablesMarker) {
			return dbtest.QueryResult{Columns: []string{"nspname", "index", "table"}, Rows: rows}, nil
		}
		return next(query, args)
	}
}

// An index whose table the cleanup has dropped went with it, so the cleanup
// sends no drop for it and does not name it among the drops before a failure.
// An index whose table is still there is dropped, and named.
func TestWriterDropAllTables_CockroachNamesOnlyTheDropsThatRan_FailurePath(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t,
		withIndexTables(cockroachCleanupQuery("on", [][]driver.Value{
			{"table", "public", "a_keep", nil, `DROP TABLE IF EXISTS "public"."a_keep" RESTRICT`},
			{"table", "public", "m_shared", nil, `DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`},
			{"index", "public", "a_keep_pkey", nil, `DROP INDEX IF EXISTS "public"."a_keep_pkey" RESTRICT`},
			{"index", "public", "m_shared_idx", nil, `DROP INDEX IF EXISTS "public"."m_shared_idx" RESTRICT`},
		}, nil, nil), [][]driver.Value{
			{"public", "a_keep_pkey", "a_keep"},
			{"public", "m_shared_idx", "m_shared"},
		}),
		recordingExec(&statements, refusingDrop("m_shared")),
	)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals, `refusing to clean schema "public": failed to drop table m_shared: `+
		`SQL execution failed: cannot drop table m_shared because other objects depend on it`+"\n"+
		`SQL: DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`+"\n"+
		`the server commits each drop as it runs; dropped before the failure: table "public"."a_keep", `+
		`index "public"."m_shared_idx"`)
	c.Assert(statements, qt.DeepEquals, []string{
		`DROP TABLE IF EXISTS "public"."a_keep" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`,
		`DROP INDEX IF EXISTS "public"."m_shared_idx" RESTRICT`,
		`DROP TABLE IF EXISTS "public"."m_shared" RESTRICT`,
	})
}
