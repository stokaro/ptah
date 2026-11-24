package postgres_test

import (
	"database/sql/driver"
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

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

// cockroachTypeProbeStatements are what the cleanup of cockroachOutsideObjects
// sends to ask the server what depends on its type, when the server plans the
// drop.
var cockroachTypeProbeStatements = []string{
	"SAVEPOINT ptah_type_dependents",
	`EXPLAIN (DDL) DROP TYPE "public"."e"`,
	"RELEASE SAVEPOINT ptah_type_dependents",
}

// cockroachRefusedTypeProbeStatements are what the cleanup of
// cockroachOutsideObjects sends to ask the server what depends on its type,
// when the server refuses to plan the drop.
var cockroachRefusedTypeProbeStatements = []string{
	"SAVEPOINT ptah_type_dependents",
	`EXPLAIN (DDL) DROP TYPE "public"."e"`,
	"ROLLBACK TO SAVEPOINT ptah_type_dependents",
}

// refusingTypeExplain answers the EXPLAIN of a type's drop with err, and
// accepts every other statement.
func refusingTypeExplain(err error) dbtest.ExecHandler {
	return func(query string, _ []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(query, "EXPLAIN (DDL) DROP TYPE ") {
			return nil, err
		}
		return driver.RowsAffected(0), nil
	}
}

// typeDependentsRefusal is the refusal CockroachDB v26.4.1 gives to plan the
// drop of type e while the objects of list depend on it.
func typeDependentsRefusal(list string) error {
	return &pgconn.PgError{
		Severity: "ERROR",
		Code:     "2BP01",
		Message:  `cannot drop type "e" because other objects ([` + list + `]) still depend on it`,
	}
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
			want:  `the text of the body of function "other"."f" names table "public"."t"`,
		},
		{
			name:  "a body that quotes the table's name",
			texts: [][]driver.Value{{"body", "other", "f", "f", "", `SELECT 1 FROM "app".public."t";`}},
			want:  `the text of the body of function "other"."f" names table "public"."t"`,
		},
		{
			name:  "a procedure body that writes the table",
			texts: [][]driver.Value{{"body", "other", "pc", "p", "", "BEGIN INSERT INTO app.public.t VALUES (1); END;"}},
			want:  `the text of the body of procedure "other"."pc" names table "public"."t"`,
		},
		{
			name:  "a body that casts the sequence's OID",
			texts: [][]driver.Value{{"body", "other", "k", "f", "", "SELECT nextval(4208:::REGCLASS)::INT8;"}},
			want:  `the text of the body of function "other"."k" names sequence "public"."s"`,
		},
		{
			name:  "a body that names the type by OID",
			texts: [][]driver.Value{{"body", "other", "h", "f", "", "SELECT b'@':::@104221;"}},
			want:  `the text of the body of function "other"."h" names type "public"."e"`,
		},
		{
			name:  "a view that calls the function",
			texts: [][]driver.Value{{"view", "other", "vf", "v", "", "SELECT public.fn() AS n;"}},
			want:  `the definition of view "other"."vf" names function "public"."fn"`,
		},
		{
			name:  "a view that casts the sequence's OID",
			texts: [][]driver.Value{{"view", "other", "vs", "v", "", "SELECT nextval(4208:::REGCLASS) AS n;"}},
			want:  `the definition of view "other"."vs" names sequence "public"."s"`,
		},
		{
			name:  "a materialized view that names the type by OID",
			texts: [][]driver.Value{{"view", "other", "mv", "m", "", "SELECT b'@':::@104221 AS x;"}},
			want:  `the definition of materialized view "other"."mv" names type "public"."e"`,
		},
		{
			name:  "a policy USING expression that calls the function",
			texts: [][]driver.Value{{"policy using", "other", "ot", "r", "p", "public.fn() = 1:::INT8"}},
			want:  `the USING expression of policy "p" on table "other"."ot" names function "public"."fn"`,
		},
		{
			name:  "a policy WITH CHECK expression that casts to the type",
			texts: [][]driver.Value{{"policy check", "other", "ot", "r", "p", "c::public.e = 'a':::public.e"}},
			want:  `the WITH CHECK expression of policy "p" on table "other"."ot" names type "public"."e"`,
		},
		{
			name:  "a policy that takes from the sequence",
			texts: [][]driver.Value{{"policy using", "other", "ot", "r", "p", "nextval('public.s'::REGCLASS) > 0:::INT8"}},
			want:  `the USING expression of policy "p" on table "other"."ot" names sequence "public"."s"`,
		},
		{
			name: "an index expression that calls the function",
			texts: [][]driver.Value{{"index", "other", "ot", "r", "ix",
				"CREATE INDEX ix ON other.ot USING btree ((public.fn() + id) ASC)"}},
			want: `index "ix" on table "other"."ot" names function "public"."fn"`,
		},
		{
			name: "a partial index predicate that casts to the type",
			texts: [][]driver.Value{{"index", "other", "ot", "r", "ix",
				"CREATE INDEX ix ON other.ot USING btree (id ASC) WHERE (c::public.e = 'a'::public.e)"}},
			want: `index "ix" on table "other"."ot" names type "public"."e"`,
		},
		{
			name:  "a trigger WHEN condition that calls the function",
			texts: [][]driver.Value{{"trigger", "other", "ot", "r", "tr", "(public.fn() = 1:::INT8)"}},
			want:  `the WHEN condition of trigger "tr" on table "other"."ot" names function "public"."fn"`,
		},
		{
			name:  "a trigger WHEN condition that casts the sequence's OID",
			texts: [][]driver.Value{{"trigger", "other", "ot", "r", "tr", "(nextval(4208:::REGCLASS) > 0:::INT8)"}},
			want:  `the WHEN condition of trigger "tr" on table "other"."ot" names sequence "public"."s"`,
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
			c.Assert(statements, qt.DeepEquals, cockroachTypeProbeStatements)
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
			name:  "a view that names a longer type OID",
			texts: [][]driver.Value{{"view", "other", "vt", "v", "", "SELECT b'@':::@1042210 AS x;"}},
		},
		{
			name:  "a definition held by a relation it drops",
			texts: [][]driver.Value{{"view", "public", "t", "v", "", "SELECT public.fn() AS n;"}},
		},
		{
			name: "an index predicate that casts to a longer type name",
			texts: [][]driver.Value{{"index", "other", "ot", "r", "ix",
				"CREATE INDEX ix ON other.ot USING btree (id ASC) WHERE (c::public.ex = 'a'::public.ex)"}},
		},
		{
			name:  "a policy that calls a function of another schema",
			texts: [][]driver.Value{{"policy using", "other", "ot", "r", "p", "other.fn() = 1:::INT8"}},
		},
		{
			name:  "a trigger WHEN condition that calls a function of a longer name",
			texts: [][]driver.Value{{"trigger", "other", "ot", "r", "tr", "(public.fn2() = 1:::INT8)"}},
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
			c.Assert(statements, qt.DeepEquals, slices.Concat(cockroachTypeProbeStatements, cockroachOutsideDrops))
			c.Assert(db.CommitCount(), qt.Equals, 1)
		})
	}
}

// cockroachOutsideDrops are the drops of cockroachOutsideObjects, in the order
// the cleanup sends them.
var cockroachOutsideDrops = []string{
	`DROP TABLE IF EXISTS "public"."t" RESTRICT`,
	`DROP TABLE IF EXISTS "public"."z_data" RESTRICT`,
	`DROP SEQUENCE IF EXISTS "public"."s" RESTRICT`,
	`DROP FUNCTION IF EXISTS "public"."fn"() RESTRICT`,
	`DROP TYPE IF EXISTS "public"."e" RESTRICT`,
}

// The server's own record of what uses a type the cleanup drops reaches the
// refusal when no catalog text shows it: CockroachDB folds `NULL::public.e[]`
// in a view to `NULL` and still refuses the type's drop. Only the objects
// outside the cleanup are named, without the database, and a holder another
// sentence already names for that type is not named twice.
func TestWriterDropAllTables_CockroachRefusesWhatTheServerReportsOnAType_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		dependents [][]driver.Value
		list       string
		want       string
	}{
		{
			name: "a view whose text lost the type",
			list: "app.public.t app.other.va",
			want: `CockroachDB records that type "public"."e" is used by other.va`,
		},
		{
			name: "two outside objects, the list separated by commas",
			list: "app.public.t, app.other.va, app.other.fa",
			want: `CockroachDB records that type "public"."e" is used by other.va other.fa`,
		},
		{
			name: "a name with a space",
			list: "app.public.z_data app.Other Schema.V x",
			want: `CockroachDB records that type "public"."e" is used by Other Schema.V x`,
		},
		{
			name:       "a holder another sentence names, and one it does not",
			dependents: [][]driver.Value{{"column type", "type", "public", "e", "relation", "other", "t4", "r", "c"}},
			list:       "app.other.t4 app.public.t app.other.va",
			want: `column "c" of table "other"."t4" has type "public"."e"; CockroachDB records that type ` +
				`"public"."e" is used by other.va`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				cockroachCleanupQuery("on", cockroachOutsideObjects, test.dependents, cockroachOutsideSpellings),
				recordingExec(&statements, refusingTypeExplain(typeDependentsRefusal(test.list))),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, `refusing to clean schema "public": an object the cleanup does not drop `+
				`depends on one it drops: `+test.want+`; the server commits each drop as it runs, so the cleanup `+
				`refuses before it drops anything`)
			c.Assert(statements, qt.DeepEquals, cockroachRefusedTypeProbeStatements)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

// A type the server reports only the cleanup's own objects as using, or a
// holder another sentence of the refusal already names, adds nothing, and a
// type nothing uses is planned: the drops run.
func TestWriterDropAllTables_CockroachDropsATypeOnlyItsOwnObjectsUse_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		explainErr     error
		wantStatements []string
	}{
		{
			name:           "its own tables",
			explainErr:     typeDependentsRefusal("app.public.t app.public.z_data"),
			wantStatements: slices.Concat(cockroachRefusedTypeProbeStatements, cockroachOutsideDrops),
		},
		{
			name:           "its own tables, separated by commas",
			explainErr:     typeDependentsRefusal("app.public.t, app.public.z_data"),
			wantStatements: slices.Concat(cockroachRefusedTypeProbeStatements, cockroachOutsideDrops),
		},
		{
			name:           "nothing",
			wantStatements: slices.Concat(cockroachTypeProbeStatements, cockroachOutsideDrops),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				cockroachCleanupQuery("on", cockroachOutsideObjects, nil, cockroachOutsideSpellings),
				recordingExec(&statements, refusingTypeExplain(test.explainErr)),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.wantStatements)
			c.Assert(db.CommitCount(), qt.Equals, 1)
		})
	}
}

// A refusal to plan a type's drop that does not list what depends on it, or
// that is not about dependents at all, refuses the cleanup before its first
// drop: nothing then says which objects use the type.
func TestWriterDropAllTables_CockroachRefusesWhenItCannotAskAboutAType_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		explainErr error
		wantErr    string
	}{
		{
			name: "a refusal without the list",
			explainErr: &pgconn.PgError{Severity: "ERROR", Code: "2BP01",
				Message: `cannot drop type "e" because other objects depend on it`},
			wantErr: `refusing to clean schema "public": failed to read what depends on type "public"."e" from the ` +
				`server's refusal: ERROR: cannot drop type "e" because other objects depend on it (SQLSTATE 2BP01)`,
		},
		{
			name: "a refusal of the privilege",
			explainErr: &pgconn.PgError{Severity: "ERROR", Code: "42501",
				Message: `user app does not have DROP privilege on type e`},
			wantErr: `refusing to clean schema "public": failed to ask the server what depends on type "public"."e": ` +
				`ERROR: user app does not have DROP privilege on type e (SQLSTATE 42501)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				cockroachCleanupQuery("on", cockroachOutsideObjects, nil, cockroachOutsideSpellings),
				recordingExec(&statements, refusingTypeExplain(test.explainErr)),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(statements, qt.DeepEquals, cockroachRefusedTypeProbeStatements)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

// cockroachOnUpdateUnread is the row the text read adds on a server whose
// information_schema.columns has no column_on_update, as v25.4.21.
var cockroachOnUpdateUnread = []driver.Value{"on update unread", "", "", "", "", ""}

// withCreateStatements answers SHOW CREATE ALL TABLES with statements, one
// row each, or with err, and every other query with next.
func withCreateStatements(next dbtest.QueryHandler, statements []string, err error) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if query != "SHOW CREATE ALL TABLES" {
			return next(query, args)
		}
		rows := make([][]driver.Value, 0, len(statements))
		for _, statement := range statements {
			rows = append(rows, []driver.Value{statement})
		}
		return dbtest.QueryResult{Columns: []string{"create_statement"}, Rows: rows}, err
	}
}

// On a server whose information_schema.columns has no column_on_update, an ON
// UPDATE expression outside the cleanup is read from the CREATE statement of
// its table, as SHOW CREATE ALL TABLES prints it on v25.4.21, and refused
// before the first drop.
func TestWriterDropAllTables_CockroachRefusesAnONUPDATEFromTheCreateStatements_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name: "a call to the function",
			statement: "CREATE TABLE other.ou (\n\tid INT8 NOT NULL,\n\tf INT8 NULL ON UPDATE public.fn(),\n" +
				"\tCONSTRAINT ou_pkey PRIMARY KEY (id ASC)\n);",
			want: `the ON UPDATE expression of column "f" of table "other"."ou" names function "public"."fn"`,
		},
		{
			name: "the sequence",
			statement: "CREATE TABLE other.ou (\n\tid INT8 NOT NULL,\n" +
				"\tg INT8 NULL ON UPDATE nextval('public.s'::REGCLASS),\n\tCONSTRAINT ou_pkey PRIMARY KEY (id ASC)\n);",
			want: `the ON UPDATE expression of column "g" of table "other"."ou" names sequence "public"."s"`,
		},
		{
			name: "quoted names",
			statement: "CREATE TABLE \"Odd S\".\"T x\" (\n\t\"Col \"\"q\"\"\" INT8 NOT VISIBLE NULL DEFAULT 1:::INT8 " +
				"ON UPDATE public.fn(),\n\tid INT8 NOT NULL,\n\tCONSTRAINT \"T x_pkey\" PRIMARY KEY (id ASC)\n);",
			want: `the ON UPDATE expression of column "Col \"q\"" of table "Odd S"."T x" names function "public"."fn"`,
		},
		{
			name: "an expression whose string holds the keyword",
			statement: "CREATE TABLE other.c (\n\tid INT8 NOT NULL,\n" +
				"\td STRING NULL ON UPDATE public.fn()::STRING || ' ON UPDATE x':::STRING,\n" +
				"\tCONSTRAINT c_pkey PRIMARY KEY (id ASC)\n);",
			want: `the ON UPDATE expression of column "d" of table "other"."c" names function "public"."fn"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				withCreateStatements(
					cockroachCleanupQuery("on", cockroachOutsideObjects, nil,
						slices.Concat(cockroachOutsideSpellings, [][]driver.Value{cockroachOnUpdateUnread})),
					[]string{"CREATE SEQUENCE public.s MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1;",
						test.statement},
					nil,
				),
				recordingExec(&statements, acceptingDrop),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, `refusing to clean schema "public": an object the cleanup does not drop `+
				`depends on one it drops: `+test.want+`; the server commits each drop as it runs, so the cleanup `+
				`refuses before it drops anything`)
			c.Assert(statements, qt.DeepEquals, cockroachTypeProbeStatements)
			c.Assert(db.CommitCount(), qt.Equals, 0)
			c.Assert(db.RollbackCount(), qt.Equals, 1)
		})
	}
}

// A CREATE statement whose ON UPDATE names nothing the cleanup drops from
// outside it adds nothing, and a server whose information_schema.columns has
// column_on_update is not asked for the statements at all: the drops run.
func TestWriterDropAllTables_CockroachDropsWhatNoONUPDATEOutsideNames_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		texts     [][]driver.Value
		statement string
	}{
		{
			name:  "a server that carries column_on_update",
			texts: cockroachOutsideSpellings,
			statement: "CREATE TABLE other.ou (\n\tid INT8 NOT NULL,\n\tf INT8 NULL ON UPDATE public.fn(),\n" +
				"\tCONSTRAINT ou_pkey PRIMARY KEY (id ASC)\n);",
		},
		{
			name:  "a table the cleanup drops",
			texts: slices.Concat(cockroachOutsideSpellings, [][]driver.Value{cockroachOnUpdateUnread}),
			statement: "CREATE TABLE public.t (\n\tid INT8 NOT NULL,\n\tf INT8 NULL ON UPDATE public.fn(),\n" +
				"\tCONSTRAINT t_pkey PRIMARY KEY (id ASC)\n);",
		},
		{
			name:  "another function",
			texts: slices.Concat(cockroachOutsideSpellings, [][]driver.Value{cockroachOnUpdateUnread}),
			statement: "CREATE TABLE other.ou (\n\tid INT8 NOT NULL,\n\tf INT8 NULL ON UPDATE public.fn2(),\n" +
				"\tCONSTRAINT ou_pkey PRIMARY KEY (id ASC)\n);",
		},
		{
			name:  "a comment that quotes the function",
			texts: slices.Concat(cockroachOutsideSpellings, [][]driver.Value{cockroachOnUpdateUnread}),
			statement: "CREATE TABLE other.c (\n\tid INT8 NOT NULL,\n\tCONSTRAINT c_pkey PRIMARY KEY (id ASC)\n);\n" +
				"COMMENT ON TABLE other.c IS 'set ON UPDATE public.fn()';",
		},
		{
			name:  "a foreign key that cascades its updates",
			texts: slices.Concat(cockroachOutsideSpellings, [][]driver.Value{cockroachOnUpdateUnread}),
			statement: "ALTER TABLE other.c ADD CONSTRAINT c_x_fkey FOREIGN KEY (x) REFERENCES other.ou(id) " +
				"ON DELETE CASCADE ON UPDATE CASCADE;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var statements []string
			db := dbtest.OpenWithExec(t,
				withCreateStatements(cockroachCleanupQuery("on", cockroachOutsideObjects, nil, test.texts),
					[]string{test.statement}, nil),
				recordingExec(&statements, acceptingDrop),
			)
			writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

			err := writer.DropAllTables(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, slices.Concat(cockroachTypeProbeStatements, cockroachOutsideDrops))
			c.Assert(db.CommitCount(), qt.Equals, 1)
		})
	}
}

// A server that cannot list the CREATE statements, where they are the only
// place its ON UPDATE expressions show, refuses the cleanup before its first
// drop.
func TestWriterDropAllTables_CockroachRefusesWhenItCannotReadTheCreateStatements_FailurePath(t *testing.T) {
	c := qt.New(t)
	var statements []string
	db := dbtest.OpenWithExec(t,
		withCreateStatements(
			cockroachCleanupQuery("on", cockroachOutsideObjects, nil,
				slices.Concat(cockroachOutsideSpellings, [][]driver.Value{cockroachOnUpdateUnread})),
			nil,
			errors.New("user app has no privileges on the database"),
		),
		recordingExec(&statements, acceptingDrop),
	)
	writer := postgres.NewPostgreSQLWriter(db.SQL, "public")

	err := writer.DropAllTables(t.Context())

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`refusing to clean schema "public": failed to read the ON UPDATE `+
		`expressions outside the cleanup: user app has no privileges on the database`))
	c.Assert(statements, qt.HasLen, 0)
	c.Assert(db.CommitCount(), qt.Equals, 0)
	c.Assert(db.RollbackCount(), qt.Equals, 1)
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
