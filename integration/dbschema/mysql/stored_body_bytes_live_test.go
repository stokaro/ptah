//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/selectext"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlrunner"
)

// information_schema reports a trigger's or a routine's body rewritten: MySQL
// writes a string literal's or a quoted name's value between its quotes,
// MariaDB writes a literal again with its own escapes, which loses a
// backslash, and both drop the character set introducer of a hex or a bit
// literal. The reader takes every body from SHOW CREATE instead. Each test creates the
// objects on one database, reads it, creates each object again from the body
// it read on a second database, and compares the two through the server.

// storedBodyEngines are the servers the tests run on.
var storedBodyEngines = []struct {
	name    string
	engine  dbtarget.Engine
	dialect string
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin, dialect: platform.MySQL},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin, dialect: platform.MariaDB},
}

// storedObject is one trigger, function or procedure. create is the
// statement that creates it, with %s for its body; prepare runs before fire,
// which returns the value the body computes.
type storedObject struct {
	name    string
	kind    string
	table   string
	create  string
	body    string
	prepare []string
	fire    string
}

// trigger is a trigger named name on its own table, whose body sets the
// columns of the inserted row: code, and one whose name holds a backquote.
func trigger(name, body string) storedObject {
	return triggerSelecting(name, body, "SELECT CONCAT_WS('|', code, `a``b`) FROM t_"+name)
}

// triggerSelecting is [trigger] for a body whose value fire selects.
func triggerSelecting(name, body, fire string) storedObject {
	table := "t_" + name
	return storedObject{
		name: name, kind: "TRIGGER", table: table, body: body,
		create:  "CREATE TRIGGER " + name + " BEFORE INSERT ON " + table + " FOR EACH ROW %s",
		prepare: []string{"INSERT INTO " + table + " (code) VALUES ('')"},
		fire:    fire,
	}
}

// function is a function named name whose body returns the value.
func function(name, body string) storedObject {
	return functionOf(name, "", body, name+"()")
}

// functionOf is a function named name taking parameters, whose body returns
// the value call selects.
func functionOf(name, parameters, body, call string) storedObject {
	return storedObject{
		name: name, kind: "FUNCTION", body: body,
		create: "CREATE FUNCTION " + name + "(" + parameters + ") RETURNS varchar(200) DETERMINISTIC %s",
		fire:   "SELECT " + call,
	}
}

// procedure is a procedure named name whose body sets @v.
func procedure(name, body string) storedObject {
	return storedObject{
		name: name, kind: "PROCEDURE", body: body,
		create:  "CREATE PROCEDURE " + name + "() %s",
		prepare: []string{"CALL " + name + "()"},
		fire:    "SELECT @v",
	}
}

// writtenBodies are bodies created under the reading session's sql_mode. Each
// but tr_plain holds a literal or a name information_schema reports rewritten
// on MySQL, MariaDB or both, and from tr_quoted_name on none holds a quote.
var writtenBodies = []storedObject{
	trigger("tr_quote", `SET NEW.code = 'a\'b'`),
	trigger("tr_dquote", `SET NEW.code = 'a\"b'`),
	trigger("tr_backslash", `SET NEW.code = 'a\\b'`),
	trigger("tr_newline_tab", `SET NEW.code = 'a\nb\tc'`),
	trigger("tr_nul", `SET NEW.code = 'a\0b'`),
	trigger("tr_like", `SET NEW.code = 'a\%b\_c'`),
	trigger("tr_letter", `SET NEW.code = 'a\qb'`),
	trigger("tr_doubled", `SET NEW.code = 'a''b'`),
	trigger("tr_double_quoted", `SET NEW.code = "a\"b'c"`),
	trigger("tr_block", "BEGIN\n  -- it's a note\n  SET NEW.code = CONCAT('x\\'', \"y\\\"\");\nEND"),
	function("f_escapes", `RETURN 'a\'b\0c\\d\qe'`),
	function("f_double_quoted", `RETURN "q\"r"`),
	procedure("p_escapes", `BEGIN SET @v = 'a\'b\\c\nd'; END`),
	trigger("tr_quoted_name", "SET NEW.`a``b` = 1"),
	triggerSelecting("tr_quoted_variable", "SET @`x``y` = 1", "SELECT @`x``y`"),
	functionOf("f_quoted_parameter", "`p``q` int", "RETURN `p``q` + 1", "f_quoted_parameter(1)"),
	trigger("tr_latin1_hex", "SET NEW.code = _latin1 0xE9"),
	trigger("tr_binary_hex", "SET NEW.code = _binary 0x41"),
	trigger("tr_latin1_bits", "SET NEW.code = _latin1 0b11101001"),
	trigger("tr_convert", "SET NEW.code = CONVERT(_utf8mb4 0xC3A9 USING latin1)"),
	trigger("tr_plain", "SET NEW.code = 1"),
}

func TestStoredBodies_LiveReadsTheBodyAsWritten(t *testing.T) {
	for _, engine := range storedBodyEngines {
		for _, object := range writtenBodies {
			t.Run(engine.name+"/"+object.name, func(t *testing.T) {
				c := qt.New(t)
				admin := openMySQLWriterLiveDatabase(c, engine.engine)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				source := storedBodyDatabase(c, admin, "ptah_body_src")
				copied := storedBodyDatabase(c, admin, "ptah_body_copy")
				createStoredObject(c, admin, source, "", object, object.body)

				read := readStoredBodies(c, engine.engine, engine.dialect, source)
				createStoredObject(c, admin, copied, "", object, read[object.name])

				c.Assert(read[object.name], qt.Equals, object.body)
				c.Assert(showCreate(c, admin, copied, object), qt.Equals, showCreate(c, admin, source, object))
				c.Assert(fire(c, admin, copied, object), qt.Equals, fire(c, admin, source, object))
			})
		}
	}
}

// modeBodies are bodies created under a sql_mode that reads a backslash or a
// double quote otherwise than the reading session does. want is the body read
// on that session, written for it.
var modeBodies = []struct {
	object storedObject
	mode   string
	want   string
}{
	{
		object: trigger("tr_no_escapes", `SET NEW.code = 'a\b''c'`), mode: "NO_BACKSLASH_ESCAPES",
		want: `SET NEW.code = 'a\\b''c'`,
	},
	{
		object: trigger("tr_ansi", `SET NEW."code" = 'a\'b'`), mode: "ANSI_QUOTES",
		want: "SET NEW.`code` = 'a\\'b'",
	},
	{
		object: function("f_no_escapes", `RETURN 'a\b\'`), mode: "NO_BACKSLASH_ESCAPES",
		want: `RETURN 'a\\b\\'`,
	},
}

func TestStoredBodies_LiveWritesABodyStoredUnderAnotherModeForTheSession(t *testing.T) {
	for _, engine := range storedBodyEngines {
		for _, row := range modeBodies {
			t.Run(engine.name+"/"+row.object.name, func(t *testing.T) {
				c := qt.New(t)
				admin := openMySQLWriterLiveDatabase(c, engine.engine)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				source := storedBodyDatabase(c, admin, "ptah_body_src")
				copied := storedBodyDatabase(c, admin, "ptah_body_copy")
				createStoredObject(c, admin, source, row.mode, row.object, row.object.body)

				read := readStoredBodies(c, engine.engine, engine.dialect, source)
				createStoredObject(c, admin, copied, "", row.object, read[row.object.name])

				c.Assert(read[row.object.name], qt.Equals, row.want)
				c.Assert(fire(c, admin, copied, row.object), qt.Equals, fire(c, admin, source, row.object))
			})
		}
	}
}

// recordingRunner is a [sql.DB] that records each query it is asked. It is
// not a [sql.DB] itself, so the reader sends its statements one at a time.
type recordingRunner struct {
	*sql.DB
	mu      sync.Mutex
	queries []string
}

func (r *recordingRunner) record(query string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, query)
}

func (r *recordingRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	r.record(query)
	return r.DB.QueryContext(ctx, query, args...)
}

func (r *recordingRunner) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	r.record(query)
	return r.DB.QueryRowContext(ctx, query, args...)
}

// The reader sends SHOW CREATE for every trigger and routine body, tr_plain's
// too, and for nothing the run's selectors leave out: an excluded table takes
// its trigger with it, and an included table leaves out every routine.
func TestStoredBodies_LiveReadsOnlyTheBodiesTheRunKeeps(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		include []string
		want    []string
	}{
		{name: "no selector", want: []string{"f_keep", "f_skip", "tr_keep", "tr_plain", "tr_skip"}},
		{name: "an excluded table", exclude: []string{"t_tr_skip"}, want: []string{"f_keep", "f_skip", "tr_keep", "tr_plain"}},
		{
			name: "an excluded trigger", exclude: []string{"t_tr_keep.tr_keep[type=trigger]"},
			want: []string{"f_keep", "f_skip", "tr_plain", "tr_skip"},
		},
		{
			name: "an excluded function", exclude: []string{"f_skip[type=function]"},
			want: []string{"f_keep", "tr_keep", "tr_plain", "tr_skip"},
		},
		{name: "an included table", include: []string{"t_tr_keep"}, want: []string{"tr_keep"}},
		{name: "every trigger excluded", exclude: []string{"*[type=trigger]"}, want: []string{"f_keep", "f_skip"}},
		{name: "every function excluded", exclude: []string{"*[type=function]"}, want: []string{"tr_keep", "tr_plain", "tr_skip"}},
	}
	for _, engine := range storedBodyEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				admin := openMySQLWriterLiveDatabase(c, engine.engine)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				source := storedBodyDatabase(c, admin, "ptah_body_scope")
				for _, object := range []storedObject{
					trigger("tr_keep", `SET NEW.code = 'a\'b'`),
					trigger("tr_skip", `SET NEW.code = 'a\'b'`),
					trigger("tr_plain", `SET NEW.code = 1`),
					function("f_keep", "RETURN 1"),
					function("f_skip", "RETURN 2"),
				} {
					createStoredObject(c, admin, source, "", object, object.body)
				}
				db, err := sql.Open("mysql", storedBodyDSN(c, engine.engine, source))
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
				runner := &recordingRunner{DB: db}
				ctx := atlasfilter.ReadScope(c.Context(), selectext.None(), test.exclude, test.include)

				_, err = mysql.NewMySQLReader(runner, "").ReadSchemaContext(ctx)

				c.Assert(err, qt.IsNil)
				c.Assert(showCreateNames(runner.queries, source), qt.DeepEquals, test.want)
			})
		}
	}
}

// A read of the whole server names each object with its database, and a
// selector that names one database's table or function leaves out that
// database's trigger or function alone.
func TestStoredBodies_LiveReadsOnlyTheBodiesAServerReadKeeps(t *testing.T) {
	for _, engine := range storedBodyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openMySQLWriterLiveDatabase(c, engine.engine)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			kept := storedBodyDatabase(c, admin, "ptah_body_realm_a")
			narrowed := storedBodyDatabase(c, admin, "ptah_body_realm_b")
			for _, database := range []string{kept, narrowed} {
				for _, object := range []storedObject{
					trigger("tr_keep", "SET NEW.code = 1"),
					trigger("tr_skip", "SET NEW.code = 2"),
					function("f_keep", "RETURN 1"),
					function("f_skip", "RETURN 2"),
				} {
					createStoredObject(c, admin, database, "", object, object.body)
				}
			}
			db, err := sql.Open("mysql", storedBodyDSN(c, engine.engine, ""))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			runner := &recordingRunner{DB: db}
			reader := mysql.NewMySQLReader(runner, "")
			reader.SetSchemas([]string{kept, narrowed})
			exclude := []string{narrowed + ".t_tr_skip", narrowed + ".f_skip[type=function]"}
			ctx := atlasfilter.ReadScope(c.Context(), selectext.None(), exclude, nil)

			_, err = reader.ReadSchemaContext(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(showCreateNames(runner.queries, kept), qt.DeepEquals, []string{"f_keep", "f_skip", "tr_keep", "tr_skip"})
			c.Assert(showCreateNames(runner.queries, narrowed), qt.DeepEquals, []string{"f_keep", "tr_keep"})
		})
	}
}

// showCreateNames returns the names of the objects of database the queries
// asked SHOW CREATE TRIGGER or SHOW CREATE FUNCTION about, sorted.
func showCreateNames(queries []string, database string) []string {
	var names []string
	for _, query := range queries {
		for _, kind := range []string{"TRIGGER", "FUNCTION"} {
			if name, asked := strings.CutPrefix(query, "SHOW CREATE "+kind+" `"+database+"`.`"); asked {
				names = append(names, strings.TrimSuffix(name, "`"))
			}
		}
	}
	slices.Sort(names)
	return names
}

// droppingRunner is a [sql.DB] that drops each trigger and routine before
// SHOW CREATE asks about it, or only the one named only, as another session
// can between the catalog read and the statement.
type droppingRunner struct {
	*sql.DB
	only string
}

func (r droppingRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	for _, kind := range []string{"TRIGGER", "FUNCTION"} {
		object, asked := strings.CutPrefix(query, "SHOW CREATE "+kind+" ")
		if !asked || (r.only != "" && !strings.HasSuffix(object, ".`"+r.only+"`")) {
			continue
		}
		if _, err := r.DB.ExecContext(ctx, "DROP "+kind+" "+object); err != nil {
			return nil, err
		}
	}
	return r.DB.QueryContext(ctx, query, args...)
}

// An object dropped between the catalog read and its SHOW CREATE fails the
// read, which names it and says a second read is the remedy.
func TestStoredBodies_LiveRefusesAnObjectDroppedDuringTheRead(t *testing.T) {
	tests := []struct {
		name    string
		object  storedObject
		wantErr string
		code    uint16
	}{
		{
			name:    "a trigger",
			object:  trigger("tr_gone", "SET NEW.code = 1"),
			wantErr: `failed to read the body of trigger tr_gone: it no longer exists: it was dropped after the catalog listed it, so read the schema again: Error 1360 .*`,
			code:    1360,
		},
		{
			name:    "a function",
			object:  function("f_gone", "RETURN 1"),
			wantErr: `failed to read the body of function f_gone: it no longer exists: it was dropped after the catalog listed it, so read the schema again: Error 1305 .*`,
			code:    1305,
		},
	}
	for _, engine := range storedBodyEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				admin := openMySQLWriterLiveDatabase(c, engine.engine)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				source := storedBodyDatabase(c, admin, "ptah_body_gone")
				createStoredObject(c, admin, source, "", test.object, test.object.body)
				db, err := sql.Open("mysql", storedBodyDSN(c, engine.engine, source))
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

				schema, err := mysql.NewMySQLReader(droppingRunner{DB: db}, "").ReadSchemaContext(c.Context())

				c.Assert(err, qt.ErrorMatches, test.wantErr)
				var serverErr *mysqldriver.MySQLError
				c.Assert(err, qt.ErrorAs, &serverErr)
				c.Assert(serverErr.Number, qt.Equals, test.code)
				c.Assert(schema, qt.IsNil)
			})
		}
	}
}

// MariaDB holds triggers whose names differ only in case or accent side by
// side, and information_schema compares trigger names under
// utf8mb3_general_ci, so a lookup by that comparison finds the siblings of a
// dropped trigger. The read still says the trigger was dropped. MySQL refuses
// such siblings (error 1359), so the test runs on MariaDB alone.
func TestStoredBodies_LiveRefusesATriggerDroppedBesideASiblingOfAnotherCase(t *testing.T) {
	c := qt.New(t)
	admin := openMySQLWriterLiveDatabase(c, dbtarget.MariaDBAdmin)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	source := storedBodyDatabase(c, admin, "ptah_body_sibling")
	session := storedBodySession(c, admin, source)
	for _, statement := range []string{
		"CREATE TABLE t (code varchar(20))",
		"CREATE TRIGGER tr_e BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'e'",
		"CREATE TRIGGER TR_E BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'E'",
		"CREATE TRIGGER `tr_\u00e9` BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
	} {
		_, err := session.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	db, err := sql.Open("mysql", storedBodyDSN(c, dbtarget.MariaDBAdmin, source))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	schema, err := mysql.NewMySQLReader(droppingRunner{DB: db, only: "tr_\u00e9"}, "").ReadSchemaContext(c.Context())

	c.Assert(err, qt.ErrorMatches, `failed to read the body of trigger tr_\x{e9}: it no longer exists: it was dropped after the `+
		`catalog listed it, so read the schema again: Error 1360 .*`)
	c.Assert(schema, qt.IsNil)
}

// MariaDB with lower_case_table_names=0 holds a database beside one whose
// name differs only in case, and information_schema.TRIGGERS compares
// TRIGGER_SCHEMA without case, so a lookup by that comparison finds the
// trigger of the same name in the other database. The read still says the
// trigger was dropped. MySQL compares TRIGGER_SCHEMA exactly, so the test
// runs on MariaDB alone.
func TestStoredBodies_LiveRefusesATriggerDroppedBesideADatabaseOfAnotherCase(t *testing.T) {
	c := qt.New(t)
	admin := openMySQLWriterLiveDatabase(c, dbtarget.MariaDBAdmin)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	source := storedBodyDatabase(c, admin, "ptah_body_rvs")
	sibling := strings.ToUpper(source)
	createMySQLWriterLiveDatabase(c, admin, sibling)
	c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, admin, sibling) })
	object := trigger("tr_x", "SET NEW.code = 1")
	createStoredObject(c, admin, source, "", object, object.body)
	createStoredObject(c, admin, sibling, "", object, object.body)
	db, err := sql.Open("mysql", storedBodyDSN(c, dbtarget.MariaDBAdmin, source))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })

	schema, err := mysql.NewMySQLReader(droppingRunner{DB: db, only: "tr_x"}, "").ReadSchemaContext(c.Context())

	c.Assert(err, qt.ErrorMatches, `failed to read the body of trigger tr_x: it no longer exists: it was dropped after the `+
		`catalog listed it, so read the schema again: Error 1360 .*`)
	c.Assert(schema, qt.IsNil)
}

// reappearingRunner is a [sql.DB] that drops the trigger named name before
// SHOW CREATE asks about it and runs create once SHOW CREATE has answered, as
// two other sessions can between the catalog read, the statement and the
// lookup that follows it.
type reappearingRunner struct {
	*sql.DB
	name   string
	create string
}

func (r reappearingRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	object, asked := strings.CutPrefix(query, "SHOW CREATE TRIGGER ")
	if !asked || !strings.HasSuffix(object, ".`"+r.name+"`") {
		return r.DB.QueryContext(ctx, query, args...)
	}
	if _, err := r.DB.ExecContext(ctx, "DROP TRIGGER "+object); err != nil {
		return nil, err
	}
	rows, err := r.DB.QueryContext(ctx, query, args...)
	if _, createErr := r.DB.ExecContext(ctx, r.create); createErr != nil {
		return nil, errors.Join(err, createErr)
	}
	return rows, err
}

// A trigger created again after SHOW CREATE missed it and before the read
// asked information_schema about it is still listed, and the read says so
// rather than that it was dropped. The pool reads names in latin1, so the
// lookup finds the trigger only by the name the session means, its latin1
// byte converted to utf8mb4, and not by the byte it sent.
func TestStoredBodies_LiveSaysATriggerCreatedAgainIsStillListed(t *testing.T) {
	for _, engine := range storedBodyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openMySQLWriterLiveDatabase(c, engine.engine)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			source := storedBodyDatabase(c, admin, "ptah_body_again")
			object := trigger("tr_\u00e9", "SET NEW.code = 1")
			createStoredObject(c, admin, source, "", object, object.body)
			db, err := sql.Open("mysql", storedBodyDSN(c, engine.engine, source))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			db.SetMaxOpenConns(1)
			_, err = db.ExecContext(c.Context(), "SET NAMES latin1")
			c.Assert(err, qt.IsNil)
			runner := reappearingRunner{
				DB:     db,
				name:   "tr_\xe9",
				create: "CREATE TRIGGER `tr_\xe9` BEFORE INSERT ON `t_tr_\xe9` FOR EACH ROW SET NEW.code = 2",
			}

			schema, err := mysql.NewMySQLReader(runner, "").ReadSchemaContext(c.Context())

			c.Assert(err, qt.ErrorMatches, `failed to read the body of trigger tr_.: the server did not find it under the `+
				`session's character set, latin1, though information_schema on that session still lists it: Error 1360 .*`)
			c.Assert(schema, qt.IsNil)
		})
	}
}

// recreatingRunner is a [sql.DB] that drops object and creates it again under
// PIPES_AS_CONCAT on session before SHOW CREATE asks about it, as another
// session can between the catalog read and the statement.
type recreatingRunner struct {
	*sql.DB
	session *sql.Conn
	object  storedObject
}

func (r recreatingRunner) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.HasPrefix(query, "SHOW CREATE "+r.object.kind+" ") {
		for _, statement := range []string{
			"DROP " + r.object.kind + " " + r.object.name,
			"SET SESSION sql_mode = CONCAT_WS(',', @@SESSION.sql_mode, 'PIPES_AS_CONCAT')",
			fmt.Sprintf(r.object.create, r.object.body),
		} {
			if _, err := r.session.ExecContext(ctx, statement); err != nil {
				return nil, err
			}
		}
	}
	return r.DB.QueryContext(ctx, query, args...)
}

// An object created again under PIPES_AS_CONCAT between the catalog read and
// its SHOW CREATE is refused as one the catalog lists under that mode is: its
// || is an OR the read would hand on as a concatenation.
func TestStoredBodies_LiveRefusesAnObjectCreatedAgainUnderARefusedMode(t *testing.T) {
	tests := []struct {
		name   string
		object storedObject
		kind   string
	}{
		{name: "a trigger", object: trigger("tr_mode", "SET NEW.code = 'a' || 'b'"), kind: "trigger"},
		{name: "a function", object: function("f_mode", "RETURN 'a' || 'b'"), kind: "function"},
	}
	for _, engine := range storedBodyEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				admin := openMySQLWriterLiveDatabase(c, engine.engine)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				source := storedBodyDatabase(c, admin, "ptah_body_mode")
				createStoredObject(c, admin, source, "", test.object, test.object.body)
				db, err := sql.Open("mysql", storedBodyDSN(c, engine.engine, source))
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
				runner := recreatingRunner{DB: db, session: storedBodySession(c, admin, source), object: test.object}

				schema, err := mysql.NewMySQLReader(runner, "").ReadSchemaContext(c.Context())

				c.Assert(err, qt.ErrorMatches, `failed to read the body of `+test.kind+` `+test.object.name+`: cannot read `+
					test.kind+` "`+source+`.`+test.object.name+`": stored sql_mode includes PIPES_AS_CONCAT, .*`)
				c.Assert(schema, qt.IsNil)
			})
		}
	}
}

// manyBodies creates 40 triggers in database, each with a body of its own,
// and returns the bodies by trigger name.
func manyBodies(c *qt.C, admin *sql.DB, database string) map[string]string {
	c.Helper()
	want := make(map[string]string)
	for index := range 40 {
		object := trigger(fmt.Sprintf("tr_%02d", index), fmt.Sprintf("SET NEW.code = 'body \\%d'", index))
		createStoredObject(c, admin, database, "", object, object.body)
		want[object.name] = object.body
	}
	return want
}

// Over a connection pool the reader sends several SHOW CREATE statements at
// once; each body still lands on its own trigger.
func TestStoredBodies_LiveReadsManyBodiesOntoTheirObjects(t *testing.T) {
	for _, engine := range storedBodyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openMySQLWriterLiveDatabase(c, engine.engine)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			source := storedBodyDatabase(c, admin, "ptah_body_many")
			want := manyBodies(c, admin, source)

			read := readStoredBodies(c, engine.engine, engine.dialect, source)

			c.Assert(read, qt.DeepEquals, want)
		})
	}
}

// An account limited to one or two sessions reads every body on the sessions
// it may open, and the read leaves the pool holding the sessions it held
// before: a session kept open past the read is one a second connection of the
// account is refused.
func TestStoredBodies_LiveReadsEveryBodyOnAConnectionLimitedAccount(t *testing.T) {
	for _, engine := range storedBodyEngines {
		for _, limit := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d", engine.name, limit), func(t *testing.T) {
				c := qt.New(t)
				admin := openMySQLWriterLiveDatabase(c, engine.engine)
				c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
				source := storedBodyDatabase(c, admin, "ptah_body_limited")
				want := manyBodies(c, admin, source)
				dsn := limitedAccount(c, admin, engine.engine, source, limit)
				db, err := sql.Open("mysql", dsn)
				c.Assert(err, qt.IsNil)
				c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
				c.Assert(db.PingContext(c.Context()), qt.IsNil)
				open := db.Stats().OpenConnections

				schema, err := mysql.NewMySQLReader(db, "").ReadSchemaContext(c.Context())

				c.Assert(err, qt.IsNil)
				c.Assert(bodiesOf(schema), qt.DeepEquals, want)
				c.Assert(db.Stats().InUse, qt.Equals, 0)
				c.Assert(db.Stats().OpenConnections, qt.Equals, open)
			})
		}
	}
}

// limitedAccount creates an account that may hold limit sessions at once and
// read everything, drops it when the test ends, and returns its driver DSN
// with database selected.
func limitedAccount(c *qt.C, admin *sql.DB, engine dbtarget.Engine, database string, limit int) string {
	c.Helper()
	user := fmt.Sprintf("ptah_lim_%d_%d", limit, time.Now().UnixNano()%1_000_000_000)
	const password = "limited-Account-pw"
	for _, statement := range []string{
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s' WITH MAX_USER_CONNECTIONS %d", user, password, limit),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'", user),
	} {
		_, err := admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
		c.Check(err, qt.IsNil)
	})
	config, err := mysqldriver.ParseDSN(storedBodyDSN(c, engine, database))
	c.Assert(err, qt.IsNil)
	config.User, config.Passwd = user, password
	return config.FormatDSN()
}

// A pool one of whose sessions was given another character set lists the
// catalog on that session, which writes a name's bytes in latin1, and reads
// the bodies on sessions that read them in utf8mb4. The read fails, saying
// so, rather than saying the trigger was dropped: MySQL finds no trigger by the
// latin1 bytes, and MariaDB refuses them as a utf8mb4 string.
func TestStoredBodies_LiveSaysASessionReadsNamesInAnotherCharacterSet(t *testing.T) {
	tests := []struct {
		engine  dbtarget.Engine
		name    string
		wantErr string
		code    uint16
	}{
		{
			engine: dbtarget.MySQLAdmin, name: "mysql",
			wantErr: `(?s)failed to read the body of trigger tr_.*: the session asked for it reads names in utf8mb4, and the ` +
				`catalog listed it in latin1, so the server looked for another name: give every session of the pool ` +
				`one character set, and read the schema again: Error 1360 .*`,
			code: 1360,
		},
		{
			engine: dbtarget.MariaDBAdmin, name: "mariadb",
			wantErr: `(?s)failed to read the body of trigger tr_.*: Error 1300 .*`,
			code:    1300,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openMySQLWriterLiveDatabase(c, test.engine)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			source := storedBodyDatabase(c, admin, "ptah_body_charset")
			for index := range 40 {
				object := trigger(fmt.Sprintf("tr_\u00e9_%02d", index), "SET NEW.code = 1")
				createStoredObject(c, admin, source, "", object, object.body)
			}
			db, err := sql.Open("mysql", storedBodyDSN(c, test.engine, source))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			_, err = db.ExecContext(c.Context(), "SET NAMES latin1")
			c.Assert(err, qt.IsNil)

			schema, err := mysql.NewMySQLReader(db, "").ReadSchemaContext(c.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var serverErr *mysqldriver.MySQLError
			c.Assert(err, qt.ErrorAs, &serverErr)
			c.Assert(serverErr.Number, qt.Equals, test.code)
			c.Assert(schema, qt.IsNil)
		})
	}
}

// Over one session, which carries one statement at a time, the reader sends
// its SHOW CREATE statements one after another.
func TestStoredBodies_LiveReadsManyBodiesOverOneSession(t *testing.T) {
	for _, engine := range storedBodyEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			admin := openMySQLWriterLiveDatabase(c, engine.engine)
			c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
			source := storedBodyDatabase(c, admin, "ptah_body_session")
			want := manyBodies(c, admin, source)
			db, err := sql.Open("mysql", storedBodyDSN(c, engine.engine, source))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
			session, err := db.Conn(c.Context())
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })

			schema, err := mysql.NewMySQLReader(sqlrunner.NewConn(c.Context(), session), "").ReadSchemaContext(c.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(bodiesOf(schema), qt.DeepEquals, want)
		})
	}
}

// storedBodyDatabase creates a database whose name starts with prefix and
// drops it when the test ends.
func storedBodyDatabase(c *qt.C, admin *sql.DB, prefix string) string {
	c.Helper()
	name := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	createMySQLWriterLiveDatabase(c, admin, name)
	c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, admin, name) })
	return name
}

// storedBodyDSN is the driver DSN of engine's admin account with database
// selected.
func storedBodyDSN(c *qt.C, engine dbtarget.Engine, database string) string {
	c.Helper()
	config, err := mysqldriver.ParseDSN(dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	config.DBName = database
	return config.FormatDSN()
}

// createStoredObject creates object in database with body, its table first,
// on a session whose sql_mode adds mode.
func createStoredObject(c *qt.C, admin *sql.DB, database, mode string, object storedObject, body string) {
	c.Helper()
	conn := storedBodySession(c, admin, database)
	statements := append(tableOf(object),
		"SET SESSION sql_mode = CONCAT_WS(',', @@SESSION.sql_mode, NULLIF('"+mode+"', ''))",
		fmt.Sprintf(object.create, body),
	)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// tableOf is the statement that creates the table object fires on, if any.
func tableOf(object storedObject) []string {
	return slices.DeleteFunc(
		[]string{"CREATE TABLE " + object.table + " (code varchar(200), `a``b` varchar(20))"},
		func(string) bool { return object.table == "" },
	)
}

// storedBodySession returns a session of admin that selected database, closed
// when the test ends.
func storedBodySession(c *qt.C, admin *sql.DB, database string) *sql.Conn {
	c.Helper()
	conn, err := admin.Conn(c.Context())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	_, err = conn.ExecContext(c.Context(), "USE `"+database+"`")
	c.Assert(err, qt.IsNil)
	return conn
}

// readStoredBodies reads database through Ptah's reader and returns the body
// of each trigger and routine by its name.
func readStoredBodies(c *qt.C, engine dbtarget.Engine, dialect, database string) map[string]string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), mysqlFamilyDatabaseURL(c, dbtarget.DriverDSN(c, engine), database, dialect))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	schema, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return bodiesOf(schema)
}

// bodiesOf returns the body of each trigger and routine of schema by its name.
func bodiesOf(schema *catalog.Database) map[string]string {
	bodies := make(map[string]string)
	for _, trigger := range schema.Triggers {
		bodies[trigger.Name] = trigger.Body
	}
	for _, routine := range schema.Functions {
		bodies[routine.Name] = routine.Body
	}
	return bodies
}

// showCreate returns the statement SHOW CREATE prints for object in database.
func showCreate(c *qt.C, admin *sql.DB, database string, object storedObject) string {
	c.Helper()
	conn := storedBodySession(c, admin, database)
	rows, err := conn.QueryContext(c.Context(), "SHOW CREATE "+object.kind+" "+object.name)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	values := make([]sql.NullString, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	c.Assert(rows.Next(), qt.IsTrue)
	c.Assert(rows.Scan(destinations...), qt.IsNil)
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(values[2].Valid, qt.IsTrue)
	return values[2].String
}

// fire runs object in database and returns the value its body computed.
func fire(c *qt.C, admin *sql.DB, database string, object storedObject) string {
	c.Helper()
	conn := storedBodySession(c, admin, database)
	for _, statement := range object.prepare {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), object.fire).Scan(&value), qt.IsNil)
	return value
}
