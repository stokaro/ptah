package mysql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/mysqlname"
)

// manyTriggers holds count triggers on child, each with a body of its own,
// and returns the bodies by trigger name beside it.
func manyTriggers(count int) (visibleCatalog, map[string]string) {
	catalog := visibleCatalog{selected: "fx", tables: []string{"child"}}
	bodies := make(map[string]string)
	for index := range count {
		name, body := fmt.Sprintf("trg_%02d", index), fmt.Sprintf("SET NEW.n = %d", index)
		catalog.triggers = append(catalog.triggers, triggerRow(name, body))
		bodies[name] = body
	}
	return catalog, bodies
}

// refusingFrom refuses every connection from attempt first on with err, and
// counts the attempts in attempts.
func refusingFrom(first int, err error, attempts *atomic.Int64) dbtest.ConnectHandler {
	return func(attempt int) error {
		attempts.Add(1)
		if attempt >= first {
			return err
		}
		return nil
	}
}

// The server refuses a session beyond a connection limit. The bodies are read
// on the sessions the limit allows, the read asks for no other once refused,
// and it leaves the pool holding the sessions it held before. With no limit it
// takes eight sessions, and one for each body where there are fewer bodies.
func TestReader_ReadsBodiesOnTheSessionsAConnectionLimitAllows_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		triggers     int
		refusal      error
		first        int
		wantAttempts int64
	}{
		{
			name:     "an account's MAX_USER_CONNECTIONS",
			triggers: 20,
			refusal:  &mysqldriver.MySQLError{Number: 1226, Message: "User 'app' has exceeded the 'max_user_connections' resource (current value: 1)"},
			first:    1, wantAttempts: 2,
		},
		{
			name:     "the max_user_connections variable",
			triggers: 20,
			refusal:  &mysqldriver.MySQLError{Number: 1203, Message: "User app already has more than 'max_user_connections' active connections"},
			first:    1, wantAttempts: 2,
		},
		{
			name:     "the server's max_connections",
			triggers: 20,
			refusal:  &mysqldriver.MySQLError{Number: 1040, Message: "Too many connections"},
			first:    1, wantAttempts: 2,
		},
		{
			name:     "a limit of three sessions",
			triggers: 20,
			refusal:  &mysqldriver.MySQLError{Number: 1226, Message: "User 'app' has exceeded the 'max_user_connections' resource (current value: 3)"},
			first:    3, wantAttempts: 4,
		},
		{name: "no limit", triggers: 20, first: 100, wantAttempts: 8},
		{name: "no limit and three bodies", triggers: 3, first: 100, wantAttempts: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog, want := manyTriggers(test.triggers)
			var attempts atomic.Int64
			db := dbtest.OpenWithConnect(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, catalog.answer),
				refusingFrom(test.first, test.refusal, &attempts))
			c.Assert(db.SQL.PingContext(t.Context()), qt.IsNil)
			open := db.SQL.Stats().OpenConnections

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			bodies := make(map[string]string)
			for _, trigger := range got.Triggers {
				bodies[trigger.Name] = trigger.Body
			}
			c.Assert(bodies, qt.DeepEquals, want)
			c.Assert(attempts.Load(), qt.Equals, test.wantAttempts)
			c.Assert(db.SQL.Stats().InUse, qt.Equals, 0)
			c.Assert(db.SQL.Stats().OpenConnections, qt.Equals, open)
		})
	}
}

// A pool with a cap on its open sessions reads every body on one session, the
// one it already holds: a read holding part of the cap while it waits for the
// rest would wait forever beside a second read holding the others.
func TestReader_ReadsBodiesOnACappedPool_HappyPath(t *testing.T) {
	for _, open := range []int{1, 2, 20} {
		t.Run(fmt.Sprint(open), func(t *testing.T) {
			c := qt.New(t)
			catalog, want := manyTriggers(20)
			var attempts atomic.Int64
			db := dbtest.OpenWithConnect(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, catalog.answer),
				refusingFrom(100, nil, &attempts))
			db.SQL.SetMaxOpenConns(open)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			c.Cleanup(cancel)

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(ctx)

			c.Assert(err, qt.IsNil)
			bodies := make(map[string]string)
			for _, trigger := range got.Triggers {
				bodies[trigger.Name] = trigger.Body
			}
			c.Assert(bodies, qt.DeepEquals, want)
			c.Assert(attempts.Load(), qt.Equals, int64(1))
			c.Assert(db.SQL.Stats().InUse, qt.Equals, 0)
		})
	}
}

// refusingAfterQuery refuses every connection opened after the pool has
// answered a question holding query, with err.
func refusingAfterQuery(visible visibleCatalog, query string, err error) (dbtest.QueryHandler, dbtest.ConnectHandler) {
	var asked atomic.Bool
	handler := dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, func(q string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		if strings.Contains(q, query) {
			asked.Store(true)
		}
		return visible.answer(q, args)
	})
	return handler, func(int) error {
		if asked.Load() {
			return err
		}
		return nil
	}
}

var errAccessDenied = &mysqldriver.MySQLError{Number: 1045, Message: "Access denied for user 'app'@'%'"}

// A session refused for any other reason than a connection limit fails the
// read, and so does a connection limit met before the read holds a session to
// read on. Every session the read took is given back.
func TestReader_ASessionRefusedForTheBodiesFailsTheRead_FailurePath(t *testing.T) {
	limit := &mysqldriver.MySQLError{Number: 1226, Message: "User 'app' has exceeded the 'max_user_connections' resource (current value: 1)"}
	tests := []struct {
		name    string
		connect func(visible visibleCatalog) (dbtest.QueryHandler, dbtest.ConnectHandler)
		idle    int
		wantErr string
		wantIs  error
	}{
		{
			name: "access denied on the second session",
			connect: func(visible visibleCatalog) (dbtest.QueryHandler, dbtest.ConnectHandler) {
				return dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, visible.answer),
					refusingFrom(1, errAccessDenied, new(atomic.Int64))
			},
			idle:    2,
			wantErr: "failed to open a session to read bodies on: Error 1045: Access denied for user 'app'@'%'",
			wantIs:  errAccessDenied,
		},
		{
			name: "a connection limit on the first session",
			connect: func(visible visibleCatalog) (dbtest.QueryHandler, dbtest.ConnectHandler) {
				return refusingAfterQuery(visible, "FROM information_schema.ROUTINES", limit)
			},
			idle: 0,
			wantErr: "failed to open a session to read bodies on: Error 1226: User 'app' has exceeded the " +
				"'max_user_connections' resource (current value: 1)",
			wantIs: limit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog, _ := manyTriggers(20)
			handler, connect := test.connect(catalog)
			db := dbtest.OpenWithConnect(t, handler, connect)
			db.SQL.SetMaxIdleConns(test.idle)

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(got, qt.IsNil)
			c.Assert(db.SQL.Stats().InUse, qt.Equals, 0)
		})
	}
}

// missingObjects answers SHOW CREATE with the error a server gives for an
// object it does not find, the session's character set with client, and
// information_schema's count of an object by name with whether listed holds
// it, as "<schema>.<name> <kind>". Every other question goes to visible.
func missingObjects(visible visibleCatalog, number uint16, message, client string, listed []string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		switch {
		case strings.HasPrefix(query, "SHOW CREATE "):
			return dbtest.QueryResult{}, &mysqldriver.MySQLError{Number: number, Message: message}
		case query == clientCharsetQuery:
			return dbtest.QueryResult{Columns: []string{"@@SESSION.character_set_client"}, Rows: [][]driver.Value{{client}}}, nil
		case strings.HasPrefix(query, "SELECT COUNT(*) FROM information_schema."):
			return listedCount(query, args, listed), nil
		}
		return visible.answer(query, args)
	}
}

// listedCount counts the objects of listed the information_schema question
// query asks about with args. A question of another shape answers no row.
func listedCount(query string, args []driver.NamedValue, listed []string) dbtest.QueryResult {
	values := make([]string, 0, len(args))
	for _, arg := range args {
		values = append(values, fmt.Sprint(arg.Value))
	}
	var object string
	switch {
	case query == "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE "+mysqlname.SameDatabase("TRIGGER_SCHEMA", "?")+
		" AND CAST(TRIGGER_NAME AS BINARY) = CAST(CONVERT(? USING utf8mb4) AS BINARY)" &&
		len(values) == 3 && values[0] == values[1]:
		object = values[0] + "." + values[2] + " TRIGGER"
	case query == "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE "+mysqlname.SameDatabase("ROUTINE_SCHEMA", "?")+
		" AND ROUTINE_NAME = ? AND ROUTINE_TYPE = ?" &&
		len(values) == 4 && values[0] == values[1]:
		object = values[0] + "." + values[2] + " " + values[3]
	default:
		return dbtest.QueryResult{Columns: []string{"COUNT(*)"}}
	}
	var count int64
	if slices.Contains(listed, object) {
		count = 1
	}
	return dbtest.QueryResult{Columns: []string{"COUNT(*)"}, Rows: [][]driver.Value{{count}}}
}

// namesSwitch answers as [missingObjects] does for a capped pool of one
// session, which another user of the pool shares: when SHOW CREATE is asked,
// that user waits for the session and runs SET NAMES latin1 on it once it has
// one, and the session's character_set_client is what the user left.
type namesSwitch struct {
	visible visibleCatalog
	pool    atomic.Pointer[sql.DB]
	latin1  atomic.Bool
	other   sync.WaitGroup
}

func (n *namesSwitch) answer(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	switch {
	case query == "SET NAMES latin1":
		n.latin1.Store(true)
		return dbtest.QueryResult{}, nil
	case strings.HasPrefix(query, "SHOW CREATE "):
		n.queueAnotherUser()
		return dbtest.QueryResult{}, &mysqldriver.MySQLError{Number: 1360, Message: "Trigger does not exist"}
	case query == clientCharsetQuery:
		client := "utf8mb4"
		if n.latin1.Load() {
			client = "latin1"
		}
		return dbtest.QueryResult{Columns: []string{"@@SESSION.character_set_client"}, Rows: [][]driver.Value{{client}}}, nil
	case strings.HasPrefix(query, "SELECT COUNT(*) FROM information_schema."):
		return listedCount(query, args, nil), nil
	}
	return n.visible.answer(query, args)
}

// queueAnotherUser starts the other user of the pool and returns once it
// waits for the pool's session, so the pool hands that user the session
// before any later request of the read.
func (n *namesSwitch) queueAnotherUser() {
	pool := n.pool.Load()
	waited := pool.Stats().WaitCount
	n.other.Go(func() {
		conn, err := pool.Conn(context.Background())
		if err != nil {
			return
		}
		defer conn.Close()
		// The fake answers SET NAMES with no row; Scan closes the rows.
		_ = conn.QueryRowContext(context.Background(), "SET NAMES latin1").Scan()
	})
	for deadline := time.Now().Add(5 * time.Second); pool.Stats().WaitCount == waited && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
}

// On a capped pool the read asks why SHOW CREATE missed an object on the
// session SHOW CREATE ran on. Asked on the pool, the question would wait behind
// another user of the pool, who can change the session's character set in
// between, and the read would blame the character set for a dropped trigger.
func TestReader_AsksAboutAMissingObjectOnTheSessionThatMissedIt_FailurePath(t *testing.T) {
	c := qt.New(t)
	visible := visibleCatalog{selected: "fx", tables: []string{"child"}, triggers: [][]driver.Value{triggerRow("trg_\u00e9", "SET NEW.n = 1")}}
	switching := &namesSwitch{visible: visible}
	db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, switching.answer))
	db.SQL.SetMaxOpenConns(1)
	switching.pool.Store(db.SQL)

	got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())
	switching.other.Wait()

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("failed to read the body of trigger trg_\u00e9: it no longer exists: "+
		"it was dropped after the catalog listed it, so read the schema again: Error 1360: Trigger does not exist"))
	c.Assert(got, qt.IsNil)
	c.Assert(switching.latin1.Load(), qt.IsTrue)
}

// recreatedUnder answers SHOW CREATE as visible does, with the object stored
// under mode, as an object dropped and created again after the catalog listed
// it is.
func recreatedUnder(visible visibleCatalog, mode string) dbtest.QueryHandler {
	return func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
		result, err := visible.answer(query, args)
		if strings.HasPrefix(query, "SHOW CREATE ") && len(result.Rows) == 1 {
			result.Rows[0][1] = mode
		}
		return result, err
	}
}

// The catalog listed a trigger or a procedure under a mode the reader accepts,
// and SHOW CREATE reports one it refuses: the object was created again in
// between. The read refuses it as it refuses one the catalog lists under that
// mode, rather than reading a body the model cannot carry.
func TestReader_RefusesAModeShowCreateReports_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		catalog visibleCatalog
		mode    string
		wantErr string
	}{
		{
			name:    "a trigger created again under PIPES_AS_CONCAT",
			catalog: visibleCatalog{selected: "fx", tables: []string{"child"}, triggers: fullCatalog().triggers, storedMode: defaultMode},
			mode:    "STRICT_TRANS_TABLES,PIPES_AS_CONCAT",
			wantErr: `failed to read the body of trigger trg_child: cannot read trigger "fx.trg_child": stored sql_mode ` +
				`includes PIPES_AS_CONCAT, which Ptah cannot preserve when recreating the body; manage this object in ` +
				`migration SQL that restores its stored sql_mode`,
		},
		{
			name:    "a procedure created again under MSSQL",
			catalog: visibleCatalog{selected: "fx", routines: fullCatalog().routines, storedMode: defaultMode},
			mode:    "MSSQL",
			wantErr: `failed to read the body of procedure p_noop: cannot read procedure "fx.p_noop": stored sql_mode ` +
				`includes MSSQL, which Ptah cannot preserve when recreating the body; manage this object in ` +
				`migration SQL that restores its stored sql_mode`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, recreatedUnder(test.catalog, test.mode)))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.IsNil)
		})
	}
}

// SHOW CREATE does not find a trigger or a procedure the catalog listed. The
// read fails and names the object. Where information_schema no longer lists
// it, another session dropped it, and a second read is the remedy; where it
// still lists it, the read says the server did not find it under the
// session's character set; where the session reads names in another
// character set than the catalog listed them in and a name holds a byte
// outside ASCII, the read says so without asking information_schema, which
// would miss the name the same way. An ASCII name reads alike in every
// character set, so information_schema decides for it.
func TestReader_AnObjectShowCreateDoesNotFindFailsTheRead_FailurePath(t *testing.T) {
	triggers := visibleCatalog{selected: "fx", tables: []string{"child"}, triggers: fullCatalog().triggers}
	routines := visibleCatalog{selected: "fx", routines: fullCatalog().routines}
	accented := visibleCatalog{selected: "fx", tables: []string{"child"}, triggers: [][]driver.Value{triggerRow("trg_\u00e9", "SET NEW.n = 1")}}
	tests := []struct {
		name     string
		catalog  visibleCatalog
		number   uint16
		message  string
		client   string
		listed   []string
		wantErr  string
		wantCode uint16
	}{
		{
			name:    "a dropped trigger",
			catalog: triggers,
			number:  1360, message: "Trigger does not exist", client: "utf8mb4",
			listed: []string{"other.trg_child TRIGGER"},
			wantErr: "failed to read the body of trigger trg_child: it no longer exists: it was dropped after the " +
				"catalog listed it, so read the schema again: Error 1360: Trigger does not exist",
			wantCode: 1360,
		},
		{
			name:    "a dropped procedure",
			catalog: routines,
			number:  1305, message: "PROCEDURE p_noop does not exist", client: "utf8mb4",
			listed: []string{"fx.p_noop FUNCTION"},
			wantErr: "failed to read the body of procedure p_noop: it no longer exists: it was dropped after the " +
				"catalog listed it, so read the schema again: Error 1305: PROCEDURE p_noop does not exist",
			wantCode: 1305,
		},
		{
			name:    "a dropped trigger asked for under utf8mb3",
			catalog: triggers,
			number:  1360, message: "Trigger does not exist", client: "utf8mb3",
			wantErr: "failed to read the body of trigger trg_child: it no longer exists: it was dropped after the " +
				"catalog listed it, so read the schema again: Error 1360: Trigger does not exist",
			wantCode: 1360,
		},
		{
			name:    "a trigger information_schema still lists",
			catalog: triggers,
			number:  1360, message: "Trigger does not exist", client: "utf8mb4",
			listed: []string{"fx.trg_child TRIGGER"},
			wantErr: "failed to read the body of trigger trg_child: the server did not find it under the session's " +
				"character set, utf8mb4, though information_schema on that session still lists it: Error 1360: " +
				"Trigger does not exist",
			wantCode: 1360,
		},
		{
			name:    "a procedure information_schema still lists",
			catalog: routines,
			number:  1305, message: "PROCEDURE p_noop does not exist", client: "utf8mb4",
			listed: []string{"fx.p_noop PROCEDURE"},
			wantErr: "failed to read the body of procedure p_noop: the server did not find it under the session's " +
				"character set, utf8mb4, though information_schema on that session still lists it: Error 1305: " +
				"PROCEDURE p_noop does not exist",
			wantCode: 1305,
		},
		{
			name:    "a trigger with an ASCII name dropped on a session of another character set",
			catalog: triggers,
			number:  1360, message: "Trigger does not exist", client: "latin1",
			listed: []string{"fx.trg_other TRIGGER"},
			wantErr: "failed to read the body of trigger trg_child: it no longer exists: it was dropped after the " +
				"catalog listed it, so read the schema again: Error 1360: Trigger does not exist",
			wantCode: 1360,
		},
		{
			name:    "a trigger asked for under another character set",
			catalog: accented,
			number:  1360, message: "Trigger does not exist", client: "latin1",
			listed: []string{"fx.trg_\u00e9 TRIGGER"},
			wantErr: "failed to read the body of trigger trg_\u00e9: the session asked for it reads names in latin1, " +
				"and the catalog listed it in utf8mb4, so the server looked for another name: give every session " +
				"of the pool one character set, and read the schema again: Error 1360: Trigger does not exist",
			wantCode: 1360,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			handler := missingObjects(test.catalog, test.number, test.message, test.client, test.listed)
			db := dbtest.Open(t, dbtest.MySQLSession(dbtest.MySQLVersion, dbtest.MySQLAllGrants, handler))

			got, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(t.Context())

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			var serverErr *mysqldriver.MySQLError
			c.Assert(err, qt.ErrorAs, &serverErr)
			c.Assert(serverErr.Number, qt.Equals, test.wantCode)
			c.Assert(got, qt.IsNil)
		})
	}
}
