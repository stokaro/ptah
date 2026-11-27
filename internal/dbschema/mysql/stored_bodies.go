package mysql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/catalog"
	"ptah.run/internal/fanout"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/mysqlstored"
	"ptah.run/internal/readscope"
	"ptah.run/internal/sqlrunner"
)

// storedBodyReaders is how many sessions [Reader.readStoredBodies] takes from
// a connection pool to send SHOW CREATE statements on at once. Each costs a
// round trip and nothing else, so over a remote link the read takes about the
// round trip times the number of bodies over the sessions it holds.
const storedBodyReaders = 8

// readStoredBodies gives each trigger and routine of dbName in schema the
// body SHOW CREATE prints, in place of the one information_schema reports.
//
// information_schema reports a body rewritten. MySQL writes each string
// literal's value between its quotes, so 'a\'b' reads back as 'a'b', a literal
// holding \0 cuts a routine's body short, and 'a\qb' reads back as 'aqb'; it
// writes a quoted name's value between backquotes too, so a name holding a
// backquote reads back as text no server parses. MariaDB writes an escaped
// quote as two quotes and 'a\\b' as 'a\b', which is a backspace. Both drop the
// character set introducer of a hex or a bit literal, so _latin1 0xE9 reads
// back as 0xE9, which is another value.
// Measured on MySQL 8.0.46, 8.4.13, 9.7.4 and 26.10.0 and on MariaDB 10.11.19,
// 11.8.9 and 12.3.3, for ACTION_STATEMENT and ROUTINE_DEFINITION alike. A body
// planned, printed or created again from that text is another program, or one
// the server refuses, and a declared body compared with it differs on every
// run. SHOW CREATE keeps the body as it was written, on every one of those
// servers.
//
// No property of the reported text says it was rewritten, so every body costs
// a statement of its own. Nothing is sent for an object the run leaves out
// ([readscope.Keeps]) or for a kind it leaves out whole. Over a connection
// pool with no cap on its open sessions the statements run on as many as
// [storedBodyReaders] sessions at once, as many as the account and the server
// let it open ([Reader.bodySessions]); over a capped pool and over one
// session, which a pinned connection or a transaction is, they run one after
// another. The bodies land on their objects whatever order the statements
// finish in.
//
// The body is read under the sql_mode the object was stored with, which SHOW
// CREATE reports beside it, and where that mode reads a backslash, a double
// quote or a square bracket otherwise than this session does, it is written
// again for this session ([mysqlstored.Respell]): the server runs the body
// under its own mode, and the same bytes run under the session's would be
// another program.
//
// It runs after the read refused a routine whose body the account may not
// see, so every routine it reads has one.
func (r *Reader) readStoredBodies(
	ctx context.Context, account sessionAccount, dbName, current string, schema *catalog.Database,
) (err error) {
	read := r.storedBodyRead(ctx, account, dbName, current, schema)
	if len(read.bodies) == 0 {
		return nil
	}
	sessions, release, err := r.bodySessions(ctx, len(read.bodies))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, release()) }()
	var sessionMode string
	var names sql.NullString
	if err := sessions[0].QueryRowContext(ctx, sessionStateQuery).Scan(&sessionMode, &names); err != nil {
		return fmt.Errorf("failed to read the session's sql_mode: %w", err)
	}
	reader := bodyReader{
		session: mysqlstored.ServerFor(read.dialect, mysqlstored.VersionID(account.version), sessionMode),
		mode:    sessionMode,
		names:   names.String,
		dbName:  dbName,
	}
	bodies, err := fanout.InOrder(ctx, sessions, len(read.bodies), func(ctx context.Context, on sqlrunner.Runner, index int) (string, error) {
		object := read.bodies[index]
		body, err := reader.body(ctx, on, object.kind, object.name)
		if err != nil {
			return "", fmt.Errorf("failed to read the body of %s %s: %w", strings.ToLower(object.kind), object.name, err)
		}
		return body, nil
	})
	if err != nil {
		return err
	}
	for index, object := range read.bodies {
		*object.body = bodies[index]
	}
	return nil
}

// storedBodyRead is which bodies [Reader.readStoredBodies] reads, on a server
// of dialect.
type storedBodyRead struct {
	dialect string
	bodies  []storedBodyObject
}

// storedBodyObject is one trigger or routine whose body is read: its kind as
// SHOW CREATE names it, TRIGGER, FUNCTION or PROCEDURE, its name, and the
// field the body is written to.
type storedBodyObject struct {
	kind string
	name string
	body *string
}

// storedBodyRead finds the triggers and routines of schema the run keeps. The
// selectors are asked about every table and view as well, since a trigger
// leaves with its table and an include selector names the tables it keeps.
// Each object is named in dbName: the read names none of them, and each gets
// dbName here, the database [inDatabase] gives it where the read describes the
// server. Where the connection selected dbName, current is dbName too, and the
// selectors read an unqualified name in it.
func (r *Reader) storedBodyRead(ctx context.Context, account sessionAccount, dbName, current string, schema *catalog.Database) storedBodyRead {
	read := storedBodyRead{dialect: account.dialect()}
	scope := readscope.Read{Dialect: read.dialect, Current: current}
	if readscope.Needs(ctx, readscope.Triggers) {
		for index := range schema.Triggers {
			trigger := &schema.Triggers[index]
			scope.Objects = append(scope.Objects, readscope.Object{
				Kind: readscope.Triggers, Schema: cmp.Or(trigger.Schema, dbName), Table: trigger.Table, Name: trigger.Name,
			})
			read.bodies = append(read.bodies, storedBodyObject{kind: "TRIGGER", name: trigger.Name, body: &trigger.Body})
		}
	}
	if readscope.Needs(ctx, readscope.Routines) {
		for index := range schema.Functions {
			routine := &schema.Functions[index]
			scope.Objects = append(scope.Objects, readscope.Object{
				Kind: readscope.Routines, Schema: cmp.Or(routine.Schema, dbName), Name: routine.Name,
			})
			read.bodies = append(read.bodies, storedBodyObject{kind: strings.ToUpper(routine.Kind), name: routine.Name, body: &routine.Body})
		}
	}
	if len(read.bodies) == 0 {
		return read
	}
	for _, table := range schema.Tables {
		scope.Objects = append(scope.Objects, readscope.Object{Kind: readscope.Tables, Schema: cmp.Or(table.Schema, dbName), Name: table.Name})
	}
	for _, view := range schema.Views {
		scope.Objects = append(scope.Objects, readscope.Object{Kind: readscope.Views, Schema: cmp.Or(view.Schema, dbName), Name: view.Name})
	}
	kept := readscope.Keeps(ctx, scope)
	bodies := read.bodies[:0]
	for index, object := range read.bodies {
		if kept[index] {
			bodies = append(bodies, object)
		}
	}
	read.bodies = bodies
	return read
}

// sessionStateQuery reads the session's sql_mode, which the bodies are
// written for, and the character set the catalog's names arrive in. It runs on
// the first session [Reader.bodySessions] returns, which on a pool is the
// session the catalog was most likely listed on: the idle one the pool hands
// out first.
const sessionStateQuery = "SELECT @@SESSION.sql_mode, @@SESSION.character_set_results"

// bodySessions returns the sessions to read bodies bodies on, at least one,
// and the function that gives them back.
//
// Over a connection pool with no cap on its open sessions it takes up to
// [storedBodyReaders] sessions of the pool, never more than bodies; over a
// capped pool it takes one, since a reader holding some of the cap while it
// waits for the rest would wait forever beside a second reader holding the
// others. The first reuses the session the read already opened, and each
// body and every question about it run on the session that asked for it. A
// refusal naming a connection limit ends the taking once one session is held,
// since an account or a server limited that way reads on what it has; any
// other failure, and a limit met before a session is held, fails the read.
// The first session goes back to the pool and the others are discarded
// ([sqlrunner.Discard]): the pool would keep them open as idle sessions, each
// counted against the account's and the server's limits, and a second pool
// opened next, as a diff opens one for each side, would be refused a session.
//
// Over any other runner the one session is the runner.
func (r *Reader) bodySessions(ctx context.Context, bodies int) ([]sqlrunner.Runner, func() error, error) {
	pool, pooled := r.db.(*sql.DB)
	if !pooled {
		return []sqlrunner.Runner{r.db}, func() error { return nil }, nil
	}
	most := min(storedBodyReaders, bodies)
	if pool.Stats().MaxOpenConnections > 0 {
		most = 1
	}
	var held []*sql.Conn
	release := func() error {
		errs := make([]error, 0, len(held))
		for index, conn := range held {
			if index == 0 {
				errs = append(errs, sqlrunner.Release(conn, "the session bodies were read on"))
				continue
			}
			errs = append(errs, sqlrunner.Discard(conn, "a session bodies were read on"))
		}
		return errors.Join(errs...)
	}
	for range most {
		conn, err := pool.Conn(ctx)
		if err != nil && len(held) > 0 && connectionLimit(err) {
			break
		}
		if err != nil {
			return nil, nil, errors.Join(fmt.Errorf("failed to open a session to read bodies on: %w", err), release())
		}
		held = append(held, conn)
	}
	sessions := make([]sqlrunner.Runner, 0, len(held))
	for _, conn := range held {
		sessions = append(sessions, sqlrunner.NewConn(ctx, conn))
	}
	return sessions, release, nil
}

// The errors a server answers a new session with when a connection limit is
// reached: ER_CON_COUNT_ERROR for max_connections, ER_TOO_MANY_USER_CONNECTIONS
// for the max_user_connections variable, and ER_USER_LIMIT_REACHED for an
// account's MAX_USER_CONNECTIONS or MAX_CONNECTIONS_PER_HOUR, on MySQL and
// MariaDB alike.
const (
	errTooManyConnections     = 1040
	errTooManyUserConnections = 1203
	errUserLimitReached       = 1226
)

// connectionLimit reports whether err is a server refusing a session for a
// connection limit.
func connectionLimit(err error) bool {
	var serverErr *mysqldriver.MySQLError
	return errors.As(err, &serverErr) &&
		(serverErr.Number == errTooManyConnections || serverErr.Number == errTooManyUserConnections ||
			serverErr.Number == errUserLimitReached)
}

// The errors a server answers SHOW CREATE with for an object that does not
// exist: ER_TRG_DOES_NOT_EXIST and ER_SP_DOES_NOT_EXIST, on MySQL and MariaDB
// alike.
const (
	errTriggerDoesNotExist = 1360
	errRoutineDoesNotExist = 1305
)

// bodyReader reads the bodies of the triggers and routines of dbName. session
// is the server the bodies are written for and mode its sql_mode, and names is
// the character set the catalog's names arrived in, which is empty where the
// session's character_set_results is NULL.
type bodyReader struct {
	session mysqlstored.Server
	mode    string
	names   string
	dbName  string
}

// body reads on db the body of the object of kind, TRIGGER, FUNCTION or
// PROCEDURE, named name, as SHOW CREATE prints it, written for the session.
func (b bodyReader) body(ctx context.Context, db sqlrunner.Runner, kind, name string) (string, error) {
	query := "SHOW CREATE " + kind + " " + quoteName(b.dbName) + "." + quoteName(name)
	rows, err := db.QueryContext(ctx, query)
	var serverErr *mysqldriver.MySQLError
	if errors.As(err, &serverErr) && (serverErr.Number == errTriggerDoesNotExist || serverErr.Number == errRoutineDoesNotExist) {
		return "", b.missing(ctx, db, kind, name, err)
	}
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return "", err
	}
	// The name, the sql_mode and the definition come first, on MySQL and
	// MariaDB alike; the character sets and the time of creation follow.
	if len(columns) < 3 {
		return "", fmt.Errorf("SHOW CREATE %s answered %d columns", kind, len(columns))
	}
	values := make([]sql.NullString, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("SHOW CREATE %s answered no row", kind)
	}
	if err := rows.Scan(destinations...); err != nil {
		return "", err
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	mode, definition := values[1], values[2]
	if !definition.Valid {
		return "", fmt.Errorf("SHOW CREATE %s printed no definition, which an account that may not see the body reads", kind)
	}
	// The catalog's mode was checked when the catalog listed the object, but
	// an object created again since then is stored under the mode SHOW CREATE
	// reports, and a body read under a mode the model cannot carry is another
	// program once it is created again.
	if err := refuseStoredProgramMode(strings.ToLower(kind), b.dbName, name, mode.String, b.mode); err != nil {
		return "", err
	}
	stored := mysqlstored.ServerFor(b.session.Dialect, b.session.VersionID, mode.String)
	body, err := mysqlstored.Body(stored, definition.String)
	if err != nil {
		return "", err
	}
	return mysqlstored.Respell(body, stored, b.session)
}

// missing says why SHOW CREATE answered db that the object of kind named name,
// which the catalog listed, does not exist (notFound).
//
// A session reads a name's bytes in its character_set_client. Where that
// differs from the character set the catalog's names arrived in and the names
// hold a byte outside ASCII, which every client character set writes alike,
// the server looked for another name, which a pool whose sessions were given
// different character sets causes; information_schema, asked on that session,
// misses the object for the same reason. Otherwise information_schema says
// whether the object is still there: an object it no longer lists was dropped
// after the catalog listed it.
func (b bodyReader) missing(ctx context.Context, db sqlrunner.Runner, kind, name string, notFound error) error {
	var client string
	if err := db.QueryRowContext(ctx, "SELECT @@SESSION.character_set_client").Scan(&client); err != nil {
		return errors.Join(notFound, fmt.Errorf("failed to read the session's character set: %w", err))
	}
	if nameEncoding(client) != nameEncoding(b.names) && !ascii(b.dbName+name) {
		return fmt.Errorf(
			"the session asked for it reads names in %s, and the catalog listed it in %s, so the server looked for "+
				"another name: give every session of the pool one character set, and read the schema again: %w",
			client, cmp.Or(b.names, "utf8mb3"), notFound,
		)
	}
	query, args := listedQuery(kind, b.dbName, name)
	var listed int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&listed); err != nil {
		return errors.Join(notFound, fmt.Errorf("failed to ask information_schema whether it is still listed: %w", err))
	}
	if listed > 0 {
		return fmt.Errorf(
			"the server did not find it under the session's character set, %s, though information_schema on that "+
				"session still lists it: %w",
			client, notFound,
		)
	}
	return fmt.Errorf("it no longer exists: it was dropped after the catalog listed it, so read the schema again: %w", notFound)
}

// listedQuery counts the objects of kind, TRIGGER, FUNCTION or PROCEDURE,
// that information_schema lists by name in dbName.
//
// The database is compared as the catalog read compares it
// ([mysqlname.SameDatabase]): MariaDB with lower_case_table_names=0 holds rvs
// and RVS side by side, and TRIGGER_SCHEMA = ? counts the triggers of both:
// under that comparison a trigger dropped from rvs reads as still listed
// beside RVS.tr_x. The routine count uses the same rule, so both counts look
// in the database the read listed.
//
// A trigger's name is compared byte for byte, in utf8mb4 on both sides:
// TRIGGER_NAME compares under utf8mb3_general_ci, and MariaDB holds tr_e,
// TR_E and tr_é side by side, so the plain comparison counts all three
// for each. A routine's name is compared as the column compares it: both
// servers refuse a second routine whose name differs from the first only in
// case or accent, so the comparison finds the one routine the name denotes.
func listedQuery(kind, dbName, name string) (string, []any) {
	if kind == "TRIGGER" {
		return "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE " + mysqlname.SameDatabase("TRIGGER_SCHEMA", "?") +
				" AND CAST(TRIGGER_NAME AS BINARY) = CAST(CONVERT(? USING utf8mb4) AS BINARY)",
			append(nameBinds(dbName, 1), name)
	}
	return "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE " + mysqlname.SameDatabase("ROUTINE_SCHEMA", "?") +
			" AND ROUTINE_NAME = ? AND ROUTINE_TYPE = ?",
		append(nameBinds(dbName, 1), name, kind)
}

// ascii reports whether text holds no byte above 0x7F.
func ascii(text string) bool {
	for index := range len(text) {
		if text[index] > 0x7F {
			return false
		}
	}
	return true
}

// nameEncoding is how a session whose character set is charset writes a
// name's bytes. A name lies in the Basic Multilingual Plane, where utf8mb3 and
// utf8mb4 write the same bytes, and a session whose character_set_results is
// NULL reads names in utf8mb3, the server's system character set.
func nameEncoding(charset string) string {
	if charset == "" || strings.HasPrefix(charset, "utf8") {
		return "utf8"
	}
	return charset
}

// quoteName writes name between backquotes, each backquote in it doubled.
func quoteName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
