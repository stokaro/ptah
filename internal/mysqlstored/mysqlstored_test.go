package mysqlstored_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqlstored"
)

// The servers statements are read for, by the version each reports.
var (
	mysql84   = mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413}
	mariadb   = mysqlstored.Server{Dialect: platform.MariaDB, VersionID: 110809}
	mysql2610 = mysqlstored.Server{Dialect: platform.MySQL, VersionID: 261000}
	unknown   = mysqlstored.Server{Dialect: platform.MySQL}
)

// The head of each statement, read on a MySQL 8.4 server. body says the
// replay guard refuses it, and keeps that the strict session gives it the
// session's own sql_mode.
func TestStatement_ReadsTheHead(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		verb      string
		kind      string
		object    string
		body      bool
		keeps     bool
	}{
		{name: "a trigger", statement: "CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a trigger with a definer", statement: "CREATE DEFINER=`app`@`%` TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a definer with a dotted host", statement: "CREATE DEFINER=trg@localhost.localdomain TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a dotted host that ends in a keyword", statement: "CREATE DEFINER=trg@localhost.trigger EVENT e ON SCHEDULE EVERY 1 DAY DO SELECT 1",
			verb: "CREATE", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a host spelled as a kind", statement: "CREATE DEFINER=app@trigger EVENT e ON SCHEDULE EVERY 1 DAY DO SELECT 1",
			verb: "CREATE", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a definer with an address", statement: "CREATE DEFINER = app@10.0.0.1 FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1",
			verb: "CREATE", kind: "FUNCTION", object: "f", body: true, keeps: true},
		{name: "an empty host", statement: "CREATE DEFINER=trg@ PROCEDURE p() SELECT 1",
			verb: "CREATE", kind: "PROCEDURE", object: "p", body: true, keeps: true},
		{name: "a host ended by a comment", statement: "CREATE DEFINER=trg@localhost/**/PROCEDURE p() SELECT 1",
			verb: "CREATE", kind: "PROCEDURE", object: "p", body: true, keeps: true},
		{name: "a space before the @", statement: "CREATE DEFINER=trg @'localhost' PROCEDURE p() SELECT 1",
			verb: "CREATE", kind: "PROCEDURE", object: "p", body: true, keeps: true},
		{name: "the current user as definer", statement: "CREATE DEFINER = CURRENT_USER() TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "the current role as definer", statement: "CREATE DEFINER=CURRENT_ROLE() PROCEDURE p() SELECT 1",
			verb: "CREATE", kind: "PROCEDURE", object: "p", body: true, keeps: true},
		{name: "a replaced trigger", statement: "CREATE OR REPLACE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a procedure in a database", statement: "CREATE PROCEDURE IF NOT EXISTS app.p() BEGIN SELECT 1; END",
			verb: "CREATE", kind: "PROCEDURE", object: "app.p", body: true, keeps: true},
		{name: "an aggregate function", statement: "CREATE AGGREGATE FUNCTION f(x INT) RETURNS INT BEGIN RETURN 1; END",
			verb: "CREATE", kind: "FUNCTION", object: "f", body: true, keeps: true},
		{name: "a loadable function", statement: "CREATE FUNCTION sys_exec RETURNS integer SONAME 'lib.so'",
			verb: "CREATE", kind: "FUNCTION", object: "sys_exec", body: true, keeps: true},
		{name: "an event", statement: "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO SELECT 1",
			verb: "CREATE", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a package", statement: "CREATE OR REPLACE PACKAGE pkg PROCEDURE p(); END",
			verb: "CREATE", kind: "PACKAGE", object: "pkg", body: true, keeps: true},
		{name: "a package body with a definer", statement: "CREATE DEFINER=app@localhost.localdomain PACKAGE BODY pkg PROCEDURE p() BEGIN DELETE FROM other.t; END; END",
			verb: "CREATE", kind: "PACKAGE BODY", object: "pkg", body: true, keeps: true},
		{name: "an event whose body changes", statement: "ALTER EVENT e DO SELECT 2",
			verb: "ALTER", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "an event with a definer whose body changes", statement: "ALTER DEFINER=app@localhost.localdomain EVENT e DO DELETE FROM other.t",
			verb: "ALTER", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a renamed event", statement: "ALTER EVENT e RENAME TO e2",
			verb: "ALTER", kind: "EVENT", object: "e", body: true},
		{name: "an event with a definer whose body stays", statement: "ALTER DEFINER=CURRENT_USER EVENT e COMMENT 'do'",
			verb: "ALTER", kind: "EVENT", object: "e", body: true},
		{name: "an event whose schedule changes", statement: "ALTER EVENT e ON SCHEDULE EVERY 2 DAY STARTS (NOW() + INTERVAL 1 DAY)",
			verb: "ALTER", kind: "EVENT", object: "e", body: true},
		{name: "a procedure's comment", statement: "ALTER PROCEDURE p COMMENT 'x'",
			verb: "ALTER", kind: "PROCEDURE", object: "p", body: true},
		{name: "a function's security", statement: "ALTER FUNCTION f SQL SECURITY INVOKER",
			verb: "ALTER", kind: "FUNCTION", object: "f", body: true},
		{name: "a view", statement: "CREATE OR REPLACE ALGORITHM=MERGE DEFINER=app@localhost.localdomain SQL SECURITY DEFINER VIEW v AS SELECT 1",
			verb: "CREATE", kind: "VIEW", object: "v"},
		{name: "an altered view", statement: "ALTER DEFINER=`app`@`%` SQL SECURITY INVOKER VIEW v AS SELECT 1",
			verb: "ALTER", kind: "VIEW", object: "v"},
		{name: "lower case", statement: "create definer=app@h trigger t_bi before insert on t for each row set new.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a comment in front", statement: "/* trigger */ -- body\nCREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a trigger as mysqldump writes it", statement: "/*!50003 CREATE*/ /*!50017 DEFINER=`root`@`localhost`*/ /*!50003 TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x' */",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a body in a comment with no version", statement: "/*! CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN (SELECT COUNT(*) FROM other.t) */",
			verb: "CREATE", kind: "FUNCTION", object: "f", body: true, keeps: true},
		{name: "a line comment inside an executable comment", statement: "/*!50003 CREATE -- note\n TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x' */",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a dotted host inside an executable comment", statement: "CREATE /*!50017 DEFINER=trg@localhost.localdomain*/ TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'",
			verb: "CREATE", kind: "TRIGGER", object: "t_bi", body: true, keeps: true},
		{name: "a dollar sign as the host", statement: "CREATE DEFINER=root@$ TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO other.t VALUES (88)",
			verb: "CREATE", kind: "TRIGGER", object: "tr", body: true, keeps: true},
		{name: "a dollar sign as the host of a function", statement: "CREATE DEFINER=root@$ FUNCTION f() RETURNS INT READS SQL DATA RETURN (SELECT COUNT(*) FROM other.t)",
			verb: "CREATE", kind: "FUNCTION", object: "f", body: true, keeps: true},
		{name: "a dollar sign as the host of an event", statement: "CREATE DEFINER=root@$ EVENT e ON SCHEDULE EVERY 1 DAY DO DELETE FROM other.t",
			verb: "CREATE", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a dollar sign as the user", statement: "CREATE DEFINER=$@h EVENT e ON SCHEDULE EVERY 1 SECOND DO DELETE FROM other.t",
			verb: "CREATE", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a carriage return inside a hash comment", statement: "CREATE #\rX\nFUNCTION f() RETURNS INT READS SQL DATA RETURN (SELECT COUNT(*) FROM other.t)",
			verb: "CREATE", kind: "FUNCTION", object: "f", body: true, keeps: true},
		{name: "a carriage return inside a dash comment", statement: "CREATE -- \rX\nTRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO other.t VALUES (77)",
			verb: "CREATE", kind: "TRIGGER", object: "tr", body: true, keeps: true},
		{name: "a carriage return inside a comment before a procedure", statement: "CREATE #\rX\nPROCEDURE p() DELETE FROM other.t",
			verb: "CREATE", kind: "PROCEDURE", object: "p", body: true, keeps: true},
		{name: "a carriage return inside a comment before an altered event", statement: "ALTER #\rX\nEVENT e DO DELETE FROM other.t",
			verb: "ALTER", kind: "EVENT", object: "e", body: true, keeps: true},
		{name: "a backslash ending a quoted host", statement: "CREATE DEFINER=root@`h\\` TRIGGER tr9 AFTER INSERT ON t FOR EACH ROW INSERT INTO other.t VALUES (90) -- `\n",
			verb: "CREATE", kind: "TRIGGER", object: "tr9", body: true, keeps: true},
		{name: "a trigger a SET STATEMENT runs", statement: "SET STATEMENT max_statement_time=0 FOR CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO other.t VALUES (93)",
			verb: "CREATE", kind: "TRIGGER", object: "tr", body: true, keeps: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok, err := mysqlstored.Statement(mysql84, test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got.Verb, qt.Equals, test.verb)
			c.Assert(got.Kind, qt.Equals, test.kind)
			c.Assert(got.Name, qt.Equals, test.object)
			c.Assert(got.Body(), qt.Equals, test.body)
			c.Assert(got.KeepsSessionMode(), qt.Equals, test.keeps)
		})
	}
}

// A statement that is not a CREATE or ALTER, and a CREATE or ALTER of an
// object that keeps no body, reads as no definition.
func TestStatement_ReadsNoDefinition(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a table", statement: "CREATE TABLE t (id INT PRIMARY KEY)"},
		{name: "a table named event", statement: "CREATE TABLE event (id INT PRIMARY KEY)"},
		{name: "a temporary table", statement: "CREATE TEMPORARY TABLE t (id INT)"},
		{name: "a replaced table", statement: "CREATE OR REPLACE TABLE t (id INT)"},
		{name: "a unique index", statement: "CREATE UNIQUE INDEX i ON t (id)"},
		{name: "a vector index", statement: "CREATE VECTOR INDEX i ON t (v)"},
		{name: "a sequence", statement: "CREATE OR REPLACE SEQUENCE s"},
		{name: "a database", statement: "CREATE DATABASE IF NOT EXISTS d"},
		{name: "a user", statement: "CREATE USER u IDENTIFIED BY 'x'"},
		{name: "a spatial reference system", statement: "CREATE OR REPLACE SPATIAL REFERENCE SYSTEM 4120 NAME 'n' DEFINITION 'd'"},
		{name: "an altered table", statement: "ALTER TABLE t MODIFY code VARCHAR(5)"},
		{name: "an online altered table", statement: "ALTER ONLINE IGNORE TABLE t ADD COLUMN c INT"},
		{name: "the instance", statement: "ALTER INSTANCE ROTATE INNODB MASTER KEY"},
		{name: "a dropped trigger", statement: "DROP TRIGGER t_bi"},
		{name: "an insert naming a trigger", statement: "INSERT INTO t VALUES (1, 'CREATE TRIGGER')"},
		{name: "a no-break space inside a name", statement: "CREATE\u00a0TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO other.t VALUES (81)"},
		{name: "an empty statement", statement: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok, err := mysqlstored.Statement(mysql84, test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, mysqlstored.Definition{})
		})
	}
}

// A CREATE or ALTER the reader cannot follow to a kind of object it knows is
// an error, never a statement that defines nothing: the guard and the strict
// session refuse it rather than miss a body. Measured on MySQL 8.4 and
// MariaDB 11.8, the server refuses each of these, apart from the kind it
// does not know, which a later server may accept.
func TestStatement_UnreadableDefinition_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a kind in quotes", statement: "CREATE `TRIGGER` t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'"},
		{name: "a host after a space", statement: "CREATE DEFINER=trg @ localhost PROCEDURE p() SELECT 1"},
		{name: "a quoted host after a space", statement: "CREATE DEFINER=trg@ 'localhost' PROCEDURE p() SELECT 1"},
		{name: "a host with a dash", statement: "CREATE DEFINER=trg@my-host PROCEDURE p() SELECT 1"},
		{name: "an exponent past the host", statement: "CREATE DEFINER=trg@1e+5 PROCEDURE p() SELECT 1"},
		{name: "a no-break space after the host", statement: "CREATE DEFINER=root@h\u00a0PROCEDURE p() SELECT 1"},
		{name: "a definer without its =", statement: "CREATE DEFINER app@localhost VIEW v AS SELECT 1"},
		{name: "parentheses after a user", statement: "CREATE DEFINER=app() VIEW v AS SELECT 1"},
		{name: "a definer the statement does not finish", statement: "CREATE DEFINER="},
		{name: "OR REPLACE after ALTER", statement: "ALTER OR REPLACE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'"},
		{name: "four digits in a comment", statement: "CREATE /*!1234*/ TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x'"},
		{name: "a kind it does not know", statement: "CREATE LIBRARY lib LANGUAGE JAVASCRIPT AS 'export function f() {}'"},
		{name: "a table after a definer", statement: "CREATE DEFINER=app@h TABLE t (id INT)"},
		{name: "a keyword spelled with a long s", statement: "CREATE \u017fEQUENCE s"},
		{name: "a keyword spelled with a dotless i", statement: "CREATE TR\u0131GGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO other.t VALUES (1)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok, err := mysqlstored.Statement(mysql84, test.statement)

			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableDefinition)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, mysqlstored.Definition{})
		})
	}
}

// The account a DEFINER clause names: the user token as written, and the host
// as one token holding the name the server reads.
func TestStatement_ReadsTheDefinerHost(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		user      string
		host      string
	}{
		{name: "quoted", statement: "CREATE DEFINER=`app`@`%` VIEW v AS SELECT 1", user: "`app`", host: "`%`"},
		{name: "strings", statement: "CREATE DEFINER='app'@'10.0.%' VIEW v AS SELECT 1", user: "'app'", host: "'10.0.%'"},
		{name: "dotted", statement: "CREATE DEFINER=trg@localhost.localdomain VIEW v AS SELECT 1", user: "trg", host: "localhost.localdomain"},
		{name: "an address", statement: "CREATE DEFINER=trg@10.0.0.1 VIEW v AS SELECT 1", user: "trg", host: "10.0.0.1"},
		{name: "a number and a name", statement: "CREATE DEFINER=trg@1e5x.y$z_w VIEW v AS SELECT 1", user: "trg", host: "1e5x.y$z_w"},
		{name: "a keyword", statement: "CREATE DEFINER=trg@localhost.procedure PROCEDURE p() SELECT 1", user: "trg", host: "localhost.procedure"},
		{name: "empty", statement: "CREATE DEFINER=trg@ VIEW v AS SELECT 1", user: "trg", host: ""},
		{name: "ended by a comment", statement: "CREATE DEFINER=trg@localhost/**/VIEW v AS SELECT 1", user: "trg", host: "localhost"},
		{name: "a dollar sign", statement: "CREATE DEFINER=root@$ VIEW v AS SELECT 1", user: "root", host: "$"},
		{name: "a dollar sign after a dot", statement: "CREATE DEFINER=root@x.$ VIEW v AS SELECT 1", user: "root", host: "x.$"},
		{name: "a dollar sign after a digit", statement: "CREATE DEFINER=root@1$ VIEW v AS SELECT 1", user: "root", host: "1$"},
		{name: "a dot after a dollar sign", statement: "CREATE DEFINER=root@$.x VIEW v AS SELECT 1", user: "root", host: "$.x"},
		{name: "two dollar signs", statement: "CREATE DEFINER=root@$$ VIEW v AS SELECT 1", user: "root", host: "$$"},
		{name: "a character outside ASCII", statement: "CREATE DEFINER=root@h\u00e9 VIEW v AS SELECT 1", user: "root", host: "h\u00e9"},
		{name: "a backslash in quotes", statement: "CREATE DEFINER=root@`h\\` VIEW v AS SELECT 1", user: "root", host: "`h\\`"},
		{name: "a dollar sign as the user", statement: "CREATE DEFINER=$@h VIEW v AS SELECT 1", user: "$", host: "h"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok, err := mysqlstored.Statement(mysql84, test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got.Definer, qt.IsNotNil)
			c.Assert(got.Definer.User.Value, qt.Equals, test.user)
			c.Assert(got.Definer.Host, qt.IsNotNil)
			c.Assert(got.Definer.Host.Value, qt.Equals, test.host)
		})
	}
}

// A definer written without a host has none, and a statement without a
// DEFINER clause names no definer.
func TestStatement_ReadsADefinerWithoutAHost(t *testing.T) {
	c := qt.New(t)

	hostless, ok, err := mysqlstored.Statement(mysql84, "CREATE DEFINER = CURRENT_USER ( ) VIEW v AS SELECT 1")
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(hostless.Definer, qt.IsNotNil)
	c.Assert(hostless.Definer.User.Value, qt.Equals, "CURRENT_USER")
	c.Assert(hostless.Definer.Host, qt.IsNil)

	none, ok, err := mysqlstored.Statement(mysql84, "CREATE VIEW v AS SELECT 1")
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(none.Definer, qt.IsNil)
}

// An executable comment is read as the server reads it, measured on MySQL
// 8.4.13 and MariaDB 11.8.9: kind is what the statement defines there, and ""
// where the comment the words stand in does not run.
func TestStatement_ReadsExecutableCommentsAsTheServerDoes(t *testing.T) {
	const trigger = " TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET NEW.code = 'x' */"
	tests := []struct {
		name      string
		server    mysqlstored.Server
		statement string
		kind      string
	}{
		{name: "no version, on MySQL", server: mysql84, statement: "/*! CREATE" + trigger, kind: "TRIGGER"},
		{name: "no version, on MariaDB", server: mariadb, statement: "/*! CREATE" + trigger, kind: "TRIGGER"},
		{name: "a five-digit version reached", server: mysql84, statement: "/*!80413 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a five-digit version not reached", server: mysql84, statement: "/*!80414 CREATE" + trigger},
		{name: "a six-digit version not reached on MySQL", server: mysql84, statement: "/*!100000 CREATE" + trigger},
		{name: "a six-digit version reached on MySQL", server: mysql2610, statement: "/*!100000 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a six-digit version on MariaDB", server: mariadb, statement: "/*!100000 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a six-digit version MariaDB has not reached", server: mariadb, statement: "/*!110810 CREATE" + trigger},
		{name: "a MySQL 5.7 version on MySQL", server: mysql84, statement: "/*!50700 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a MySQL 5.7 version on MariaDB", server: mariadb, statement: "/*!50700 CREATE" + trigger},
		{name: "a MySQL 9 version on MariaDB", server: mariadb, statement: "/*!90000 CREATE" + trigger},
		{name: "a version before 5.7 on MariaDB", server: mariadb, statement: "/*!50699 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a MariaDB comment on MariaDB", server: mariadb, statement: "/*M! CREATE" + trigger, kind: "TRIGGER"},
		{name: "a MariaDB comment with a version", server: mariadb, statement: "/*M!100100 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a MariaDB comment with a MySQL 5.7 version", server: mariadb, statement: "/*M!50700 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a MariaDB comment on MySQL", server: mysql84, statement: "/*M! CREATE" + trigger},
		{name: "a lowercase m on MariaDB", server: mariadb, statement: "/*m! CREATE" + trigger},
		{name: "a version before 5.7 on a server of unknown version", server: unknown, statement: "/*!50003 CREATE" + trigger, kind: "TRIGGER"},
		{name: "a kind split across comments", server: mariadb, statement: "/*!50003 CREATE*/ /*M!100100 OR REPLACE*/ /*!50003" + trigger, kind: "TRIGGER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, _, err := mysqlstored.Statement(test.server, test.statement)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Kind, qt.Equals, test.kind)
		})
	}
}

// A comment whose effect on the server the reader cannot tell is an error,
// not a guess: the strict session refuses the statement rather than run a
// body it may have missed.
func TestStatement_UnreadableComment_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		server    mysqlstored.Server
		statement string
	}{
		{name: "seven digits on MySQL", server: mysql84, statement: "/*!8000000 CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1 */"},
		{name: "seven digits on MariaDB", server: mariadb, statement: "/*M!1000000 CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1 */"},
		{name: "a comment that does not close", server: mysql84, statement: "/*!50003 CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1"},
		{name: "a comment that holds another", server: mysql84, statement: "CREATE /*!99999 OR /* x */ REPLACE */ TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1"},
		{name: "a string past the comment's end", server: mysql84,
			statement: "/*!50003 CREATE DEFINER='a*/b'@'%' TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1 */"},
		{name: "a quoted name past the comment's end", server: mysql84,
			statement: "/*!50003 CREATE DEFINER=`a*/b`@`%` TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1 */"},
		{name: "a line comment past the comment's end", server: mysql84,
			statement: "/*!50003 CREATE # x */ TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1"},
		{name: "a version from 5.7 on a server of unknown version", server: unknown, statement: "/*!50700 CREATE TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1 */"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok, err := mysqlstored.Statement(test.server, test.statement)

			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableComment)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, mysqlstored.Definition{})
		})
	}
}

// A session's sql_mode decides where a quoted user ends: NO_BACKSLASH_ESCAPES
// reads 'a\' as a whole string, ANSI_QUOTES reads "a\" as a name, and
// MariaDB's MSSQL mode reads [a'b] as one, so the definer ends there and the
// kind follows it. The default rules read on past the quote, find no kind,
// and cannot read the statement.
func TestStatement_ReadsTheSessionsQuoteRules(t *testing.T) {
	const trigger = " TRIGGER t_bi BEFORE INSERT ON t FOR EACH ROW SET @a = 1"
	tests := []struct {
		name      string
		server    mysqlstored.Server
		statement string
	}{
		{name: "NO_BACKSLASH_ESCAPES", server: mysqlstored.Server{Dialect: platform.MySQL, NoBackslashEscapes: true},
			statement: `CREATE DEFINER='a\'@'%'` + trigger},
		{name: "ANSI_QUOTES", server: mysqlstored.Server{Dialect: platform.MySQL, AnsiQuotes: true},
			statement: `CREATE DEFINER="a\"@'%'` + trigger},
		{name: "MSSQL", server: mysqlstored.Server{Dialect: platform.MariaDB, BracketNames: true},
			statement: `CREATE DEFINER=[a'b]@'%'` + trigger},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			read, ok, err := mysqlstored.Statement(test.server, test.statement)
			c.Assert(err, qt.IsNil)
			c.Assert(ok, qt.IsTrue)
			c.Assert(read.Kind, qt.Equals, "TRIGGER")

			_, ok, err = mysqlstored.Statement(mysqlstored.Server{Dialect: test.server.Dialect}, test.statement)
			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableDefinition)
			c.Assert(ok, qt.IsFalse)
		})
	}
}

// Wrapped finds the statement a SET STATEMENT or an ANALYZE runs: want is
// the first word of that statement.
func TestWrapped_HoldsAStatement(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{name: "SET STATEMENT", statement: "SET STATEMENT max_statement_time=0 FOR INSERT INTO t VALUES (1)", want: "INSERT"},
		{name: "SET STATEMENT with a list", statement: "set statement a=1, b='for' for delete from t", want: "delete"},
		{name: "SET STATEMENT with FOR in a subquery", statement: "SET STATEMENT a=(SELECT 1 FROM t FOR UPDATE) FOR DELETE FROM t", want: "DELETE"},
		{name: "SET STATEMENT with FOR in a function", statement: "SET STATEMENT lc_time_names=SUBSTRING('en_USx' FROM 1 FOR 5) FOR DELETE FROM t", want: "DELETE"},
		{name: "ANALYZE", statement: "ANALYZE DELETE FROM t", want: "DELETE"},
		{name: "ANALYZE with a format", statement: "ANALYZE FORMAT=JSON SELECT 1", want: "SELECT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tokens, err := mysqlstored.Tokens(mariadb, test.statement)
			c.Assert(err, qt.IsNil)

			start, ok := mysqlstored.Wrapped(tokens)

			c.Assert(ok, qt.IsTrue)
			c.Assert(tokens[start].Value, qt.Equals, test.want)
		})
	}
}

// A statement that runs no other, ANALYZE TABLE and MySQL's EXPLAIN ANALYZE
// among them.
func TestWrapped_HoldsNone(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "SET STATEMENT without FOR", statement: "SET STATEMENT max_statement_time=0"},
		{name: "SET STATEMENT ending in FOR", statement: "SET STATEMENT max_statement_time=0 FOR"},
		{name: "a plain SET", statement: "SET max_statement_time=0"},
		{name: "ANALYZE TABLE", statement: "ANALYZE TABLE t"},
		{name: "ANALYZE NO_WRITE_TO_BINLOG", statement: "ANALYZE NO_WRITE_TO_BINLOG TABLE t"},
		{name: "ANALYZE LOCAL", statement: "ANALYZE LOCAL TABLE t"},
		{name: "ANALYZE alone", statement: "ANALYZE"},
		{name: "EXPLAIN ANALYZE", statement: "EXPLAIN ANALYZE UPDATE t SET c = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			tokens, err := mysqlstored.Tokens(mariadb, test.statement)
			c.Assert(err, qt.IsNil)

			start, ok := mysqlstored.Wrapped(tokens)

			c.Assert(ok, qt.IsFalse)
			c.Assert(start, qt.Equals, 0)
		})
	}
}

// Tokens keeps each token's position in the statement, a token read from an
// executable comment included, which is how a host is told from the words
// after it.
func TestTokens_KeepsPositions(t *testing.T) {
	c := qt.New(t)

	tokens, err := mysqlstored.Tokens(mysql84, "CREATE /*!50017 DEFINER=a@b*/ VIEW v")

	c.Assert(err, qt.IsNil)
	c.Assert(tokens, qt.HasLen, 8)
	c.Assert(tokens[1].Value, qt.Equals, "DEFINER")
	c.Assert(tokens[1].Start, qt.Equals, 16)
	c.Assert(tokens[5].Value, qt.Equals, "b")
	c.Assert(tokens[5].End, qt.Equals, 27)
	c.Assert(tokens[6].Value, qt.Equals, "VIEW")
	c.Assert(tokens[6].Start, qt.Equals, 30)
}

func TestWrittenAsExecutable(t *testing.T) {
	tests := []struct {
		comment string
		want    bool
	}{
		{comment: "/*! x */", want: true},
		{comment: "/*!50003 x */", want: true},
		{comment: "/*M! x */", want: true},
		{comment: "/*m!100100 x */", want: true},
		{comment: "/* x */"},
		{comment: "/*+ hint */"},
		{comment: "/*M x */"},
		{comment: "-- /*! x */"},
	}
	for _, test := range tests {
		t.Run(test.comment, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlstored.WrittenAsExecutable(test.comment), qt.Equals, test.want)
		})
	}
}

func TestVersionID(t *testing.T) {
	tests := []struct {
		version string
		want    int
	}{
		{version: "8.0.46", want: 80046},
		{version: "8.1.0", want: 80100},
		{version: "8.2.0", want: 80200},
		{version: "8.4.11", want: 80411},
		{version: "26.10.0", want: 261000},
		{version: "5.7.44", want: 50744},
		{version: "8.0.46-0ubuntu1", want: 80046},
		{version: "12.3.3-MariaDB-ubu2404", want: 120303},
		{version: "11.4.2-MariaDB-log", want: 110402},
		{version: "8.0", want: 80000},
		{version: "8.x.1", want: 0},
		{version: "8-beta.1", want: 0},
		{version: "garbage", want: 0},
		{version: "", want: 0},
	}
	for _, test := range tests {
		t.Run(test.version, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlstored.VersionID(test.version), qt.Equals, test.want)
		})
	}
}
