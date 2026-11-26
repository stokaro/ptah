package mysqlstored_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/mysqlstored"
)

// The sessions a stored body is read for, by the sql_mode it names.
var (
	defaultMode = mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413}
	noEscapes   = mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413, NoBackslashEscapes: true}
	ansiQuotes  = mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413, AnsiQuotes: true}
	mariaANSI   = mysqlstored.Server{Dialect: platform.MariaDB, VersionID: 110809, AnsiQuotes: true}
)

// Each definition is one SHOW CREATE printed on MySQL 8.4.13, MySQL 26.10.0,
// MariaDB 11.8.9 or MariaDB 12.3.3, and the body is the one the statement
// that created the object wrote.
func TestBody_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		server     mysqlstored.Server
		definition string
		body       string
	}{
		{
			name:       "a MySQL trigger with an escaped quote",
			server:     defaultMode,
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `tr_sq` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.code = 'a\\'b'",
			body:       `SET NEW.code = 'a\'b'`,
		},
		{
			name:       "a MySQL trigger keeps the comment before its body",
			server:     defaultMode,
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `tr1` BEFORE INSERT ON `t q` FOR EACH ROW /* lead */ SET NEW.code = 'x' -- tail",
			body:       "/* lead */ SET NEW.code = 'x' -- tail",
		},
		{
			name:       "a MySQL trigger whose body is a block",
			server:     defaultMode,
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `tr3` BEFORE UPDATE ON `t q` FOR EACH ROW BEGIN\n  -- inner 'q\\'s\n  SET NEW.code = 'a\\'b'; /* c */\nEND",
			body:       "BEGIN\n  -- inner 'q\\'s\n  SET NEW.code = 'a\\'b'; /* c */\nEND",
		},
		{
			name:       "a MariaDB trigger head as written",
			server:     mariadb,
			definition: "CREATE DEFINER=`root`@`%` trigger /* head */ tr1 before insert on `t q` for each row /* lead */ SET NEW.code = 'x' -- tail",
			body:       "/* lead */ SET NEW.code = 'x' -- tail",
		},
		{
			name:       "a MariaDB trigger whose body starts on the next line",
			server:     mariadb,
			definition: "CREATE DEFINER=`root`@`%` TRIGGER tr3 BEFORE UPDATE ON `t q` FOR EACH ROW\nBEGIN\n  SET NEW.code = 'a\\'b';\nEND",
			body:       "BEGIN\n  SET NEW.code = 'a\\'b';\nEND",
		},
		{
			name:       "a MariaDB trigger stored under ANSI_QUOTES",
			server:     mariaANSI,
			definition: "CREATE DEFINER=\"root\"@\"%\" TRIGGER tr6 AFTER UPDATE ON \"t q\" FOR EACH ROW SET @f = 'a\\'b'",
			body:       `SET @f = 'a\'b'`,
		},
		{
			name:       "a trigger with a FOLLOWS clause",
			server:     mariadb,
			definition: "CREATE DEFINER=`root`@`%` trigger tr2 before insert on `t` for each row follows tr1 SET NEW.n = 1",
			body:       "SET NEW.n = 1",
		},
		{
			name:       "a trigger stored under NO_BACKSLASH_ESCAPES",
			server:     noEscapes,
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `tr_nbe` BEFORE INSERT ON `t` FOR EACH ROW SET NEW.code = 'a\\'",
			body:       `SET NEW.code = 'a\'`,
		},
		{
			name:       "a trigger whose body holds a comment the replay guard refuses",
			server:     defaultMode,
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `pf_bi` BEFORE INSERT ON `pf_t` FOR EACH ROW SET NEW.code = 'a' /*!99999 /* */ */ ''",
			body:       "SET NEW.code = 'a' /*!99999 /* */ */ ''",
		},
		{
			name:       "a MySQL function with characteristics",
			server:     defaultMode,
			definition: "CREATE DEFINER=`root`@`%` FUNCTION `f1`(a int) RETURNS varchar(20) CHARSET utf8mb4\n    DETERMINISTIC\n    COMMENT 'c''m'\nRETURN 'a\\'b'",
			body:       `RETURN 'a\'b'`,
		},
		{
			name:       "a MySQL function whose type is followed by a characteristic",
			server:     mysql2610,
			definition: "CREATE DEFINER=`root`@`%` FUNCTION `f3`() RETURNS enum('a','b''c') CHARSET latin1\n    READS SQL DATA\n    SQL SECURITY INVOKER\nRETURN 'a'",
			body:       "RETURN 'a'",
		},
		{
			name:       "a MariaDB function without characteristics",
			server:     mariadb,
			definition: "CREATE DEFINER=`root`@`%` FUNCTION `f1`() RETURNS int(11)\nRETURN 1",
			body:       "RETURN 1",
		},
		{
			name:       "a MariaDB function whose body starts with a label",
			server:     mariadb,
			definition: "CREATE DEFINER=`root`@`%` FUNCTION `f4`() RETURNS text CHARSET latin1 COLLATE latin1_bin\n    MODIFIES SQL DATA\n    COMMENT 'x\\ny'\nlbl: BEGIN RETURN 'q'; END",
			body:       "lbl: BEGIN RETURN 'q'; END",
		},
		{
			name:       "a MariaDB aggregate function",
			server:     mariadb,
			definition: "CREATE DEFINER=`root`@`%` AGGREGATE FUNCTION `fa`(x INT) RETURNS int(11)\nBEGIN DECLARE s INT DEFAULT 0; RETURN s; END",
			body:       "BEGIN DECLARE s INT DEFAULT 0; RETURN s; END",
		},
		{
			name:       "a MariaDB function stored under ORACLE",
			server:     mariaANSI,
			definition: "CREATE DEFINER=\"root\"@\"%\" FUNCTION \"fo\"(a IN NUMBER) RETURN double\nAS BEGIN RETURN a || 'x\\'y'; END",
			body:       `AS BEGIN RETURN a || 'x\'y'; END`,
		},
		{
			name:       "a MySQL 9.7 function in JavaScript",
			server:     mysqlstored.Server{Dialect: platform.MySQL, VersionID: 90704},
			definition: "CREATE DEFINER=`root`@`localhost` FUNCTION `fj`() RETURNS int\n    DETERMINISTIC\n    LANGUAGE JAVASCRIPT\nAS $$return 1$$",
			body:       "AS $$return 1$$",
		},
		{
			name:       "a procedure whose parameters hold a comment",
			server:     mysql2610,
			definition: "CREATE DEFINER=`root`@`%` PROCEDURE `p2`(\n  IN a INT -- c'\n)\nBEGIN END",
			body:       "BEGIN END",
		},
		{
			name:       "a procedure with a comment",
			server:     defaultMode,
			definition: "CREATE DEFINER=`root`@`%` PROCEDURE `p1`(IN a int, OUT b varchar(9))\n    COMMENT 'x'\nBEGIN SET b = 'a\\'b'; END",
			body:       `BEGIN SET b = 'a\'b'; END`,
		},
		{
			name:       "a MariaDB procedure stored under ORACLE",
			server:     mariaANSI,
			definition: "CREATE DEFINER=\"root\"@\"%\" PROCEDURE \"po\"(a IN NUMBER)\nIS BEGIN NULL; END",
			body:       "IS BEGIN NULL; END",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			body, err := mysqlstored.Body(test.server, test.definition)

			c.Assert(err, qt.IsNil)
			c.Assert(body, qt.Equals, test.body)
		})
	}
}

// A definition whose body cannot be found is an error, never a body that
// holds part of the head.
func TestBody_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		wantErr    string
	}{
		{
			name:       "a view",
			definition: "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS select 1 AS `1`",
			wantErr:    `a stored body that cannot be read: a view keeps no body this reads`,
		},
		{
			name:       "a statement that creates nothing",
			definition: "SELECT 1",
			wantErr:    `a stored body that cannot be read: "SELECT 1": it is not a CREATE of a stored object`,
		},
		{
			name:       "a trigger head without FOR EACH ROW",
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `t` BEFORE INSERT ON `t`",
			wantErr:    `(?s)a stored body that cannot be read: .*: its head does not end where a trigger's does`,
		},
		{
			name:       "a trigger head and no body",
			definition: "CREATE DEFINER=`root`@`%` TRIGGER `t` BEFORE INSERT ON `t` FOR EACH ROW  ",
			wantErr:    `a stored body that cannot be read: nothing follows its head`,
		},
		{
			name:       "a function without RETURNS",
			definition: "CREATE DEFINER=`root`@`%` FUNCTION `f`()\nRETURN 1",
			wantErr:    `(?s)a stored body that cannot be read: .*: its head does not end where a function's does`,
		},
		{
			name:       "a routine whose parameter list does not close",
			definition: "CREATE DEFINER=`root`@`%` PROCEDURE `p`(IN a int",
			wantErr:    `(?s)a stored body that cannot be read: .*: its head does not end where a procedure's does`,
		},
		{
			name:       "a head that ends inside an executable comment",
			definition: "CREATE DEFINER=`root`@`%` /*!50003 TRIGGER `t` BEFORE INSERT ON `t` FOR EACH ROW SET @a = 1 */",
			wantErr:    `a stored body that cannot be read: its head ends inside an executable comment`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			body, err := mysqlstored.Body(defaultMode, test.definition)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableBody)
			c.Assert(body, qt.Equals, "")
		})
	}
}

// A comment in the head that cannot be read stops the reading before the
// head ends, so the error names the comment.
func TestBody_FailurePathUnreadableCommentInTheHead(t *testing.T) {
	c := qt.New(t)

	body, err := mysqlstored.Body(defaultMode, "CREATE DEFINER=`root`@`%` /*!99999 /* */ */ TRIGGER `t` BEFORE INSERT ON `t` FOR EACH ROW SET @a = 1")

	c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableComment)
	c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableBody)
	c.Assert(body, qt.Equals, "")
}

// ServerFor reads the modes of a stored object's sql_mode that change where
// a string, a quoted name or a comment ends; MSSQL reads square brackets as
// a name on MariaDB alone.
func TestServerFor(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		mode    string
		want    mysqlstored.Server
	}{
		{
			name: "the default mode", dialect: platform.MySQL,
			mode: "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
			want: mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413},
		},
		{
			name: "NO_BACKSLASH_ESCAPES", dialect: platform.MySQL, mode: "ONLY_FULL_GROUP_BY,NO_BACKSLASH_ESCAPES",
			want: mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413, NoBackslashEscapes: true},
		},
		{
			name: "ANSI_QUOTES", dialect: platform.MySQL, mode: "ANSI_QUOTES,STRICT_TRANS_TABLES",
			want: mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413, AnsiQuotes: true},
		},
		{
			name: "MSSQL on MySQL", dialect: platform.MySQL, mode: "MSSQL",
			want: mysqlstored.Server{Dialect: platform.MySQL, VersionID: 80413},
		},
		{
			name: "MSSQL on MariaDB", dialect: platform.MariaDB, mode: "PIPES_AS_CONCAT,ANSI_QUOTES,MSSQL",
			want: mysqlstored.Server{Dialect: platform.MariaDB, VersionID: 80413, AnsiQuotes: true, BracketNames: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(mysqlstored.ServerFor(test.dialect, 80413, test.mode), qt.Equals, test.want)
		})
	}
}

// Respell writes a body stored under one mode so a session under another
// reads the same program; a body that reads alike under both is kept byte
// for byte.
func TestRespell_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		from, to mysqlstored.Server
		want     string
	}{
		{
			name: "the same mode", body: `SET NEW.code = 'a\'b\qc'`, from: defaultMode, to: defaultMode,
			want: `SET NEW.code = 'a\'b\qc'`,
		},
		{
			name: "a backslash stored as the character", body: `SET NEW.code = 'a\b''c'`, from: noEscapes, to: defaultMode,
			want: `SET NEW.code = 'a\\b''c'`,
		},
		{
			name: "an escaped quote read where a backslash is the character", body: `SET NEW.code = 'a\'b'`, from: defaultMode, to: noEscapes,
			want: `SET NEW.code = 'a''b'`,
		},
		{
			name: "a string that reads alike in both", body: `SET NEW.code = 'a\%b', @x = 'plain'`, from: defaultMode, to: noEscapes,
			want: `SET NEW.code = 'a\%b', @x = 'plain'`,
		},
		{
			name: "a name between double quotes", body: `SET NEW."code" = 'x'`, from: ansiQuotes, to: defaultMode,
			want: "SET NEW.`code` = 'x'",
		},
		{
			name: "a string between double quotes read where they quote a name", body: `SET NEW.code = "a\"b"`, from: defaultMode, to: ansiQuotes,
			want: `SET NEW.code = 'a"b'`,
		},
		{
			name: "a comment is kept", body: "SET NEW.code = 'a\\b' -- it's \\ fine\n", from: noEscapes, to: defaultMode,
			want: "SET NEW.code = 'a\\\\b' -- it's \\ fine\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			body, err := mysqlstored.Respell(test.body, test.from, test.to)

			c.Assert(err, qt.IsNil)
			c.Assert(body, qt.Equals, test.want)
		})
	}
}

// Respell does not write the text of an executable comment again, so one
// that holds a quote is refused where the modes differ.
func TestRespell_FailurePath(t *testing.T) {
	c := qt.New(t)

	body, err := mysqlstored.Respell(`SET NEW.code = 'a' /*!50003 + 'b\c' */`, noEscapes, defaultMode)

	c.Assert(err, qt.ErrorMatches, `a stored body that cannot be read: the executable comment "/\*!50003 \+ 'b\\\\c' \*/" holds a quote or a backslash`)
	c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableBody)
	c.Assert(body, qt.Equals, "")
}
