package migrator

// White-box testing required: the rules are properties of one unexported
// validator, and the engines they speak for are MySQL, MariaDB and Oracle.
// Driving them through the exported path needs a connection to one of those,
// and asking a real server is the one thing these tests must not do: if a
// guard were wrong the server would write the file or advance the sequence,
// which is the outcome the guard exists to prevent. So the assertion is that
// the text is refused before any query is built.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A MySQL-family `SELECT ... INTO OUTFILE` begins with SELECT and writes a file
// on the server, outside anything the transaction owns: no isolation level and
// no read-only session takes that back. Reading the assertion is the only thing
// in front of it.
//
// The recognition is sqlreach's, shared with the plan guard, so these rows
// measure that the validator consults it and that the effective SQL is what it
// consults it with -- not a second copy of the construct list.
func TestValidateCheckAssertionStatically_RefusesAServerSideFileWrite(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		serverVersion string
		assertion     string
		wantErr       string
	}{
		{
			name:      "mysql outfile",
			dialect:   "mysql",
			assertion: `SELECT 'x' INTO OUTFILE '/tmp/ptah-check-probe'`,
			wantErr:   `check assertion must not use SELECT \.\.\. INTO OUTFILE, which writes a file on the database server host`,
		},
		{
			name:      "mariadb outfile",
			dialect:   "mariadb",
			assertion: `SELECT 'x' INTO OUTFILE '/tmp/ptah-check-probe'`,
			wantErr:   `check assertion must not use SELECT \.\.\. INTO OUTFILE, which writes a file on the database server host`,
		},
		{
			name:      "mysql dumpfile",
			dialect:   "mysql",
			assertion: `SELECT 'x' INTO DUMPFILE '/tmp/ptah-check-probe'`,
			wantErr:   `check assertion must not use SELECT \.\.\. INTO DUMPFILE, which writes a file on the database server host`,
		},
		// An executable comment is SQL the server runs and the lexer reports as
		// one opaque token, so a scan of the text as written never sees the
		// clause. The rule reads the effective SQL, which is the same form the
		// SELECT-only rule already reads.
		{
			name:      "mysql outfile inside an executable comment",
			dialect:   "mysql",
			assertion: `SELECT 'x' /*! INTO OUTFILE '/tmp/ptah-check-probe' */`,
			wantErr:   `check assertion must not use SELECT \.\.\. INTO OUTFILE, which writes a file on the database server host`,
		},
		// A versioned executable comment needs a server version to decide
		// whether the server would run it. This one would, so the clause is
		// real SQL and the rule reads it.
		{
			name:          "mysql dumpfile inside a versioned executable comment",
			dialect:       "mysql",
			serverVersion: "8.0.36",
			assertion:     `SELECT 'x' /*!50001 INTO DUMPFILE '/tmp/ptah-check-probe' */`,
			wantErr:       `check assertion must not use SELECT \.\.\. INTO DUMPFILE, which writes a file on the database server host`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, test.serverVersion),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// An Oracle NEXTVAL advances the sequence for good, and Oracle opens no
// read-only session here, so the text is the only place it can be refused.
func TestValidateCheckAssertionStatically_RefusesAnOracleSequenceAdvance(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "qualified nextval",
			assertion: `SELECT release_probe.NEXTVAL FROM dual`,
		},
		{
			name:      "lower case",
			assertion: `SELECT release_probe.nextval FROM dual`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "oracle", ""),
				qt.ErrorMatches, `check assertion must not advance an Oracle sequence with NEXTVAL`)
		})
	}
}

// The control: the rule reads the construct, not the engine, so an ordinary
// predicate on the same dialect is accepted and a check whose text merely
// mentions the word in a literal is not refused for it.
func TestValidateCheckAssertionStatically_AcceptsAnOrdinaryPredicate(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
	}{
		{
			name:      "mysql count predicate",
			dialect:   "mysql",
			assertion: `SELECT COUNT(*) = 0 FROM users WHERE tier IS NULL`,
		},
		{
			name:      "the word in a string literal",
			dialect:   "mysql",
			assertion: `SELECT COUNT(*) = 0 FROM audit WHERE note = 'INTO OUTFILE'`,
		},
		{
			name:      "a dialect with no such construct",
			dialect:   "postgres",
			assertion: `SELECT COUNT(*) = 0 FROM users`,
		},
		{
			name:      "oracle reading a sequence without advancing it",
			dialect:   "oracle",
			assertion: `SELECT release_probe.CURRVAL > 0 FROM dual`,
		},
		{
			name:      "oracle counting rows",
			dialect:   "oracle",
			assertion: `SELECT COUNT(*) FROM users WHERE tier IS NULL`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""), qt.IsNil)
		})
	}
}

// Each row is a SELECT that touches something no transaction owns, which is
// why the recognition sits in one catalog rather than in a list per caller.
//
// dblink runs its statement through a second connection, which commits on its
// own; the read-only transaction binds the local one. And under
// NO_BACKSLASH_ESCAPES a MySQL server ends the string at the quote the
// backslash reading swallows, so the INTO OUTFILE clause the scanner saw as
// data is live SQL on the server. The scan reads both interpretations rather
// than asking the session which mode it is in.
func TestValidateCheckAssertionStatically_RefusesWhatReachesOutsideTheDatabase(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
		wantErr   string
	}{
		{
			name:      "postgres dblink writes through another connection",
			dialect:   "postgres",
			assertion: `SELECT dblink_exec('dbname=target', 'INSERT INTO audit VALUES (1)') = 'INSERT 0 1'`,
			wantErr:   `check assertion must not use dblink, which opens a connection to another database server`,
		},
		{
			name:      "postgres reads a file on the host",
			dialect:   "postgres",
			assertion: `SELECT length(pg_read_file('/etc/passwd')) > 0`,
			wantErr:   `check assertion must not use pg_read_file, which reads a file on the database server host`,
		},
		{
			name:      "sql server runs a shell command",
			dialect:   "sqlserver",
			assertion: `SELECT 1 FROM (SELECT xp_cmdshell('whoami')) AS t`,
			wantErr:   `check assertion must not use xp_cmdshell, which .*`,
		},
		{
			name:      "mysql outfile behind a backslash the server does not escape",
			dialect:   "mysql",
			assertion: `SELECT 'x\' INTO OUTFILE '/tmp/ptah-check-probe'`,
			wantErr:   `check assertion must not use SELECT \.\.\. INTO OUTFILE, which writes a file on the database server host`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// The two decisions compose, which is what this row is for. Expanding an
// executable comment needs a tokenizer, and which characters end a string
// decides whether the comment is a comment at all: with backslash escapes on
// the string opens at `'x` and runs through the comment, so nothing is
// expanded and a scan of that expansion sees a harmless SELECT; with them off
// the string ends at the second quote and the comment is SQL the server runs.
// Reading one expansion under both interpretations is not enough -- the
// expansion has to happen under both.
func TestValidateCheckAssertionStatically_RefusesAcrossEscapeModesAndComments(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		serverVersion string
		assertion     string
	}{
		{
			name:      "mysql outfile in an executable comment behind a backslash",
			dialect:   "mysql",
			assertion: `SELECT 'x\' /*! INTO OUTFILE '/tmp/ptah-check-probe' */`,
		},
		{
			name:      "mariadb dumpfile in an executable comment behind a backslash",
			dialect:   "mariadb",
			assertion: `SELECT 'x\' /*M! INTO DUMPFILE '/tmp/ptah-check-probe' */`,
		},
		{
			name:          "mysql versioned comment behind a backslash",
			dialect:       "mysql",
			serverVersion: "8.0.36",
			assertion:     `SELECT 'x\' /*!50001 INTO OUTFILE '/tmp/ptah-check-probe' */`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, test.serverVersion),
				qt.IsNotNil)
		})
	}
}

// ClickHouse takes a remote source as a table function in an ordinary FROM
// clause, so a statement that declares no table and begins with SELECT still
// fetches a URL from inside the server's network. It also runs the assertion
// outside any transaction, because its driver implements none, so nothing
// after the statement can take it back.
func TestValidateCheckAssertionStatically_RefusesAClickHouseRemoteSource(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "url",
			assertion: `SELECT count() > 0 FROM url('http://169.254.169.254/latest/meta-data', 'CSV')`,
		},
		{
			name:      "remote",
			assertion: `SELECT count() > 0 FROM remote('other:9000', 'db', 'users')`,
		},
		{
			name:      "s3",
			assertion: `SELECT count() > 0 FROM s3('https://bucket/key', 'CSV')`,
		},
		{
			name:      "file",
			assertion: `SELECT count() > 0 FROM file('/etc/passwd', 'CSV')`,
		},
		{
			name:      "a cluster variant",
			assertion: `SELECT count() > 0 FROM icebergCluster('c', 'https://bucket/key')`,
		},
		{
			name:      "a lake format the first list missed",
			assertion: `SELECT count() > 0 FROM hudi('https://bucket/key')`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "clickhouse", ""),
				qt.ErrorMatches, `check assertion must not use ClickHouse remote table function, which .*`)
		})
	}
}

// The control: an ordinary ClickHouse predicate over a real table is accepted,
// and so is a column or alias that merely shares a name with one of the
// functions, because call position is what the rule reads.
func TestValidateCheckAssertionStatically_AcceptsAnOrdinaryClickHousePredicate(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "count over a table",
			assertion: `SELECT count() = 0 FROM users WHERE tier IS NULL`,
		},
		{
			name:      "a column named like a table function",
			assertion: `SELECT count() = 0 FROM documents WHERE url IS NULL`,
		},
		{
			name:      "a local function whose name starts like one",
			assertion: `SELECT count() = 0 FROM links WHERE urlHash(href) = 0`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "clickhouse", ""), qt.IsNil)
		})
	}
}

// A rule speaks for the grammar that gives the construct its meaning. `url(...)`
// is a remote read on ClickHouse and an ordinary user function name anywhere
// else, so a scan that applied it to every dialect would refuse a PostgreSQL
// assertion for a grammar PostgreSQL does not have.
func TestValidateCheckAssertionStatically_AcceptsAClickHouseNameOnAnotherDialect(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
	}{
		{
			name:      "postgres function named url",
			dialect:   "postgres",
			assertion: `SELECT url(path) IS NOT NULL FROM links`,
		},
		{
			name:      "mysql function named remote",
			dialect:   "mysql",
			assertion: `SELECT remote(id) = 1 FROM links`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""), qt.IsNil)
		})
	}
}

// SQL Server reaches a linked server by function too, and OPENQUERY sat beside
// the two forms the catalog already knew.
func TestValidateCheckAssertionStatically_RefusesALinkedServerQuery(t *testing.T) {
	c := qt.New(t)

	err := validateCheckAssertionStatically(
		`SELECT COUNT(*) > 0 FROM OPENQUERY(remote, 'SELECT id FROM audit')`, "sqlserver", "")

	c.Assert(err, qt.ErrorMatches, `check assertion must not use OPENQUERY, which runs a query on a linked server.*`)
}

// A PostgreSQL read-only transaction permits every one of these and undoes
// none: terminating the connection the verification is running on is not a
// write, and no rollback brings it back.
func TestValidateCheckAssertionStatically_RefusesAServerControlFunction(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
	}{
		{
			name:      "terminate another backend",
			dialect:   "postgres",
			assertion: `SELECT pg_terminate_backend(4242)`,
		},
		{
			name:      "cancel another backend",
			dialect:   "postgres",
			assertion: `SELECT pg_cancel_backend(4242)`,
		},
		{
			name:      "reload the configuration",
			dialect:   "postgres",
			assertion: `SELECT pg_reload_conf()`,
		},
		{
			name:      "rewrite a session setting",
			dialect:   "postgres",
			assertion: `SELECT set_config('statement_timeout', '0', false) = '0'`,
		},
		{
			name:      "on a wire-compatible product",
			dialect:   "cockroachdb",
			assertion: `SELECT pg_terminate_backend(4242)`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""),
				qt.ErrorMatches, `check assertion must not use PostgreSQL server control function, which .*`)
		})
	}
}

// The control: an ordinary read of the same catalog those functions act on is
// still a read.
func TestValidateCheckAssertionStatically_AcceptsACatalogRead(t *testing.T) {
	c := qt.New(t)

	err := validateCheckAssertionStatically(
		`SELECT COUNT(*) = 0 FROM pg_stat_activity WHERE state = 'idle in transaction'`, "postgres", "")

	c.Assert(err, qt.IsNil)
}

// A replication slot outlives everything a session can undo: it is created by
// a SELECT, it survives the rollback and the discarded connection, and it
// retains write-ahead log until an operator removes it.
func TestValidateCheckAssertionStatically_RefusesAReplicationSlotFunction(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "create a physical slot",
			assertion: `SELECT slot_name IS NOT NULL FROM pg_create_physical_replication_slot('ptah_verify')`,
		},
		{
			name:      "drop a slot",
			assertion: `SELECT pg_drop_replication_slot('ptah_verify') IS NULL`,
		},
		{
			name:      "advance a replication origin",
			assertion: `SELECT pg_replication_origin_advance('o', '0/0') IS NULL`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "postgres", ""),
				qt.ErrorMatches, `check assertion must not use PostgreSQL server control function, which .*`)
		})
	}
}

// MySQL starts a line comment at `--` only when whitespace follows, so
// `SELECT 1--1 INTO OUTFILE '/tmp/p'` is one live statement there. A scanner
// that used its own lexer options read a grammar no server has and dropped the
// clause as a comment.
func TestValidateCheckAssertionStatically_ReadsTheDialectsCommentGrammar(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
	}{
		{
			name:      "mysql",
			dialect:   "mysql",
			assertion: `SELECT 1--1 INTO OUTFILE '/tmp/ptah-check-probe'`,
		},
		{
			name:      "mariadb",
			dialect:   "mariadb",
			assertion: `SELECT 1--1 INTO OUTFILE '/tmp/ptah-check-probe'`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""), qt.IsNotNil)
		})
	}
}

// The control on the other side of that rule: PostgreSQL does start a comment
// at a bare `--`, so the same text there is a SELECT with a comment on it.
func TestValidateCheckAssertionStatically_AcceptsABareDashCommentOnPostgres(t *testing.T) {
	c := qt.New(t)

	err := validateCheckAssertionStatically(`SELECT 1--1 IS NOT NULL`, "postgres", "")

	c.Assert(err, qt.IsNil)
}

// Every row is a SELECT whose effect no session undoes: statistics cleared for
// good, a directory on the host listed, a request sent from the server.
func TestValidateCheckAssertionStatically_RefusesMoreThanTheFirstList(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
		wantErr   string
	}{
		{
			name:      "reset the statistics",
			dialect:   "postgres",
			assertion: `SELECT pg_stat_reset() IS NULL`,
			wantErr:   `check assertion must not use PostgreSQL server control function, which .*`,
		},
		{
			name:      "reset one table's counters",
			dialect:   "postgres",
			assertion: `SELECT pg_stat_reset_single_table_counters(1) IS NULL`,
			wantErr:   `check assertion must not use PostgreSQL server control function, which .*`,
		},
		{
			name:      "list the log directory",
			dialect:   "postgres",
			assertion: `SELECT count(*) >= 0 FROM pg_ls_logdir()`,
			wantErr:   `check assertion must not use pg_ls_ directory listing, which .*`,
		},
		{
			name:      "list the write-ahead log directory",
			dialect:   "postgres",
			assertion: `SELECT count(*) >= 0 FROM pg_ls_waldir()`,
			wantErr:   `check assertion must not use pg_ls_ directory listing, which .*`,
		},
		{
			name:      "send an HTTP request from Oracle",
			dialect:   "oracle",
			assertion: `SELECT UTL_HTTP.REQUEST('http://169.254.169.254/') IS NOT NULL FROM dual`,
			wantErr:   `check assertion must not use Oracle network package, which .*`,
		},
		{
			name:      "resolve a host from Oracle",
			dialect:   "oracle",
			assertion: `SELECT UTL_INADDR.GET_HOST_ADDRESS('example.com') IS NOT NULL FROM dual`,
			wantErr:   `check assertion must not use Oracle network package, which .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// The controls those three rules must not swallow: reading the statistics is
// not resetting them, a column named like a package is a column, and the
// Oracle rule speaks only for Oracle.
func TestValidateCheckAssertionStatically_AcceptsTheReadsBesideThem(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
	}{
		{
			name:      "read the statistics",
			dialect:   "postgres",
			assertion: `SELECT numbackends = 0 FROM pg_stat_database WHERE datname = current_database()`,
		},
		{
			name:      "a column named like an Oracle package",
			dialect:   "postgres",
			assertion: `SELECT COUNT(*) = 0 FROM endpoints WHERE utl_http IS NULL`,
		},
		{
			name:      "count rows on Oracle",
			dialect:   "oracle",
			assertion: `SELECT COUNT(*) FROM users WHERE tier IS NULL`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""), qt.IsNil)
		})
	}
}

// The same class reached by a spelling that is easy to leave out: ClickHouse's
// pool variant of the executable table function, and adminpack, which ships
// with PostgreSQL and writes files on the host from a scalar SELECT.
func TestValidateCheckAssertionStatically_RefusesThePoolAndTheFileWriters(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
		wantErr   string
	}{
		{
			name:      "clickhouse executable pool",
			dialect:   "clickhouse",
			assertion: `SELECT count() >= 0 FROM executablePool('script.py', 'CSV', 'id UInt32')`,
			wantErr:   `check assertion must not use ClickHouse remote table function, which .*`,
		},
		{
			name:      "adminpack writes a file",
			dialect:   "postgres",
			assertion: `SELECT pg_file_write('/tmp/ptah', 'x', false) > 0`,
			wantErr:   `check assertion must not use adminpack file function, which .*`,
		},
		{
			name:      "adminpack removes a file",
			dialect:   "postgres",
			assertion: `SELECT pg_file_unlink('/tmp/ptah')`,
			wantErr:   `check assertion must not use adminpack file function, which .*`,
		},
		{
			name:      "adminpack renames a file",
			dialect:   "postgres",
			assertion: `SELECT pg_file_rename('/tmp/a', '/tmp/b')`,
			wantErr:   `check assertion must not use adminpack file function, which .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// Two spellings the first pass missed. A nontransactional logical message
// reaches whatever is decoding the write-ahead log and survives the rollback,
// and Oracle resolves a quoted package name to the same package the bare one
// names while the lexer reports it as a different kind of token.
func TestValidateCheckAssertionStatically_RefusesTheSpellingsAroundTheEdges(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		assertion string
		wantErr   string
	}{
		{
			name:      "a nontransactional logical message",
			dialect:   "postgres",
			assertion: `SELECT pg_logical_emit_message(false, 'release-check', 'x') IS NOT NULL`,
			wantErr:   `check assertion must not use PostgreSQL server control function, which .*`,
		},
		{
			name:      "a quoted Oracle package",
			dialect:   "oracle",
			assertion: `SELECT "UTL_HTTP"."REQUEST"('http://example.com/') IS NOT NULL FROM dual`,
			wantErr:   `check assertion must not use Oracle network package, which .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// The control the quoted spelling must not swallow: a string literal that
// merely contains the package name is data, not a call.
func TestValidateCheckAssertionStatically_AcceptsThePackageNameAsData(t *testing.T) {
	c := qt.New(t)

	err := validateCheckAssertionStatically(
		`SELECT COUNT(*) = 0 FROM audit WHERE note = 'UTL_HTTP'`, "oracle", "")

	c.Assert(err, qt.IsNil)
}

// A name the server resolves is the name this refuses, whatever spelling
// reaches it. PostgreSQL decodes a `U&"..."` identifier before it looks the
// function up, so a catalog that compares the characters the author typed
// refuses the plain spelling and runs the escaped one.
func TestValidateCheckAssertionStatically_RefusesUnicodeEscapedNames(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
		wantErr   string
	}{
		{
			name:      "the default escape character",
			assertion: `SELECT U&"pg\005Fread\005Ffile"('/etc/passwd') IS NOT NULL`,
			wantErr:   `check assertion must not use pg_read_file, which .*`,
		},
		{
			name:      "an escape character the clause names",
			assertion: `SELECT U&"pg!005Fread!005Ffile" UESCAPE '!' ('/etc/passwd') IS NOT NULL`,
			wantErr:   `check assertion must not use pg_read_file, which .*`,
		},
		{
			name:      "a code point written in six digits",
			assertion: `SELECT U&"pg\+00005Fterminate\+00005Fbackend"(1)`,
			wantErr:   `check assertion must not use PostgreSQL server control function, which .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "postgres", ""),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// The control the decoding needs. `U&` introduces an identifier only when it
// sits against the quote, so a bitwise operator over a column named `u` stays
// an ordinary read, and a doubled escape character stands for itself rather
// than opening a sequence.
func TestValidateCheckAssertionStatically_AcceptsWhatIsNotAnEscapedName(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "a bitwise operator between two columns",
			assertion: `SELECT u & "pg_read_file" > 0 FROM flags`,
		},
		{
			name:      "a doubled escape character names no code point",
			assertion: `SELECT U&"pg\\005Fread" IS NOT NULL FROM names`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "postgres", ""), qt.IsNil)
		})
	}
}

// A package name is refused where it calls something, and left alone where it
// is an ordinary identifier. Oracle reaches the network and the instance
// through packages whose names a table is free to reuse as a column.
func TestValidateCheckAssertionStatically_RefusesOraclePackageCalls(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
		wantErr   string
	}{
		{
			name:      "a pipe removed from the instance",
			assertion: `SELECT CASE DBMS_PIPE.REMOVE_PIPE('APP_EVENTS') WHEN 0 THEN 1 ELSE 2 END FROM dual`,
			wantErr:   `check assertion must not use Oracle server control package, which .*`,
		},
		{
			name:      "a job that runs after this statement",
			assertion: `SELECT 1 FROM dual WHERE DBMS_SCHEDULER.RUNNING_JOB_COUNT > 0`,
			wantErr:   `check assertion must not use Oracle server control package, which .*`,
		},
		{
			name:      "a request sent from the server",
			assertion: `SELECT UTL_HTTP.REQUEST('http://example.com') IS NOT NULL FROM dual`,
			wantErr:   `check assertion must not use Oracle network package, which .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "oracle", ""),
				qt.ErrorMatches, test.wantErr)
		})
	}
}

// The control the call shape needs. A column carrying a package's name is an
// ordinary read, and refusing it would cost the author an assertion they are
// entitled to write.
func TestValidateCheckAssertionStatically_AcceptsAPackageNameAsAColumn(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "a column named after a network package",
			assertion: `SELECT count(*) FROM endpoints WHERE UTL_HTTP IS NOT NULL`,
		},
		{
			name:      "a column named after a control package",
			assertion: `SELECT count(*) FROM queues WHERE DBMS_PIPE IS NOT NULL`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "oracle", ""), qt.IsNil)
		})
	}
}

// Server control is not only the functions that stop a backend: a slot moved
// forward loses write-ahead log a consumer had not read, and a backend told to
// log its memory contexts writes to the server log, which no rollback retracts.
func TestValidateCheckAssertionStatically_RefusesMoreServerControl(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
	}{
		{
			name:      "a replication slot advanced",
			assertion: `SELECT end_lsn IS NOT NULL FROM pg_replication_slot_advance('consumer', '0/5000000')`,
		},
		{
			name:      "the backend's memory contexts logged",
			assertion: `SELECT pg_log_backend_memory_contexts(pg_backend_pid())`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "postgres", ""),
				qt.ErrorMatches, `check assertion must not use PostgreSQL server control function, which .*`)
		})
	}
}
