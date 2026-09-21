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

// The two constructs the review found after the file-write rule was written,
// and the reason the recognition moved to one place: each is a SELECT that
// touches something no transaction owns, and each was outside the list this
// file first carried.
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, "clickhouse", ""), qt.IsNil)
		})
	}
}
