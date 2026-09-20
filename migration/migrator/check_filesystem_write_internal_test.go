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
			wantErr:   `check assertion must not write a file with INTO OUTFILE or INTO DUMPFILE`,
		},
		{
			name:      "mariadb outfile",
			dialect:   "mariadb",
			assertion: `SELECT 'x' INTO OUTFILE '/tmp/ptah-check-probe'`,
			wantErr:   `check assertion must not write a file with INTO OUTFILE or INTO DUMPFILE`,
		},
		{
			name:      "mysql dumpfile",
			dialect:   "mysql",
			assertion: `SELECT 'x' INTO DUMPFILE '/tmp/ptah-check-probe'`,
			wantErr:   `check assertion must not write a file with INTO OUTFILE or INTO DUMPFILE`,
		},
		// An executable comment is SQL the server runs and the lexer reports as
		// one opaque token, so a scan of the text as written never sees the
		// clause. The rule reads the effective SQL, which is the same form the
		// SELECT-only rule already reads.
		{
			name:      "mysql outfile inside an executable comment",
			dialect:   "mysql",
			assertion: `SELECT 'x' /*! INTO OUTFILE '/tmp/ptah-check-probe' */`,
			wantErr:   `check assertion must not write a file with INTO OUTFILE or INTO DUMPFILE`,
		},
		// A versioned executable comment needs a server version to decide
		// whether the server would run it. This one would, so the clause is
		// real SQL and the rule reads it.
		{
			name:          "mysql dumpfile inside a versioned executable comment",
			dialect:       "mysql",
			serverVersion: "8.0.36",
			assertion:     `SELECT 'x' /*!50001 INTO DUMPFILE '/tmp/ptah-check-probe' */`,
			wantErr:       `check assertion must not write a file with INTO OUTFILE or INTO DUMPFILE`,
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
