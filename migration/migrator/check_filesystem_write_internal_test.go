package migrator

// White-box testing required: the rule is a property of one unexported
// validator, and the only engines it speaks for are MySQL and MariaDB. Driving
// it through the exported path needs a connection to one of those, and asking
// a real server is the one thing this test must not do: if the guard were
// wrong the server would write the file, which is the outcome the guard exists
// to prevent. So the assertion is that the text is refused before any query is
// built.

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
		name      string
		dialect   string
		assertion string
		wantErr   string
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""),
				qt.ErrorMatches, test.wantErr)
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(validateCheckAssertionStatically(test.assertion, test.dialect, ""), qt.IsNil)
		})
	}
}
