package atlasreport_test

import (
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/migrationreplay"
)

// A replay that stopped at a statement the dev database answered reads as the
// community binary's line: the statement as the file spells it, quoted onto
// one line, the version's revision identity, and the answer alone. The chain
// it was read from stays reachable.
func TestNewValidateStatementError_HappyPath(t *testing.T) {
	c := qt.New(t)
	answer := errors.New("SQL logic error: no such table: nosuch (1)")
	failed := &migrationreplay.MigrationError{
		Version:    2,
		VersionKey: "1.5",
		Err:        errors.New("failed to execute migration SQL"),
		Statement: &migrationreplay.FailedStatement{
			SQL:     "INSERT INTO \"nosuch\"\n\tVALUES ('a\"b')",
			Written: "INSERT INTO \"nosuch\"\n\tVALUES ('a\"b');",
			Index:   2,
			Answer:  answer,
		},
	}

	err := atlasreport.NewValidateStatementError(fmt.Errorf("error validating migration SQL on dev database: %w", failed))

	c.Assert(err, qt.ErrorMatches, `replaying the migration directory: sql/migrate: read migration directory state: `+
		`sql/migrate: executing statement "INSERT INTO \\"nosuch\\"\\n\\tVALUES \('a\\"b'\);" from version "1\.5": `+
		`SQL logic error: no such table: nosuch \(1\)`)
	c.Assert(err, qt.ErrorIs, failed)
	executed, ok := errors.AsType[*atlasreport.ExecutingStatementError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(executed.Answer, qt.Equals, answer)
}

// A replay that did not stop at a statement the database answered keeps
// Ptah's own wording: the line claims a statement was executed.
func TestNewValidateStatementError_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "no error", err: nil},
		{name: "a failure that names no migration", err: errors.New("error connecting to dev database: refused")},
		{
			name: "a statement refused before it ran",
			err: &migrationreplay.MigrationError{
				Version:    1,
				VersionKey: "1",
				Err:        errors.New("sqlite migration replay rejects ATTACH"),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlasreport.NewValidateStatementError(test.err), qt.IsNil)
		})
	}
}
