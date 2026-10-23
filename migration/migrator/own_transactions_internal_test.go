package migrator

// White-box testing required: how a file's own transaction boundaries are
// accepted is decided by the unexported statement validation, which runs before
// any statement reaches a database.

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
)

type refusingValidator struct{ seen []string }

func (v *refusingValidator) ValidateStatement(stmt string) error {
	v.seen = append(v.seen, stmt)
	if stmt == "BEGIN" || stmt == "COMMIT" {
		return errors.New("refused " + stmt)
	}
	return nil
}

type boundaryValidator struct {
	refusingValidator
	boundaries []int
	err        error
}

func (v *boundaryValidator) TransactionBoundaries([]string) ([]int, error) {
	return v.boundaries, v.err
}

type indexedError struct{ index int }

func (e indexedError) Error() string       { return "bad boundary" }
func (e indexedError) StatementIndex() int { return e.index }

func TestValidateMigrationStatements_PlainValidatorKeepsRefusingBoundaries(t *testing.T) {
	c := qt.New(t)
	validator := &refusingValidator{}

	err := validateMigrationStatements("1.sql", []string{"BEGIN", "SELECT 1", "COMMIT"}, validator)

	var failure *MigrationExecutionError
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Statement, qt.Equals, "BEGIN")
	c.Assert(validator.seen, qt.DeepEquals, []string{"BEGIN"})
}

func TestValidateMigrationStatements_BoundariesAreAcceptedAndTheRestIsValidated(t *testing.T) {
	c := qt.New(t)
	validator := &boundaryValidator{boundaries: []int{0, 2}}

	err := validateMigrationStatements("1.sql", []string{"BEGIN", "SELECT 1", "COMMIT"}, validator)

	c.Assert(err, qt.IsNil)
	c.Assert(validator.seen, qt.DeepEquals, []string{"SELECT 1"})
}

// A boundary is accepted by position. The same words anywhere else in the file
// are still validated, so a file cannot smuggle a second COMMIT past the check.
func TestValidateMigrationStatements_OnlyTheNamedPositionsAreAccepted(t *testing.T) {
	c := qt.New(t)
	validator := &boundaryValidator{boundaries: []int{0, 2}}

	err := validateMigrationStatements("1.sql", []string{"BEGIN", "SELECT 1", "COMMIT", "COMMIT"}, validator)

	var failure *MigrationExecutionError
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.StatementIndex, qt.Equals, 4)
	c.Assert(failure.Statement, qt.Equals, "COMMIT")
}

func TestValidateMigrationStatements_RefusedStructureNamesTheStatement(t *testing.T) {
	c := qt.New(t)
	validator := &boundaryValidator{err: indexedError{index: 1}}

	err := validateMigrationStatements("1.sql", []string{"BEGIN", "SAVEPOINT s", "COMMIT"}, validator)

	var failure *MigrationExecutionError
	c.Assert(err, qt.ErrorAs, &failure)
	c.Assert(failure.Statement, qt.Equals, "SAVEPOINT s")
	c.Assert(failure.StatementIndex, qt.Equals, 2)
	c.Assert(failure.Err, qt.ErrorMatches, `failed to validate migration SQL in 1.sql: bad boundary`)
	c.Assert(validator.seen, qt.IsNil)
}

func TestFileOwnsTransactions_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		validator StatementValidator
		want      bool
	}{
		{name: "no validator", validator: nil},
		{name: "plain validator", validator: &refusingValidator{}},
		{name: "file with no boundary", validator: &boundaryValidator{}},
		{name: "file with boundaries", validator: &boundaryValidator{boundaries: []int{0, 1}}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := fileOwnsTransactions(nil, "1.sql", "BEGIN; COMMIT;", test.validator)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestFileOwnsTransactions_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := fileOwnsTransactions(nil, "1.sql", "BEGIN;", &boundaryValidator{err: errors.New("no")})

	c.Assert(err, qt.IsNotNil)
	c.Assert(got, qt.IsFalse)
}
