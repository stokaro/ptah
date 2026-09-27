package compare

// White-box testing required: validates nonEmptyDiffExitCode, which is the
// package-local adapter between schema diff results and CLI exit codes.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestCompareExitCode_EmptyDiff(t *testing.T) {
	c := qt.New(t)

	err := nonEmptyDiffExitCode(&difftypes.SchemaDiff{}, nil)

	c.Assert(err, qt.IsNil)
}

func TestCompareExitCode_NonEmptyDiff(t *testing.T) {
	c := qt.New(t)

	err := nonEmptyDiffExitCode(&difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "users"}}}, nil)

	c.Assert(err, qt.ErrorMatches, "schema diff is non-empty")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}

// An object the comparison withheld is one it could not check, so an empty
// diff beside it is not a proof that the database matches. The gate fails on
// it with the code a difference gets: the check ran, and the database is not
// shown to match (stokaro/ptah#3834).
func TestCompareExitCode_UndecidedWithoutDifferences(t *testing.T) {
	c := qt.New(t)

	err := nonEmptyDiffExitCode(&difftypes.SchemaDiff{}, []coverage.Object{undecidedRole("reporter")})

	c.Assert(err, qt.ErrorMatches, "1 declared object could not be decided")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}

func TestCompareExitCode_UndecidedBesideDifferences(t *testing.T) {
	c := qt.New(t)

	err := nonEmptyDiffExitCode(
		&difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "users"}}},
		[]coverage.Object{undecidedRole("reporter"), undecidedRole("auditor")},
	)

	c.Assert(err, qt.ErrorMatches, "schema diff is non-empty")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}

// undecidedRole is the record the comparator makes for a declared role it
// withheld because the database account was refused the role catalog.
func undecidedRole(name string) coverage.Object {
	object := coverage.Refused(coverage.Role)
	object.Name = name
	return object
}
