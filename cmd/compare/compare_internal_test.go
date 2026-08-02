package compare

// White-box testing required: validates nonEmptyDiffExitCode, which is the
// package-local adapter between schema diff results and CLI exit codes.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestCompareExitCode_EmptyDiff(t *testing.T) {
	c := qt.New(t)

	err := nonEmptyDiffExitCode(&difftypes.SchemaDiff{})

	c.Assert(err, qt.IsNil)
}

func TestCompareExitCode_NonEmptyDiff(t *testing.T) {
	c := qt.New(t)

	err := nonEmptyDiffExitCode(&difftypes.SchemaDiff{TablesAdded: []string{"users"}})

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}
