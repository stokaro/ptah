package compare

// White-box testing required: validates nonEmptyDiffExitCode, which is the
// package-local adapter between schema diff results and CLI exit codes.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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
